package strategies

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	constraintv1alpha1 "github.com/ciena/turnbuckle/pkg/apis/constraint/v1alpha1"
	"github.com/ciena/turnbuckle/pkg/apis/underlay"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	klog "k8s.io/klog/v2"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
)

var localSchemeBuilder = runtime.SchemeBuilder{
	constraintv1alpha1.AddToScheme,
}
var Scheme = runtime.NewScheme()

func init() {
	// Seed the random number generator to each run
	// will have a unique sequence
	rand.Seed(time.Now().UTC().Unix())
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
	utilruntime.Must(localSchemeBuilder.AddToScheme(Scheme))

}

type constraintPolicyBindingStatus struct {
	name               string
	namespace          string
	mitigatedTimestamp metav1.Time
}

type podUpdateInfo struct {
	pod       *v1.Pod
	finalizer string
}

var errInvalidResource = errors.New("invalid-resource")
var errNotFound = errors.New("not-found")
var errCreateK8sClient = errors.New("create-k8s-client")
var errPodEvictionFailure = errors.New("pod-eviction-failure")
var updateItems []interface{}

type UnderlayController interface {
	Mitigate(pathId []string, src *v1.Node, peer *v1.Node, rules []*constraintv1alpha1.ConstraintPolicyRule) (string, error)
	Release(string) error
}

type underlayController struct {
	Service v1.Service
}

func getUnderlayController(client clientset.Interface) (UnderlayController, error) {
	svcs, err := client.CoreV1().Services("").List(context.Background(),
		metav1.ListOptions{LabelSelector: "constraint.ciena.com/underlay-controller"})
	if err != nil {
		return nil, err
	}
	if len(svcs.Items) == 0 {
		return nil, errors.New("not-found")
	}
	return &underlayController{Service: svcs.Items[0]}, nil
}

func (c *underlayController) Release(pathId string) error {
	var gopts []grpc.DialOption
	dns := fmt.Sprintf("%s.%s.svc.cluster.local:9999", c.Service.Name, c.Service.Namespace)
	gopts = append(gopts, grpc.WithInsecure())
	conn, err := grpc.Dial(dns, gopts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := underlay.NewUnderlayControllerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := underlay.ReleaseRequest{Id: pathId}
	_, err = client.Release(ctx, &req)
	if err != nil {
		return err
	}
	return nil
}

func (c *underlayController) Mitigate(currentPathId []string, src *v1.Node, peer *v1.Node, rules []*constraintv1alpha1.ConstraintPolicyRule) (string, error) {
	var gopts []grpc.DialOption
	dns := fmt.Sprintf("%s.%s.svc.cluster.local:9999", c.Service.Name, c.Service.Namespace)
	gopts = append(gopts, grpc.WithInsecure())
	conn, err := grpc.Dial(dns, gopts...)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	client := underlay.NewUnderlayControllerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := underlay.MitigateRequest{Id: currentPathId,
		Src:  &underlay.NodeRef{Name: src.Name},
		Peer: &underlay.NodeRef{Name: peer.Name},
	}
	resp, err := client.Mitigate(ctx, &req)
	if err != nil {
		return "", err
	}
	return resp.Id, nil
}

func getUnderlayPath(finalizers []string) []string {
	paths := []string{}
	finalizerPrefix := "constraint.ciena.io/remove-underlay_"
	for _, finalizer := range finalizers {
		if strings.HasPrefix(finalizer, finalizerPrefix) {
			paths = append(paths, finalizer[len(finalizerPrefix):])
		}
	}
	return paths
}

func belongsToList(val string, list []string) bool {
	for _, l := range list {
		if l == val {
			return true
		}
	}
	return false
}

func mitigate(client clientset.Interface, controller UnderlayController, srcPod *v1.Pod, dstPod *v1.Pod,
	srcNode *v1.Node, dstNode *v1.Node, rules []*constraintv1alpha1.ConstraintPolicyRule) error {
	paths := []string{}
	if srcPod.GetFinalizers() != nil {
		paths = getUnderlayPath(srcPod.GetFinalizers())
	} else if dstPod.GetFinalizers() != nil {
		paths = getUnderlayPath(dstPod.GetFinalizers())
	}
	klog.V(0).InfoS("descheduler-mitigate-with-paths", "path", paths)
	pathId, err := controller.Mitigate(paths, srcNode, dstNode, rules)
	if err != nil {
		klog.V(0).ErrorS(err, "mitigate-failed", "src", srcNode.Name, "dst", dstNode.Name)
		return err
	}
	klog.V(0).InfoS("descheduler-mitigate", "pathId", pathId, "src", srcNode.Name, "peer", dstNode.Name)
	//update the src and dst pod with the path finalizer
	finalizer := "constraint.ciena.io/remove-underlay_" + pathId
	if !belongsToList(finalizer, srcPod.ObjectMeta.Finalizers) {
		srcPod.ObjectMeta.Finalizers = append(srcPod.ObjectMeta.Finalizers, finalizer)
		if _, err := client.CoreV1().Pods(srcPod.Namespace).Update(context.Background(), srcPod, metav1.UpdateOptions{}); err != nil {
			klog.V(0).ErrorS(err, "mitigate-pod-finalizer-update-failed", "pod", srcPod.Name)
			//enqueue for retry
			klog.V(0).InfoS("mitigate-pod-update-enqueue", "pod", srcPod.Name)
			updateItems = append(updateItems, &podUpdateInfo{pod: srcPod, finalizer: finalizer})
		}
	}
	if !belongsToList(finalizer, dstPod.ObjectMeta.Finalizers) {
		dstPod.ObjectMeta.Finalizers = append(dstPod.ObjectMeta.Finalizers, finalizer)
		if _, err := client.CoreV1().Pods(dstPod.Namespace).Update(context.Background(), dstPod, metav1.UpdateOptions{}); err != nil {
			klog.V(0).ErrorS(err, "mitigate-pod-finalizer-update-failed", "pod", dstPod.Name)
			//enqueue for retry
			klog.V(0).InfoS("mitigate-pod-update-enqueue", "pod", dstPod.Name)
			updateItems = append(updateItems, &podUpdateInfo{pod: dstPod, finalizer: finalizer})
		}
	}
	return nil
}

func getPodsAndNodesFromBinding(client clientset.Interface, binding *constraintv1alpha1.ConstraintPolicyBinding) (map[string]*v1.Pod, map[string]*v1.Node, error) {
	if len(binding.Spec.Targets) == 0 {
		return nil, nil, fmt.Errorf("no-targets-found-for-binding-%s", binding.GetName())
	}

	targets := make(map[string]*v1.Pod, len(binding.Spec.Targets))
	podToNodeMap := make(map[string]*v1.Node, len(binding.Spec.Targets))

	for name, ref := range binding.Spec.Targets {
		if ref.Kind == "Pod" {
			pod, err := client.CoreV1().Pods(ref.Namespace).Get(context.Background(), ref.Name, metav1.GetOptions{})
			if err == nil {
				node, err := client.CoreV1().Nodes().Get(context.Background(), pod.Spec.NodeName, metav1.GetOptions{})
				if err == nil {
					targets[name] = pod
					podToNodeMap[pod.Name] = node
				}
			}
		}
	}

	return targets, podToNodeMap, nil
}

func getPolicyRules(genericClient ctlrclient.Client, offer *constraintv1alpha1.ConstraintPolicyOffer) ([]*constraintv1alpha1.ConstraintPolicyRule, error) {
	var rules []*constraintv1alpha1.ConstraintPolicyRule
	for _, policyName := range offer.Spec.Policies {
		var policy constraintv1alpha1.ConstraintPolicy
		err := genericClient.Get(context.Background(),
			ctlrclient.ObjectKey{
				Namespace: offer.GetNamespace(),
				Name:      string(policyName),
			},
			&policy)
		if err != nil {
			klog.V(0).ErrorS(err, "unknown-policy", "name", string(policyName))
			continue
		}
		if rules == nil {
			rules = policy.Spec.Rules
		} else {
			rules = mergeRules(rules, policy.Spec.Rules)
		}
	}
	if len(rules) == 0 {
		klog.V(0).InfoS("no-rules", "offer", offer.GetName())
		return nil, fmt.Errorf("no-rules-found-for-offer-%s", offer.GetName())
	}
	return rules, nil
}

func updateBindingStatusTimestamp(genericClient ctlrclient.Client, binding *constraintv1alpha1.ConstraintPolicyBinding,
	mitigatedTimestamp metav1.Time, enqueueOnFailure bool) error {
	binding.Status.LastMitigatedTimestamp = mitigatedTimestamp
	if err := genericClient.Status().Update(context.Background(),
		binding); err != nil {
		klog.V(0).ErrorS(err, "update-binding-status-failed", "binding", binding.GetName())
		if enqueueOnFailure {
			//enqueue to retry later
			klog.V(0).InfoS("update-binding-status-enqueue", "binding", binding.GetName())
			updateItems = append(updateItems, &constraintPolicyBindingStatus{
				name: binding.GetName(), namespace: binding.GetNamespace(), mitigatedTimestamp: mitigatedTimestamp,
			})
		} else {
			return err
		}
	}
	return nil
}

func mergeRules(existingRules []*constraintv1alpha1.ConstraintPolicyRule, newRules []*constraintv1alpha1.ConstraintPolicyRule) []*constraintv1alpha1.ConstraintPolicyRule {
	presenceMap := make(map[string]struct{})
	for _, r := range existingRules {
		presenceMap[r.Name] = struct{}{}
	}
	for _, r := range newRules {
		if _, ok := presenceMap[r.Name]; !ok {
			existingRules = append(existingRules, r)
		}
	}
	return existingRules
}

func ConstraintPolicyEvaluation(
	ctx context.Context,
	client clientset.Interface,
	strategy api.DeschedulerStrategy,
	nodes []*v1.Node,
	podEvictor *evictions.PodEvictor,
	sopts ...StrategyOption) {

	opts := buildStrategyOptions(sopts)

	config, err := rest.InClusterConfig()
	if err != nil {
		if opts.kubeconfigFile == "" {
			klog.V(0).ErrorS(errCreateK8sClient, "unable to create Kubernetes API client")
			return
		} else {
			bytes, err := ioutil.ReadFile(opts.kubeconfigFile)
			if err != nil {
				klog.V(0).ErrorS(err, "unable to read config file", "filename", opts.kubeconfigFile)
				return
			}
			config, err = clientcmd.RESTConfigFromKubeConfig(bytes)
			if err != nil {
				klog.V(0).ErrorS(err, "unable to create Kubernetes API client config")
				return
			}
		}
	}
	genericClient, err := ctlrclient.New(config, ctlrclient.Options{
		Scheme: Scheme,
	})
	if err != nil {
		klog.V(0).ErrorS(err, "unable to create generic controller client")
		return
	}
	uc, err := getUnderlayController(client)
	if err != nil {
		klog.V(0).ErrorS(err, "underlay-controller-not-found. Mitigation would be disabled")
	}

	var bindings constraintv1alpha1.ConstraintPolicyBindingList
	err = genericClient.List(context.Background(), &bindings,
		ctlrclient.InNamespace(metav1.NamespaceAll))
	if err != nil {
		klog.V(0).ErrorS(err, "unable to list constraint policy bindings")
		return
	}
	if opts.mitigationGracePeriod.Seconds() == 0 {
		opts.mitigationGracePeriod = time.Minute
	}
	klog.V(0).InfoS("descheduler", "mitigation-grace-period", opts.mitigationGracePeriod, "bindings", len(bindings.Items))
	for _, binding := range bindings.Items {
		if binding.GetDeletionTimestamp() != nil {
			// being deleted, ignore
			continue
		}
		comp := binding.Status.Compliance
		klog.V(0).InfoS("compliance-evaluation",
			"cluster", binding.GetClusterName(),
			"namespace", binding.GetNamespace(),
			"name", binding.GetName(),
			"compliance", comp)
		if comp == "Violation" {
			var offer constraintv1alpha1.ConstraintPolicyOffer
			err = genericClient.Get(context.Background(),
				ctlrclient.ObjectKey{
					Namespace: binding.GetNamespace(),
					Name:      binding.Spec.Offer,
				},
				&offer)
			if err != nil {
				klog.V(0).ErrorS(err, "unknown-policy-offer", "name", binding.Spec.Offer)
				continue
			}
			// Get violation policy
			vpolicy := offer.Spec.ViolationPolicy
			klog.V(0).InfoS("violation-policy", "policy", vpolicy)
			if vpolicy == "Evict" {
				targets, podToNodeMap, err := getPodsAndNodesFromBinding(client, binding)
				if err != nil {
					klog.V(0).ErrorS(err, "error-processing-binding-targets", "binding", binding.GetName())
					continue
				}

				if len(targets) == 0 {
					klog.V(0).InfoS("no-targets-to-be-processed", "binding", binding.GetName())
					continue
				}

				var srcPod, dstPod *v1.Pod

				if _, ok := targets["source"]; ok {
					srcPod = targets["source"]
				}

				if _, ok := targets["destination"]; ok {
					dstPod = targets["destination"]
				}

				var graceDuration time.Duration
				if d, err := time.ParseDuration(offer.Spec.Grace); err == nil {
					graceDuration = d
				}
				lastComplianceChangeTimestamp := binding.Status.LastComplianceChangeTimestamp
				expiry := lastComplianceChangeTimestamp.Add(graceDuration)
				if time.Now().Before(expiry) {
					klog.V(0).InfoS("descheduler-eviction-grace-period", "binding", binding.GetName(), "offer", offer.GetName())
					continue
				}
				lastMitigatedTimestamp := binding.Status.LastMitigatedTimestamp
				if lastMitigatedTimestamp.IsZero() {
					if uc != nil && srcPod != nil && dstPod != nil {
						// get the offer policies
						var policyRules []*constraintv1alpha1.ConstraintPolicyRule
						rules, err := getPolicyRules(genericClient, &offer)
						if err != nil {
							klog.V(0).ErrorS(err, "rule-not-found", "offer", offer.GetName())
						} else {
							policyRules = rules
						}
						if err := mitigate(client, uc, srcPod, dstPod, podToNodeMap[srcPod.Name], podToNodeMap[dstPod.Name], policyRules); err == nil {
							if err := updateBindingStatusTimestamp(genericClient, binding, metav1.Now(), true); err != nil {
								klog.V(0).ErrorS(err, "update-binding-failed", "binding", binding.GetName(), "offer", offer.GetName())
							} else {
								klog.V(0).InfoS("update-binding-success", "binding", binding.GetName(), "offer", offer.GetName())
							}
							klog.V(0).InfoS("descheduler-mitigate", "src", srcPod.Name, "dst", dstPod.Name, "binding", binding.GetName())
							continue
						}
					}
				} else {
					expiry := lastMitigatedTimestamp.Add(opts.mitigationGracePeriod)
					if time.Now().Before(expiry) {
						klog.V(0).InfoS("binding-under-mitigation-grace-period", "binding", binding.GetName())
						continue
					} else {
						klog.V(0).InfoS("binding-outside-mitigation-grace-period", "binding", binding.GetName())
					}
				}
				var targetPod *v1.Pod
				var targetNode *v1.Node
				// we are here when we dont have source/destination targets
				// or when mitigation grace period has expired or mitigation failed for source/destination pods
				switch {
				case srcPod != nil && dstPod != nil:
					klog.V(0).InfoS("selecting-from-src-dst-pods-to-evict", "src", srcPod.Name, "dst", dstPod.Name)
					targetPods := []*v1.Pod{srcPod, dstPod}
					targetNodes := []*v1.Node{podToNodeMap[srcPod.Name], podToNodeMap[dstPod.Name]}
					target := rand.Intn(2)
					targetPod, targetNode = targetPods[target], targetNodes[target]
				default:
					// randomly pick a pod from the targets
					klog.V(0).InfoS("selecting-from-available-pods-to-evict", "num-pods", len(targets))
					pods := make([]*v1.Pod, 0, len(targets))
					for _, p := range targets {
						pods = append(pods, p)
					}
					targetPod = pods[rand.Intn(len(pods))]
					targetNode = podToNodeMap[targetPod.Name]
				}
				klog.V(0).InfoS("evict-pod", "namespace", targetPod.Namespace,
					"name", targetPod.Name, "node", targetNode.Name)
				ok, err := podEvictor.EvictPod(context.TODO(), targetPod, targetNode,
					fmt.Sprintf("policy offer '%s' is in violation", offer.GetName()))
				if err != nil {
					klog.V(0).ErrorS(err, "pod-eviction-error",
						"namespace", targetPod.Namespace, "name", targetPod.Name, "node", targetNode.Name)
				} else if !ok {
					klog.V(0).ErrorS(errPodEvictionFailure,
						"namespace", targetPod.Namespace, "name", targetPod.Name, "node", targetNode.Name)
				} else {
					klog.V(0).InfoS("pod-evicted", "namespace", targetPod.Namespace,
						"name", targetPod.Name, "node", targetNode.Name)
				}
			}
		}
	}

	if len(updateItems) > 0 {
		if opts.wg == nil {
			klog.V(0).InfoS("no-waitgroup-created-to-update-batch. Call strategy WithWaitGroup option")
		} else {
			klog.V(0).InfoS("update-workqueue", "num-items", len(updateItems))
			opts.wg.Add(1)
			go updateWorkqueue(genericClient, client, updateItems, opts.wg)
		}
	}
}

func processBindingUpdate(genericClient ctlrclient.Client, constraintBinding *constraintPolicyBindingStatus) (requeue bool) {
	var binding constraintv1alpha1.ConstraintPolicyBinding
	requeue = false
	err := genericClient.Get(context.Background(),
		ctlrclient.ObjectKey{
			Namespace: constraintBinding.namespace,
			Name:      constraintBinding.name,
		},
		&binding)
	if err != nil {
		klog.V(0).ErrorS(err, "process-binding-update-failed", "binding", constraintBinding.name)
		return
	}
	if err := updateBindingStatusTimestamp(genericClient, &binding, constraintBinding.mitigatedTimestamp, false); err != nil {
		requeue = true
	} else {
		klog.V(0).InfoS("process-binding-update-success", "binding", constraintBinding.name)
	}
	return
}

func processPodUpdate(client clientset.Interface, podUpdate *podUpdateInfo) (requeue bool) {
	requeue = false
	//get the new copy of the pod
	pod, err := client.CoreV1().Pods(podUpdate.pod.GetNamespace()).Get(context.Background(), podUpdate.pod.GetName(), metav1.GetOptions{})
	if err != nil {
		klog.V(0).ErrorS(err, "pod-update-get-failed", "pod", podUpdate.pod.GetName())
		return
	}
	// check if the finalizer already exist.
	finalizers := pod.GetFinalizers()
	if finalizers != nil && belongsToList(podUpdate.finalizer, finalizers) {
		//already exist.
		return
	}
	//set the finalizer
	pod.ObjectMeta.Finalizers = append(pod.ObjectMeta.Finalizers, podUpdate.finalizer)
	if _, err := client.CoreV1().Pods(pod.GetNamespace()).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		klog.V(0).ErrorS(err, "pod-update-finalizer-failed", "pod", pod.GetName())
		requeue = true
	} else {
		klog.V(0).InfoS("pod-update-success", "pod", pod.GetName())
	}
	return
}

func updateWorkqueue(genericClient ctlrclient.Client, client clientset.Interface, updateItems []interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	defer queue.ShutDown()
	maxRequeues := 2
	for len(updateItems) > 0 {
		updateItem := updateItems[0]
		updateItems = updateItems[1:]
		klog.V(0).InfoS("update-workqueue-adding-item-ratelimited")
		queue.AddRateLimited(updateItem)
		// wait for the item to appear
		item, quit := queue.Get()
		if quit {
			break
		}

		// nolint S1034
		switch item.(type) {
		case *constraintPolicyBindingStatus:
			if requeue := processBindingUpdate(genericClient, item.(*constraintPolicyBindingStatus)); requeue {
				// we don't want to requeue forever as this will freeze the descheduler on the waitgroup
				// we allow up to 2 requeues
				if queue.NumRequeues(item) <= maxRequeues {
					updateItems = append(updateItems, item)
					klog.V(0).InfoS("update-workqueue-enqueueing-binding-update-again", "numrequeues", queue.NumRequeues(item))
				} else {
					klog.V(0).InfoS("update-workqueue-done-retrying-binding-update", "numrequeues", queue.NumRequeues(item))
					queue.Forget(item)
				}
			} else {
				klog.V(0).InfoS("update-workqueue-binding-done")
				queue.Forget(item)
			}
		case *podUpdateInfo:
			if requeue := processPodUpdate(client, item.(*podUpdateInfo)); requeue {
				if queue.NumRequeues(item) <= maxRequeues {
					updateItems = append(updateItems, item)
					klog.V(0).InfoS("update-workqueue-enqueueing-pod-update-again", "numrequeues", queue.NumRequeues(item))
				} else {
					klog.V(0).InfoS("update-workqueue-done-retrying-pod-update", "numrequeues", queue.NumRequeues(item))
					queue.Forget(item)
				}
			} else {
				klog.V(0).InfoS("update-workqueue-pod-done")
				queue.Forget(item)
			}
		default:
			klog.V(0).InfoS("update-workqueue-unknown-type")
			queue.Forget(item)
		}
		queue.Done(item)
	}
}

// The below is copied from the constraint-policy repository utilizing the go
// proverb "A little copying is better than a little dependency". No need
// to pull in the whole repo as a dependency for this little type.

// Endpoint a concreate end point reference
type Endpoint struct {
	Cluster   string
	Namespace string
	Kind      string
	Name      string
	IP        string
}

func (ep Endpoint) String() string {
	var buf strings.Builder

	if ep.Cluster != "" {
		buf.WriteString(ep.Cluster)
		buf.WriteString("/")
	}

	if ep.Namespace != "" {
		buf.WriteString(ep.Namespace)
		buf.WriteString(":")
	} else if ep.Cluster != "" {
		buf.WriteString("default:")
	}
	if ep.Kind != "" {
		buf.WriteString(ep.Kind)
		buf.WriteString("/")
	}
	buf.WriteString(ep.Name)
	if ep.IP != "" {
		buf.WriteString("[")
		buf.WriteString(ep.IP)
		buf.WriteString("]")
	}

	return buf.String()
}

var endpointRE = regexp.MustCompile(`^((([a-zA-Z0-9_-]*)/)?([a-zA-Z0-9-]*):)?(([a-zA-Z0-9_-]*)/)?([a-zA-Z0-9_-]+)(\[([0-9.]*)\])?$`)

// ParseEndpoint parses a string representation of an endpoint
// to a Endpoint
func ParseEndpoint(in string) (*Endpoint, error) {
	var ep Endpoint

	parts := endpointRE.FindStringSubmatch(in)

	//fmt.Printf("%+#v\n", parts)
	if len(parts) == 0 {
		return nil, fmt.Errorf(`invalid endpoint "%s"`, in)
	}
	if len(parts) > 0 {
		ep.Cluster = parts[3]
		ep.Namespace = parts[4]
		ep.Kind = parts[6]
		ep.Name = parts[7]
		ep.IP = parts[9]
	}
	return &ep, nil
}

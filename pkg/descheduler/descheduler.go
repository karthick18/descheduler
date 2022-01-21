/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package descheduler

import (
	"context"
	"errors"
	"fmt"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies/nodeutilization"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"sigs.k8s.io/descheduler/cmd/descheduler/app/options"
	"sigs.k8s.io/descheduler/metrics"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/client"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	eutils "sigs.k8s.io/descheduler/pkg/descheduler/evictions/utils"
	nodeutil "sigs.k8s.io/descheduler/pkg/descheduler/node"
	"sigs.k8s.io/descheduler/pkg/descheduler/strategies"
)

func Run(rs *options.DeschedulerServer) error {
	metrics.Register()

	ctx := context.Background()
	rsclient, err := client.CreateClient(rs.KubeconfigFile)
	if err != nil {
		return err
	}
	rs.Client = rsclient

	deschedulerPolicy, err := LoadPolicyConfig(rs.PolicyConfigFile)
	if err != nil {
		return err
	}
	if deschedulerPolicy == nil {
		return fmt.Errorf("deschedulerPolicy is nil")
	}

	evictionPolicyGroupVersion, err := eutils.SupportEviction(rs.Client)
	if err != nil || len(evictionPolicyGroupVersion) == 0 {
		return err
	}

	stopChannel := make(chan struct{})
	return RunDeschedulerStrategies(ctx, rs, deschedulerPolicy, evictionPolicyGroupVersion, stopChannel)
}

type strategyFunction func(ctx context.Context, client clientset.Interface, strategy api.DeschedulerStrategy, nodes []*v1.Node, podEvictor *evictions.PodEvictor, options ...strategies.StrategyOption)

func RunDeschedulerStrategies(ctx context.Context, rs *options.DeschedulerServer, deschedulerPolicy *api.DeschedulerPolicy, evictionPolicyGroupVersion string, stopChannel chan struct{}) error {
	sharedInformerFactory := informers.NewSharedInformerFactory(rs.Client, 0)
	nodeInformer := sharedInformerFactory.Core().V1().Nodes()

	sharedInformerFactory.Start(stopChannel)
	sharedInformerFactory.WaitForCacheSync(stopChannel)

	strategyFuncs := map[api.StrategyName]strategyFunction{
		"RemoveDuplicates":                            strategies.RemoveDuplicatePods,
		"LowNodeUtilization":                          nodeutilization.LowNodeUtilization,
		"HighNodeUtilization":                         nodeutilization.HighNodeUtilization,
		"RemovePodsViolatingInterPodAntiAffinity":     strategies.RemovePodsViolatingInterPodAntiAffinity,
		"RemovePodsViolatingNodeAffinity":             strategies.RemovePodsViolatingNodeAffinity,
		"RemovePodsViolatingNodeTaints":               strategies.RemovePodsViolatingNodeTaints,
		"RemovePodsHavingTooManyRestarts":             strategies.RemovePodsHavingTooManyRestarts,
		"PodLifeTime":                                 strategies.PodLifeTime,
		"RemovePodsViolatingTopologySpreadConstraint": strategies.RemovePodsViolatingTopologySpreadConstraint,
		"RemoveFailedPods":                            strategies.RemoveFailedPods,
		"ConstraintPolicyEvaluation":                  strategies.ConstraintPolicyEvaluation,
	}

	nodeSelector := rs.NodeSelector
	if deschedulerPolicy.NodeSelector != nil {
		nodeSelector = *deschedulerPolicy.NodeSelector
	}

	evictLocalStoragePods := rs.EvictLocalStoragePods
	if deschedulerPolicy.EvictLocalStoragePods != nil {
		evictLocalStoragePods = *deschedulerPolicy.EvictLocalStoragePods
	}

	evictSystemCriticalPods := false
	if deschedulerPolicy.EvictSystemCriticalPods != nil {
		evictSystemCriticalPods = *deschedulerPolicy.EvictSystemCriticalPods
		if evictSystemCriticalPods {
			klog.V(1).InfoS("Warning: EvictSystemCriticalPods is set to True. This could cause eviction of Kubernetes system pods.")
		}
	}

	ignorePvcPods := false
	if deschedulerPolicy.IgnorePVCPods != nil {
		ignorePvcPods = *deschedulerPolicy.IgnorePVCPods
	}

	maxNoOfPodsToEvictPerNode := rs.MaxNoOfPodsToEvictPerNode
	if deschedulerPolicy.MaxNoOfPodsToEvictPerNode != nil {
		maxNoOfPodsToEvictPerNode = *deschedulerPolicy.MaxNoOfPodsToEvictPerNode
	}

	wait.Until(func() {
		nodes, err := nodeutil.ReadyNodes(ctx, rs.Client, nodeInformer, nodeSelector)
		if err != nil {
			klog.V(1).InfoS("Unable to get ready nodes", "err", err)
			close(stopChannel)
			return
		}

		if len(nodes) <= 1 {
			klog.V(1).InfoS("The cluster size is 0 or 1 meaning eviction causes service disruption or degradation. So aborting..")
			close(stopChannel)
			return
		}

		podEvictor := evictions.NewPodEvictor(
			rs.Client,
			evictionPolicyGroupVersion,
			rs.DryRun,
			maxNoOfPodsToEvictPerNode,
			nodes,
			evictLocalStoragePods,
			evictSystemCriticalPods,
			ignorePvcPods,
		)

		// It is possible that after a single call to the strategies the descheduer exits. This assumes that each
		// strategy can complete its work in a single call. This may not be the case and some strategies may
		// need to continue processing for a short while. To accommodate this a wait group option has been
		// added. If a strategy needs to complete some tasks (such as updating a k8s resource which may
		// take several retries because of object version clashes) they can add themselves to the wait group.
		var wg sync.WaitGroup

		for name, strategy := range deschedulerPolicy.Strategies {
			if f, ok := strategyFuncs[name]; ok {
				if strategy.Enabled {
					f(ctx, rs.Client, strategy, nodes, podEvictor, strategies.WithKubeconfigFile(rs.KubeconfigFile),
						strategies.WithWaitGroup(&wg), strategies.WithMitigationGracePeriod(rs.MitigationGracePeriod))
				}
			} else {
				klog.ErrorS(fmt.Errorf("unknown strategy name"), "skipping strategy", "strategy", name)
			}
		}

		// Wait for all strategies on the wait group to complete
		klog.V(1).InfoS("Waiting for strategies to complete...", "waitTime", rs.DeschedulingRunTimeout.String())

		c := make(chan struct{})

		go func() {
			defer close(c)
			wg.Wait()
		}()

		select {
		case <-c:
			klog.V(1).InfoS("All strategies run to completion")
		case <-time.After(rs.DeschedulingRunTimeout):
			// NOTE: This leaks a go routine until the original wait group completes
			klog.V(0).ErrorS(errors.New("time-out"), "Timeout while waiting for strategies to complete", "timeout", rs.DeschedulingRunTimeout.String())
		}

		klog.V(1).InfoS("Number of evicted pods", "totalEvicted", podEvictor.TotalEvicted())

		// If there was no interval specified, send a signal to the stopChannel to end the wait.Until loop after 1 iteration
		if rs.DeschedulingInterval.Seconds() == 0 {
			close(stopChannel)
		}
	}, rs.DeschedulingInterval, stopChannel)

	return nil
}

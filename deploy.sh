#!/usr/bin/env bash

OPTIND=1

namespace="turnbuckle-system"
tag=""
repo="k8s.gcr.io/descheduler/descheduler"
kubeconfig=$KUBECONFIG
delete=0

usage() {
    echo "deploy.sh -n|namespace -t|image tag -r |image repo -k|kubeconfig -d|delete"
    exit 0
}

while getopts "h?n:t:r:k:d" opt; do
    case "$opt" in
	h|\?)
	    usage
	    ;;
	n)
	    namespace=$OPTARG
	    ;;
	t)
	    tag=$OPTARG
	    ;;
	r)
	    repo=$OPTARG
	    ;;
	k)
	    kubeconfig=$OPTARG
	    ;;
	d)
	    delete=1
	    ;;
    esac
done

shift $((OPTIND-1))

if [ $delete -eq 1 ]; then
    echo "Deleting descheduler installation from namespace $namespace"
    helm --kubeconfig $kubeconfig delete -n $namespace descheduler
    exit $?
fi

echo "Installing descheduler with namespace $namespace, repo $repo, tag $tag"

if [ "x$kubeconfig" = "x" ]; then
    helm upgrade --install --wait --create-namespace --namespace $namespace --set image.tag=$tag --set image.repository=$repo --set image.pullPolicy=Always --values ./charts/descheduler/values.yaml descheduler ./charts/descheduler/
else
    echo "Using kubeconfig $kubeconfig"
    helm --kubeconfig $kubeconfig upgrade --install --wait --create-namespace --namespace $namespace --set image.tag=$tag --set image.repository=$repo --set image.pullPolicy=Always --values ./charts/descheduler/values.yaml descheduler ./charts/descheduler/
fi

exit $?

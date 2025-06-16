#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

kubectl delete -f manifests/deploy.yaml || true

${SCRIPT_DIR}/build.sh


kubectl apply -f manifests/ns.yaml
kubectl apply -f manifests/sa+rbac.yaml
kubectl apply -f manifests/deploy.yaml


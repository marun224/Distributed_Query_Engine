#!/bin/sh
# Runs INSIDE the kind node (decision 29): the node's kubectl and admin kubeconfig, so the
# Windows host needs no route to the API server (its host port is unavailable, tracker
# Known gaps). scripts\k8s.ps1 copies deploy/ and the job scripts to /var/tmp/icefleet and
# calls this with `docker exec`; nothing here needs quoting from PowerShell.
#
#   node.sh apply                    namespace, RBAC, the fleet WorkflowTemplate
#   node.sh secrets  < env-file      icefleet-s3, icefleet-catalog, minio-creds (stdin; RAM only)
#   node.sh jobs                     ConfigMap icefleet-jobs from /var/tmp/icefleet/jobs/*.py
#   node.sh workers up|down          model A's standing fleet
#   node.sh submit key=value ...     a Workflow from icefleet-fleet; prints its name
#   node.sh wait NAME [seconds]      until it ends; prints the phase, exit 0 only on Succeeded
#   node.sh logs NAME                the coordinator's log (and the workers' on failure)
#   node.sh leftovers NAME [seconds] waits for podGC; lists anything of NAME still there
#   node.sh status                   workflows, pods, services in the namespace
set -eu
export KUBECONFIG=/etc/kubernetes/admin.conf
NS=icefleet
DEPLOY=/var/tmp/icefleet/deploy
verb="${1:-}"
[ $# -gt 0 ] && shift

case "$verb" in
  apply)
    kubectl apply -f "$DEPLOY/k8s/namespace.yaml"
    kubectl apply -f "$DEPLOY/k8s/rbac.yaml" -f "$DEPLOY/argo/fleet.yaml"
    ;;

  secrets)
    # KEY=VALUE lines on stdin, already rewritten for in-cluster host names. Split into
    # the workers' Secret (object store only) and the coordinator's (catalog), plus the
    # minio-creds the Argo controller's log archive expects in every namespace.
    umask 077
    work=$(mktemp -d /dev/shm/icefleet.XXXXXX)
    trap 'rm -rf "$work"' EXIT
    # CRs and a UTF-8 byte-order mark from a Windows pipe would end up in key names.
    tr -d '\r' | sed '1s/^\xEF\xBB\xBF//' > "$work/all.env"
    grep -E '^ICETL_S3_' "$work/all.env" > "$work/s3.env" || true
    grep -E '^(ICETL_CATALOG_|ICETL_DEFAULT_NAMESPACE=)' "$work/all.env" > "$work/catalog.env" || true
    sed -n 's/^ICETL_S3_ACCESS_KEY_ID=//p' "$work/all.env" | tr -d '\n' > "$work/accessKey"
    sed -n 's/^ICETL_S3_SECRET_ACCESS_KEY=//p' "$work/all.env" | tr -d '\n' > "$work/secretKey"
    for f in s3.env catalog.env accessKey secretKey; do
      [ -s "$work/$f" ] || { echo "node.sh secrets: nothing for $f on stdin" >&2; exit 2; }
    done
    kubectl -n "$NS" create secret generic icefleet-s3 --from-env-file="$work/s3.env" \
      --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n "$NS" create secret generic icefleet-catalog --from-env-file="$work/catalog.env" \
      --dry-run=client -o yaml | kubectl apply -f -
    kubectl -n "$NS" create secret generic minio-creds \
      --from-file=accessKey="$work/accessKey" --from-file=secretKey="$work/secretKey" \
      --dry-run=client -o yaml | kubectl apply -f -
    ;;

  jobs)
    set --
    for f in /var/tmp/icefleet/jobs/*.py; do set -- "$@" "--from-file=$f"; done
    kubectl -n "$NS" create configmap icefleet-jobs "$@" --dry-run=client -o yaml | kubectl apply -f -
    ;;

  workers)
    case "${1:-}" in
      up) kubectl apply -f "$DEPLOY/k8s/workers.yaml" ;;
      down) kubectl delete --ignore-not-found -f "$DEPLOY/k8s/workers.yaml" ;;
      *) echo "node.sh workers up|down" >&2; exit 2 ;;
    esac
    ;;

  submit)
    manifest=$(mktemp)
    trap 'rm -f "$manifest"' EXIT
    {
      echo "apiVersion: argoproj.io/v1alpha1"
      echo "kind: Workflow"
      echo "metadata:"
      echo "  generateName: icefleet-fleet-"
      echo "  namespace: $NS"
      echo "spec:"
      echo "  workflowTemplateRef:"
      echo "    name: icefleet-fleet"
      echo "  arguments:"
      echo "    parameters:"
      for pair in "$@"; do
        key=${pair%%=*}
        value=${pair#*=}
        # JSON string quoting is valid YAML: backslashes and double quotes escaped.
        escaped=$(printf '%s' "$value" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')
        echo "      - name: $key"
        echo "        value: \"$escaped\""
      done
    } > "$manifest"
    kubectl create -f "$manifest" -o jsonpath='{.metadata.name}'
    echo
    ;;

  wait)
    name="$1"
    limit="${2:-3600}"
    waited=0
    phase=""
    while [ "$waited" -lt "$limit" ]; do
      phase=$(kubectl -n "$NS" get workflow "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
      case "$phase" in
        Succeeded|Failed|Error) break ;;
      esac
      sleep 5
      waited=$((waited + 5))
    done
    echo "$name: ${phase:-unknown} after ${waited}s"
    [ "$phase" = "Succeeded" ]
    ;;

  logs)
    name="$1"
    kubectl -n "$NS" logs -l "workflows.argoproj.io/workflow=$name,icefleet.io/role=coordinator" \
      -c main --tail=-1 || true
    phase=$(kubectl -n "$NS" get workflow "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    if [ "$phase" != "Succeeded" ]; then
      echo "--- workflow message: $(kubectl -n "$NS" get workflow "$name" -o jsonpath='{.status.message}')"
      echo "--- workers (last 40 lines each)"
      kubectl -n "$NS" logs -l "workflows.argoproj.io/workflow=$name,icefleet.io/role=worker" \
        -c main --tail=40 --prefix || true
    fi
    ;;

  leftovers)
    name="$1"
    limit="${2:-180}"
    waited=0
    while [ "$waited" -lt "$limit" ]; do
      pods=$(kubectl -n "$NS" get pods -l "workflows.argoproj.io/workflow=$name" -o name)
      services=$(kubectl -n "$NS" get services -o name | grep -F "service/$name-" || true)
      [ -z "$pods$services" ] && break
      sleep 5
      waited=$((waited + 5))
    done
    if [ -n "$pods$services" ]; then
      echo "$name: still present after ${waited}s:"
      echo "$pods"
      echo "$services"
      exit 1
    fi
    echo "$name: no pods and no Service left (after ${waited}s)"
    ;;

  status)
    kubectl -n "$NS" get workflows,pods,services,configmaps,secrets 2>&1
    ;;

  *)
    echo "node.sh apply | secrets | jobs | workers up|down | submit k=v... | wait NAME | logs NAME | leftovers NAME | status" >&2
    exit 2
    ;;
esac

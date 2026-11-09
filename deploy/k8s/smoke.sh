#!/usr/bin/env sh
# End-to-end check of the Kubernetes deployment: run the smoke Job, then
# read the run back through the load-balanced apiserver Service and the
# dashboard. Exits nonzero on any failure.
set -eu
NS=evalrunner
kubectl -n $NS wait --for=condition=available --timeout=180s deploy --all
kubectl -n $NS delete job smoke --ignore-not-found
kubectl apply -f "$(dirname "$0")/smoke-job.yaml"
kubectl -n $NS wait --for=condition=complete --timeout=180s job/smoke
kubectl -n $NS logs job/smoke | tee /tmp/smoke.json

kubectl -n $NS port-forward svc/apiserver 18080:8080 >/dev/null 2>&1 &
PF1=$!
kubectl -n $NS port-forward svc/web 13000:3000 >/dev/null 2>&1 &
PF2=$!
trap 'kill $PF1 $PF2 2>/dev/null || true' EXIT
for i in $(seq 1 30); do curl -sf localhost:18080/healthz >/dev/null && curl -sf localhost:13000 >/dev/null && break; sleep 1; done

RUN=$(curl -sf localhost:18080/api/runs | python3 -c 'import json,sys; r=[x for x in json.load(sys.stdin)["runs"] if x["suite"]=="k8s-smoke"]; print(r[0]["run_id"])')
curl -sf "localhost:18080/api/runs/$RUN" | python3 -c '
import json, sys
s = json.load(sys.stdin)
print("run", s["run_id"], "done", s["done"], "succeeded", s["succeeded"], "cached", s["cached"], "failed", s["failed"])
assert s["done"] and s["failed"] == 0 and s["succeeded"] + s["cached"] == 2000, s
'
curl -sf localhost:13000 | grep -q k8s-smoke
echo "dashboard renders the run"
echo "attempts per worker pod:"
kubectl -n $NS exec deploy/postgres -- psql -U eval -tAc \
  "SELECT worker_id, count(*) FROM attempts WHERE run_id = '$RUN' GROUP BY 1 ORDER BY 1"
echo ok

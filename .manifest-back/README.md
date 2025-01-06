

+ for now this is deployed manually (if postgres Dockerfile was updated, remember to run build_and_push.sh)

```

cd ..
kubectl apply -f .manifest-back

```


kubectl rollout restart deployment fi-postgres-deployment -n default


kubectl rollout restart deployment fi-luigi-deployment -n default
kubectl rollout restart deployment fi-enqueue-deployment -n default
kubectl rollout restart deployment fi-beat-deployment -n default
kubectl rollout restart deployment fi-worker-deployment -n default
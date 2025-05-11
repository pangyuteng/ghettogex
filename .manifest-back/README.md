

+ for now this is deployed manually (if postgres Dockerfile was updated, remember to run build_and_push.sh)

```

cd ..
kubectl apply -f .manifest-back

```

+ restart deployments

```
kubectl rollout restart deployment postgres-deployment -n gg
kubectl rollout restart deployment adminer-deployment -n gg
kubectl rollout restart deployment rabbitmq-deployment -n gg
kubectl rollout restart deployment redis-deployment -n gg


kubectl rollout restart deployment luigi-deployment -n gg
kubectl rollout restart deployment enqueue-deployment -n gg
kubectl rollout restart deployment beat-deployment -n gg
kubectl rollout restart deployment worker-default-deployment -n gg
kubectl rollout restart deployment worker-stream-deployment -n gg

```

+ tail logs

```

kubectl logs -l app=worker-default -n gg -f --max-log-requests 12 --tail=2000
kubectl logs -l app=worker-stream -n gg -f --max-log-requests 12 --tail=2000


```
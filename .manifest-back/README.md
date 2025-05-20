

+ for now this is deployed manually (if postgres Dockerfile was updated, remember to run build_and_push.sh)

```

cd ..
kubectl apply -f .manifest-back

```

+ restart deployments

```
kubectl rollout restart deployment postgres -n gg
kubectl rollout restart deployment adminer -n gg
kubectl rollout restart deployment rabbitmq -n gg
kubectl rollout restart deployment redis -n gg


kubectl rollout restart deployment luigi -n gg
kubectl rollout restart deployment enqueue -n gg
kubectl rollout restart deployment beat -n gg
kubectl rollout restart deployment worker-default -n gg
kubectl rollout restart deployment worker-stream -n gg

```

+ tail logs

```

kubectl logs -l app=worker-default -n gg -f --max-log-requests 12 --tail=2000
kubectl logs -l app=worker-stream -n gg -f --max-log-requests 12 --tail=2000


```
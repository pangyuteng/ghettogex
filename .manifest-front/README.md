

+ in case you want to deploy manually:

```

cd ..
kubectl apply -f .manifest-front

kubectl rollout restart deployment fi-app-deployment -n default
kubectl rollout restart deployment fi-cache-deployment -n default


```

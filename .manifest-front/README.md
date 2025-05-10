

+ in case you want to deploy manually:

```

kubectl create namespace gg

metadata:
    namespace: gg

cd ..
kubectl apply -f .manifest-front

kubectl rollout restart deployment fi-app-deployment -n default


```

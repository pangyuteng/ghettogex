

+ in case you want to deploy manually:

```

kubectl create namespace gg

metadata:
    namespace: gg

cd ..
kubectl apply -f .manifest-front

kubectl rollout restart deployment flask -n gg


kubectl logs -l app=flask -n gg -f --max-log-requests 12 --tail=20

```

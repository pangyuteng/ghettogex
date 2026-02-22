
```

cd ..

kubectl apply -f .manifest-volume

kubectl get persistentvolumeclaim -n gg

sudo su
kubectl port-forward --address 0.0.0.0 svc/longhorn-frontend -n longhorn-system 80:80 

```
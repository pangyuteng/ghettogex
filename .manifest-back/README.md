+ init project kubectl setup notes:



+ [x] create new machine for runner, setup access to kube cluster

  + create new vm "runner" in "happyfeet" see `queensburymanning/runner/README.md`

  + [x] install self-hosted github runner

https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/adding-self-hosted-runners
https://github.com/pangyuteng/fi.aigonewrong.com/settings/actions/runners
https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/configuring-the-self-hosted-runner-application-as-a-service


+ [x] setup bare miminal cicd to deploy to ghettogex.aigonewrong.com

```

kubectl create secret docker-registry ghcr-login-secret --docker-server=https://ghcr.io --docker-username=${{ github.actor }} --docker-password=${{ secrets.GITHUB_TOKEN }}

kubectl create secret docker-registry registry-credentials --docker-server=docker.io \
--docker-username=<username> --docker-password=<token> --dry-run=client \
-o yaml > registry-credentials.yml

kubectl apply -f registry-credentials.yml

kubectl get secrets registry-credentials -n default -o json |  jq 'del(.metadata["namespace","creationTimestamp","resourceVersion","selfLink","uid","annotations"])' | kubectl apply -n gg -f -

IS_TEST=
TASTYTRADE_USERNAME=
TASTYTRADE_PASSWORD=

kubectl create secret generic tasty-env --from-env-file=.tasty

kubectl get secret tasty-env -o yaml

kubectl get secrets tasty-env -n default -o json |  jq 'del(.metadata["namespace","creationTimestamp","resourceVersion","selfLink","uid","annotations"])' | kubectl apply -n gg -f -



```


+ in proxmox host6, prepare for postgres

  + setup ssd passthrough

  + tweaked host6 ram cpu

+ label host6 to have ssd

kubectl label nodes host6 disktype=ssd


+ for now this is deployed manually (if postgres Dockerfile was updated, remember to run build_and_push.sh)

```

cd ..
kubectl apply -f .manifest-volume
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
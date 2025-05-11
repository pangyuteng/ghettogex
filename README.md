# fi.aigonewrong.com


+ [x] create new machine for runner, setup access to kube cluster

  + create new vm "runner" in "happyfeet"

  + resize disk to 96GB https://pve.proxmox.com/wiki/Resize_disks

  + install kubectl, add ~/.kube/config

+ [x] install self-hosted github runner

    + install runner under aigonewrong@runner:~

https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/adding-self-hosted-runners
https://github.com/pangyuteng/fi.aigonewrong.com/settings/actions/runners
https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/configuring-the-self-hosted-runner-application-as-a-service

    
+ [x] setup bare miminal cicd to deploy to fi.aigonewrong.com


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

+ [x] main page to show SPX, VIX and SPX gex

+ [x] cleanup shit, moved scratch code to below

```
/mnt/hd1/code/public-misc/finance/options/iv-surface-plot
https://github.com/pangyuteng/public-misc/tree/main/finance/options/iv-surface-plot
/mnt/hd1/code/github/hello-cloud/kube-volume
https://github.com/pangyuteng/hello-cloud/tree/main/kube-volume
papaya-flask-celery/render-pdf-gradio
https://github.com/pangyuteng/papaya-flask-celery/tree/master/render-pdf-gradio
```

+ [ ] show gex given ticker

+ [ ] overallmarket gex??? (SPX+SPY+(weighted,but-how?)tickers-gex)

https://polygon.io/options
https://x.com/ag_trader
i'll @ you if i make any (ugly) frontend/js progress.


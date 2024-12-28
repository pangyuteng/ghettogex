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

    
+ [ ] setup bare miminal cicd to deploy to fi.aigonewrong.com


kubectl create secret docker-registry ghcr-login-secret --docker-server=https://ghcr.io --docker-username=${{ github.actor }} --docker-password=${{ secrets.GITHUB_TOKEN }}


kubectl create secret docker-registry registry-credentials --docker-server=docker.io \
--docker-username=<username> --docker-password=<token> --dry-run=client \
-o yaml > registry-credentials.yml

kubectl apply -f registry-credentials.yml


+ [ ] main page to show SPX, VIX and SPX gex

+ [ ] show gex given ticker

+ [ ] overallmarket gex??? (SPX+SPY+(weighted,but-how?)tickers-gex)

https://polygon.io/options
https://x.com/ag_trader
i'll @ you if i make any (ugly) frontend/js progress.


prototype shit moved:

/mnt/hd1/code/public-misc/finance/options/iv-surface-plot/proto


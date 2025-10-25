
+ create tunnel Zero Trust->Networks->Tunnels->Select Cloudflared


name: `prod1-k3s`


+ stash the token.
this used to be in namespace default  we now move it to namespace gg

```
TOKEN="asdfasdf"
kubectl create secret generic cloudflare-token --namespace gg --from-literal=CLOUDFLARE_TOKEN="$TOKEN"    -o yaml --dry-run=client | tee cloudflare-token.yaml

kubectl apply -f cloudflare-token.yaml


```

+ create cloudflared

kubectl create -f cloudflared-deployment.yml

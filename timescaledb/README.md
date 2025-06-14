



```

+ switched to timescaledb, prior postgres files are stashed in `timescaledb/postgres`

https://github.com/timescale/timescaledb
https://gist.github.com/chrismckelt/efe8e3ed3ae9a61a07a67b9d3454b2dd
https://docs.timescale.com/self-hosted/latest/install/installation-docker
https://docs.timescale.com/api/latest/hypertable/create_table
https://docs.timescale.com/api/latest/hypertable/create_index


kubectl delete -f .manifest-back/deployment-postgres.yaml
kubectl delete -f .manifest-back/service-postgres.yaml

nfswavestorm:/mnt/hd1/data
sudo su
mkdir pgmount
chown -R 1000:1000 pgmount


kubectl apply -f .manifest-back/deployment-postgres.yaml
kubectl apply -f .manifest-back/service-postgres.yaml

kubectl describe pod postgres -n gg

kubectl logs -l app=postgres -n gg -f --max-log-requests 12 --tail=200

http://192.168.68.80:8080/?pgsql=postgres&username=postgres&db=postgres&ns=public


TODO:
use longhorn & pvc ... then HA
https://docs.timescale.com/self-hosted/latest/install/installation-kubernetes/

```


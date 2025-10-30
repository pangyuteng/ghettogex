
+ in proxmox tweaked host6 ram cpu

kubectl label nodes host6 disktype=ssd

kubectl delete -f .manifest-back/deployment-postgres.yaml

you need to stop postgres container and remove existing container
k3s crictl rmi --prune

cd timescaledb
bash build_and_push.sh 

kubectl apply -f .manifest-back/deployment-postgres.yaml


show max_connections;
show shared_buffers;



# postgres old stuff

+ postgres files are deleted, commit b85bd02a43372c52cbc869aaa90b97efe7ffa34b



# OLD NOTES, playing with longhorn`


```

+ 2025-10-29, query performance is slow to table `quote`, helpful link: https://blog.cloudflare.com/timescaledb-art

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

```

http://192.168.68.80:8080/?pgsql=postgres&username=postgres&db=postgres&ns=public



+ [x] use longhorn & pvc ... then HA

https://docs.timescale.com/self-hosted/latest/install/installation-kubernetes/

longhorn & pvc is not fast.
  see queensburymanning/k3s/test-longhorn-timescaledb


+ use local storage

```

kubectl get nodes

https://kubernetes.io/docs/tasks/configure-pod-container/configure-persistent-volume-storage/

https://kubernetes.io/docs/tasks/configure-pod-container/assign-pods-nodes-using-node-affinity/

# sudo sh -c "echo 'Hello from Kubernetes storage' > /mnt/kxg256gb/pgmount/index.html"

move timescaledb to host6 to use nvme 


+ in longhorn , reserve 100G to nvme in host5

kubectl get pod -o wide -n gg | grep post

kubectl label nodes host5 disktype=ssd

# delete label kubectl label nodes host5 disktype-

kubectl port-forward --address 0.0.0.0 pod/task-pv-pod -n default 80:80

kubectl apply -f tmp-redq-affinity.yaml

kubectl port-forward --address 0.0.0.0 pod/task-pv-pod -n default 5432:5432

kubectl port-forward --address 0.0.0.0 pod/task-pv-pod -n default 5432:5432
cd ghettogex/scratch/archive/psycopg-memory-profile
python pg_profiling.py

pvc with ssd (5sec) not faster than nfs (5 sec).
but docker is fast <2 sec.

```
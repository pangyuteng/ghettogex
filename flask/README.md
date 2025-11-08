

```

kubectl port-forward --address 0.0.0.0 svc/postgres -n gg 5432:5432
kubectl port-forward --address 0.0.0.0 svc/redis -n gg 6379:6379

docker run -it \
    --env-file=.env \
    -e CACHE_FOLDER="/tmp/sm-data/fi" \
    -e POSTGRES_URI="postgres://postgres:postgres@192.168.68.143:5432/postgres" \
    -e REDIS_URI="redis://192.168.68.143:6379/1" \
    -w $PWD -v /mnt:/mnt \
    -p 80:80 -p 8888:8888 fi-notebook:latest bash

python app.py 80

```
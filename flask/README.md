


```


# .env file content
IS_TEST=FALSE
TASTYTRADE_CLIENT_SECRET=
TASTYTRADE_REFRESH_TOKEN=
EXPECTED_HASH=
POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres
REDIS_URI=redis://192.168.68.143:6379/1


kubectl port-forward --address 0.0.0.0 svc/postgres -n gg 5432:5432
kubectl port-forward --address 0.0.0.0 svc/redis -n gg 6379:6379

docker run -it --env-file=.env \
    -w $PWD -v /mnt:/mnt \
    -p 80:80 -p 8888:8888 fi-notebook:latest bash

python app.py 80

```
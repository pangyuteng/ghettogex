



kubectl port-forward --address 0.0.0.0 svc/redis -n gg 6379:6379
kubectl port-forward --address 0.0.0.0 svc/rabbitmq -n gg 5672:5672


ssh hawktuah
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:latest-pg17

docker run -it \
-e CACHE_FOLDER="/mnt/hd1/data/fi" -e CACHE_TASTY_FOLDER="/mnt/hd1/data/tastyfi" \
-e POSTGRES_URI="postgres://postgres:password@192.168.68.156:5432/postgres" \
-e AMQP_URI="amqp://192.168.68.143:5672" \
-e REDIS_URI="redis://192.168.68.143:6379/1" \
-e C_FORCE_ROOT=true \
-w $PWD -v /mnt:/mnt  fi-notebook:latest bash

python create.py

python insert.py


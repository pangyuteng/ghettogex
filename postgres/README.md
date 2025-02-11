




+ postgres container is build and pushed manually via

```

bash build_and_push.sh

```

+ bump vm RAM to 32GB
+ update config, update and push container again, then restart kube postgres deployment

/mnt/hd1/data/pgdata/postgresql.conf

max_connections = 2000
shared_buffers = 72MB





+ postgres container is build and pushed manually via

```

bash build_and_push.sh

```

+ bump vm RAM to 32GB
+ update config, update and push container again, then restart kube postgres deployment

/mnt/hd1/data/pgdata/postgresql.conf

+  increase connection
    https://stackoverflow.com/questions/30778015/how-to-increase-the-max-connections-in-postgres
    max_connections = 2000
    shared_buffers = 72MB

    after config update
    you need to stop postgres container and remove existing container
    k3s crictl rmi --prune


+ update index to be tuple of (tstamp,ticker/event_symbol)
    https://stackoverflow.com/a/35541546/868736
    vacuum candle, quote, summary, greeks,timeandsale
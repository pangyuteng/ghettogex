




+ postgres container is build and pushed manually via

```

bash build_and_push.sh

```

+ bump vm RAM to 32GB
+ update config, update and push container again, then restart kube postgres deployment

/mnt/hd1/data/pgdata/postgresql.conf

+  increase connection
    
    https://stackoverflow.com/questions/30778015/how-to-increase-the-max-connections-in-postgres
    
    vim /mnt/hd1/data/pgdata/postgresql.conf

    copy postgres/postgresql.conf to  above

    after config update
    you need to stop postgres container and remove existing container
    k3s crictl rmi --prune

    https://pgtune.leopard.in.ua/?dbVersion=17&osType=linux&dbType=web&cpuNum=4&totalMemory=24&totalMemoryUnit=GB&connectionNum=2000&hdType=ssd
    
    max_connections = 2000
    shared_buffers = 6GB
    effective_cache_size = 18GB
    maintenance_work_mem = 1536MB
    checkpoint_completion_target = 0.9
    wal_buffers = 16MB
    default_statistics_target = 100
    random_page_cost = 1.1
    effective_io_concurrency = 200
    work_mem = 3139kB
    huge_pages = off
    min_wal_size = 1GB
    max_wal_size = 4GB
    max_worker_processes = 4
    max_parallel_workers_per_gather = 2
    max_parallel_workers = 4
    max_parallel_maintenance_workers = 2

    https://stackoverflow.com/questions/8288823/query-a-parameter-postgresql-conf-setting-like-max-connections

+ update index to be tuple of (tstamp,ticker/event_symbol)
    https://stackoverflow.com/a/35541546/868736
    
    cluster greeks_tstamp_ticker_index on greeks;
    cluster event_agg_dstamp_ticker_index on event_agg;
    cluster gex_strike_tstamp_ticker_index on gex_strike;
    cluster gex_net_tstamp_ticker_index on gex_net;

    vacuum candle, quote, summary, greeks, timeandsale
            await streamer.subscribe(Profile, streamer_symbols)
            await streamer.subscribe(TheoPrice, streamer_symbols)
            await streamer.subscribe(Underlying, [ticker])

    delete from profile, theoprice, underlying
    delete from candle, event, greeks, quote, summary, timeandsale, trade, gex_net, gex_strike, event_agg






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


+ [x] main page to show SPX, VIX and SPX gex

+ [x] cleanup shit, moved scratch code to below

```
/mnt/hd1/code/public-misc/finance/options/iv-surface-plot
https://github.com/pangyuteng/public-misc/tree/main/finance/options/iv-surface-plot
/mnt/hd1/code/github/hello-cloud/kube-volume
https://github.com/pangyuteng/hello-cloud/tree/main/kube-volume
papaya-flask-celery/render-pdf-gradio
https://github.com/pangyuteng/papaya-flask-celery/tree/master/render-pdf-gradio
```

+ [x] show gex given ticker

+ [x] overallmarket gex??? (SPX+SPY+(weighted,but-how?)tickers-gex)

https://polygon.io/options
https://x.com/ag_trader
i'll @ you if i make any (ugly) frontend/js progress.





```


sudo chown "aigonewrong:aigonewrong" -R .ipynb_checkpoints
sudo chown "aigonewrong:aigonewrong" -R tmp

docker run -it -u $(id -u):$(id -g) -w $PWD -v /mnt:/mnt -v $PWD/tmp:/.local -p 8888:8888 fi-notebook:latest bash

jupyter notebook --ip=*

docker run -it -u $(id -u):$(id -g) \
    --env-file=.env \
    -w $PWD -v /mnt:/mnt \
    fi-flask:latest bash


```






# logic to update OI by strike

```

    for every second
        for every contract  
            get prior second OI (or prior day)
            get prior second bid/ask size
            update OI

    ABOVE LOGIC WILL WORK BUT IS TOO FREAKING SLOW
```

# TODO:
+ [x] obtain intraday OI and gex from dxlink sub
+ [x] see json2parquet.py compute OI in vectorized mode per event_symbol
+ [x] see json2parquet.py async above per event_symbol
+ [x] investigate/compute intraday naive? GEX with with dxlink sub data.
+ [x] revive hello-tastytrade/dxlink-sub, to save to both json and postgres via kube.
    (utils/compute_intraday.py,utils/data_tasty.py,tasks.py)
+ [x] manual 2024-Q4 historical EOD download from UW. (/mnt/hd2/data/finance/bot-eod-zip)
+ [x] subscribe for a few expirations per ticker
+ [o] verify visualize second-freq OI and visualize GEX (scratch/plot_gex_strike_from_events.py)
+ [x] optimization
    sql: create index with tstamp -> 4 to lt 1 sec
    sql: use = as oppose to like 
    union all is fast, use asyncio and gather 5 queries -> 700ms to 250ms
    replace `getattr` with `dict.get` https://stackoverflow.com/questions/9790991/why-is-getattr-so-much-slower-than-self-dict-get
    replace for loop with series.apply
+ [x] patch oi compute bug, na needs to be replaced with 0 for numerical values.
+ [x] verify again GEX
      + compute and visulize gex oi from volume only
      + compute and visulize gex oi from bid/ask volume
      + compute and visulize gex oi from timeandsale
      volume only is what people say bouat naive gex
      oi-change from "candle bid/ask volume" vs timeanedsale is similar.
      get "candle bid/ask volume" - advantage: no need to subscribe timeanedsale, meaning you can subcribe to more expiration events?
      use timeanedsale if you want sub second level updates.
+ [x] enable (end of day) GEX surface plots with plotly js 
+ [x] shutoff/restart worker at market close to patch hanging stream worker
+ [todo] verify again GEX with gexbot-state
+ [o] testing live gex-strike
    + [o] monitor system performance for a week or two
    + [problem] LivePrices hangs, luigi task does not exit.
    + [x] Attempt to listen to 30 expirations to get DDOI
    + [x] added connection pool
    + [x] postgres optimization - increased connection, see postgres/README.md
    + [x] closed apool prior shutdown luigi task
    + [x] postgres optimization - tuple as index, see postgres/README.md
    + [x] async insert https://stackoverflow.com/questions/67944791/fastest-way-to-apply-an-async-function-to-pandas-dataframe
    + [x] added true_gex 
+ [x] verify again GEX
    + [x] made uw_gex_utils.py, verified GEX
          need contract_type_int, since while gamma is same for put and call
          once open interest can be tracked with the same method,
          to remain delta neutral, to long or short the underlying is different based on contract type.
+ [x] get `time` from below events, consider using time when inserting to events table?
    tables:
    candle, greeks, theoprice, timeandsale, underlying, trade
    # NOTE: TODO: datetime.datetime.fromtimestamp(row['time']//1000,tz=datetime.timezone.utc)
    # time is epoch at utc, later if you need to merge events
    Decided to to do this, since we want to gather local server stamp timeandsale 
    and per aggregated time slot, lump timeandsale  for oi
    using time, means you have to wait and guess, since events don't come in at fix frequency.

+ [x] for naive-gex, added more tickers, and show on main page.
+ [x] added cron job to do vacuum
+ [x] health status (latest tstamps from all event tables)

+ [x] postgres insert and query got slow...
    https://chriserwin.com/table-partitioning
    + [x] look into how to do table partition
    """
    https://stackoverflow.com/questions/78373494/large-number-of-partitions
    https://chriserwin.com/table-partitioning/
    CREATE TABLE orders_2020_07 PARTITION OF orders
        FOR VALUES FROM ('2020-07-01') TO ('2020-08-01');
    ALTER TABLE orders DETACH PARTITION orders_2020_06;
    """

+ [x] switch from postgres to timescaledb

+ [o] study hua volatility
    + when do you pick naive vs order-book vs iv-surface?
    + [x] naive method - price near bid/ask price.
    + [o] use order book (quote events) to determine bid/ask side 
      [x] postgres
      [ ] uw
    + [ ] use price to derive IV, create IV surface, then compute theo_price
      use diff between price vs theo_price to determine bid/ask side 
      ? why not juse use UnivariateSpline <-- tried no good!
      ? why do we HAVE to use modeles like SABR? docs/vol-surface/README.md
      A:   arb free and many firms use this for modeling price & vol!
      
    + ? how do you do above in near real time? also do this with UW data?
      A: instead of using quote event
    
    + for relatively large orders
      start with 2 sec lag
      query 1 sec in future for candle and timeandsale price
      to determine bid/ask side.
    + MAYBE/Think about for price at mid, use domokane/FinancePy ???
    
    + [ ] verify beween DXLINK, UW, and GEXBOT

+ [x] what the heck is gexbot convexity? https://www.youtube.com/watch?v=p8aafkGbXNk
      + [x] updated convexity definition

+ [x] insert call_dex, put_dex, call_gex, put_gex

+ [x] how about insert to redis as well for each second per day? and purging per day?
    solution: see timescaledb-materialized-view

+ [x] investigate uplot, to enable sub-second plot update
     
    https://github.com/pangyuteng/hello-htmx-quart/tree/main/uplot

    https://github.com/leeoniya/uPlot

+ [x] css flexbox for layout!
    https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_flexible_box_layout/Relationship_of_flexbox_to_other_layout_methods

+ [x] after viewing gex_net for whole day you have artifact where data lumps at appx 1-minute aggregate.

    + [x] maybe dxfeed or could be compute_intraday.py event agg join issue
        commit 539b471 "updated event_agg join order"

    + [x] likely unrelated to oom item. bump up tastytrade==10.2.3, just want to observe order flow.
        refresh_interval now using default
        timeandsale event comes in near real time.

    you get a drop in gex every minute.
    but when you visualize true_oi, oi doesn't drop
    seems like it is due to querying of (missing volatility) greeks event.

    + [x] compute_intraday patched to get get greeks using past 2 min as oppose to 1min.
      seems to resolve the 1-min spike
      commit 651268f


+ [x] investigate faster query (with timescale), to enable sub-second plot update

    https://github.com/pangyuteng/ghettogex.aigonewrong.com/blob/main/flask/utils/pg_queries.py

    + [o] use-timescaledb-materialized-view for aggregated view

    https://www.tigerdata.com/blog/materialized-views-the-timescale-way

    https://docs.tigerdata.com/use-timescale/latest/continuous-aggregates/create-a-continuous-aggregate/

+ [o] continue to monitor memory usage for workers
    for detail see see `scratch/psycopg2-memory-leak-issue`

    *** MEMORY LEAK CRITICAL ISSUE ***

+ [x] tastytrade version updated to tastytrade==10.2.3
    observations:
    VIX candle still around 4 bars per minute, but SPX is about 1 bar per second
    timeandsale and candle are still coming in at 1 min bolus

+ [x] add gexbot order flow GEX,DEX visualization

    https://www.cboe.com/insights/posts/volatility-insights-evaluating-the-market-impact-of-spx-0-dte-options

+ [ ] (for speed) volatility and greeks compute and caching service
      gex needs a realtime, or else we are viewing 1min-lagged greeks (`compute_intraday.py`)

    *** volatility is outdated CRITICAL ISSUE? ***

+ [ ] (for speed) make event_agg as hypertable and gex_strike and gex_net as materialize views.

+ [ ] compute next expiration gex_net,gex_strike seperatly.
      or add cron task to get true_oi and open_interest

+ [ ] investigate GEX Regime classification and future 1min,5min,10min,30min probability.
      use SqueezeMetrics paper
        
        https://www.wrighters.io/options-to-run-pandas-dataframe-apply-in-parallel/
        
        `scratch/gex-classify-dev.ipynb`
        
        awesome https://github.com/nalepae/pandarallel
        https://stackoverflow.com/questions/26784164/pandas-multiprocessing-apply
        https://stackoverflow.com/questions/45545110/make-pandas-dataframe-apply-use-all-cores
        https://stackoverflow.com/search?q=pandas+apply+parallel

+ [ ] how to assess/confirm trend and resistance?
      dex increase, gex increase.

+ [ ] DL for IV computation... and then... DL for trend and resistance...???
https://developer.nvidia.com/blog/accelerating-python-for-exotic-option-pricing/
0 DTE Pricing
https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4503344
https://quant.stackexchange.com/questions/77299/0dte-volatility-and-greeks


+ [ ] play sound during events.
    + [ ] flash crash
    
    echo $'\a'
    https://askubuntu.com/a/673759/231735
    https://stackoverflow.com/questions/9419263/how-to-play-audio

    + [ ] ideal setup for long
    + [ ] gex png and proposed direction, support/major levels.

+ [ ] ?automate daily download from UW, and parse data to put to postgres??? for EOD-DDOI

+ [ ] ideally you want to monitor OI (ask and bid seperately)
      each contract, if `summary OI` is 0, then start tracking candle bid ask volumes and/or timeandsale.

+ [ ] plot volatility, gex, surface as contour plot - optiondepth style
https://quant.stackexchange.com/questions/68164/mixed-greeks-in-python-how-plot-the-following

--
+ [ ] sideswap/aqua btc to L BTC and back to btc than coldstorage.
+ [ ] setup tor for exporsed btc node port

open interest folder structure (THIS TURNED OUT TO BE A NIGHTMARE, DONT SAVE SMALL FILES SYNC, IO BOTTLENECK!)

oi/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv
gex/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv

```



# logic to 

select * from quote where event_symbol = '.SPXW250530C5900'
order by tstamp

select * from summary where event_symbol = '.SPXW250530C5900'
order by tstamp

select * from summary where event_symbol = '.SPXW250602C6200'
order by tstamp

select * from timeandsale where event_symbol = '.SPXW250523C5890'
order by tstamp

select * from candle where event_symbol = '.SPXW250602C7000'
order by tstamp

select * from quote where event_symbol = '.SPXW250602C7000'
order by tstamp


```


docker run -it  \
-e CACHE_FOLDER="/mnt/hd1/data/fi" -e CACHE_TASTY_FOLDER="/mnt/hd1/data/tastyfi"  \
-e POSTGRES_URI="postgres://postgres:postgres@192.168.68.143:5432/postgres" \
-e REDIS_URI="redis://192.168.68.143:6379/1" \
-w $PWD -v /mnt:/mnt \
-p 80:80 -p 8888:8888 fi-notebook:latest bash

```
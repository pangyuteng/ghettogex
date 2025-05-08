

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
+ [ ] verify again GEX
+ [ ] ?automate daily download from UW, and parse data to put to postgres??? for EOD-DDOI
+ [ ] play sound during events.
    + [ ] flash crash
    + [ ] ideal setup for long
    + [ ] gex png and proposed direction, support/major levels.
+ [ ] ideally you want to monitor OI (ask and bid seperately)
      each contract, if `summary OI` is 0, then start tracking candle bid ask volumes and/or timeandsale.


--
+ [ ] sideswap/aqua btc to L BTC and back to btc than coldstorage.
+ [ ] setup tor for exporsed btc node port

open interest folder structure (THIS TURNED OUT TO BE A NIGHTMARE, DONT SAVE SMALL FILES SYNC, IO BOTTLENECK!)

oi/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv
gex/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv

```



# logic to 

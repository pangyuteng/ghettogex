

```


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
+ [o] verify visualize second-freq OI and visualize GEX (scratch/plot_gex_strike_from_events.py)
+ [ ] enable (end of day) GEX surface plots with plotly js 
+ [ ] ?automate daily download from UW, and parse data to put to postgres???
+ [ ] subscribe for a few expirations per ticker, then OI ultimately have a break down between sell/buy-side.
--
+ [ ] sideswap/aqua btc to L BTC and back to btc than coldstorage.
+ [ ] setup tor for exporsed btc node port

open interest folder structure (THIS TURNED OUT TO BE A NIGHTMARE, DONT SAVE SMALL FILES SYNC, IO BOTTLENECK!)

oi/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv
gex/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv

```



# logic to 

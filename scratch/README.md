

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
+ [o] obtain intraday OI and gex from dxlink sub
+ [x] see json2parquet.py compute OI in vectorized mode per event_symbol
+ [x] see json2parquet.py async above per event_symbol
+ [ ] investigate/compute intraday naive? GEX with with dxlink sub data.
+ [ ] verify visualize second-level OI and visualize GEX
+ [ ] revive hello-tastytrade/dxlink-sub, to save to both json and postgres via kube.
+ [o] manual 2024-Q4 historical EOD download from UW.
+ [ ] enable (end of day) GEX surface plots with plotly js 
+ [ ] ?automate daily download from UW, and parse data to put to postgres???
+ [ ] subscribe for a few expirations per ticker, then OI ultimately have a break down between sell/buy-side.
--
+ [ ] sidwap btc to L BTC and back to btc than coldstorage.

open interest folder structure (THIS TURNED OUT TO BE A NIGHTMARE, DONT SAVE SMALL FILES SYNC, IO BOTTLENECK!)

oi/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv
gex/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv

```



# logic to 

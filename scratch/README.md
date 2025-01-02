

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
+ compute OI in vectorized mode per event_symbol
+ async above per event_symbol
+ verify visualize second-level OI and visualize gex
+ revive hello-tastytrade/dxlink-sub, to save to both json and postgres via kube.
+ automate daily download from UW, and parse data to put to postgres???
+ subscribe for a few expirations per ticker, then OI ultimately have a break down between sell/buy-side.


open interest folder structure (THIS TURNED OUT TO BE A NIGHTMARE, DONT SAVE SMALL FILES SYNC, IO BOTTLENECK!)

oi/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv
gex/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv

```



# logic to 

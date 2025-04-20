# per tstamp 
# compute
# from py_vollib.ref_python.black_scholes_merton.implied_volatility import implied_volatility
#iv = implied_volatility(price, S, K, t, r, q, flag)

import pandas as pd
import matplotlib.pyplot as plt

for ticker,pq_file in [
    ("SPY","/mnt/hd1/data/uw-options-cache/SPY/2024-09-30.parquet.gzip"),
    ("SPX","/mnt/hd1/data/uw-options-cache/SPX/2024-09-30.parquet.gzip"),
    ]:
    df = pd.read_parquet(pq_file)
    print(df.underlying_symbol.value_counts())
    ok=df[['underlying_price','tstamp_sec']] 
    ok=ok.drop_duplicates()
    if ticker == 'SPY':
        plt.subplot(121)
    if ticker == 'SPX':
        plt.subplot(122)
    plt.plot(ok.tstamp_sec,ok.underlying_price)
plt.show()
plt.savefig("ok.png")

"""
to get underlying price:
+ tastytrade dxfeed Candle historical stream not available
+ yfinance not stable
+ maybe derive from option price?
+ or use SPY price to derive SPX? this is likely possible
  but you still need daily open price for SPX

docker run -it -u $(id -u):$(id -g) --env-file=.env \
    -w $PWD -v /mnt:/mnt fi-notebook:latest bash

python get_historical_price.py

> stock = yf.Ticker("SPX")
>>> stock.history("all")

"""


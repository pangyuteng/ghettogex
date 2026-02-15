

+ in `app.py` make a home page `/` and show:

    it needs the following params:

    + `main-ticker`: this single ticker will show charts using those specified in `main-charts`
        default value: `SPX`
    
    + `main-charts`: use to specify charts for `main-ticker`
        default value: `price,volatility,dexflow,convexity,gex,gexflow,call-order-imbalance,put-order-imbalance,call-last-x-min,put-last-x-min`

    `other-tickers`: these tickers, seperated by comma, will show charts using those specified in `other-charts`
        default value: `SPY,QQQ,NDX`

    `other-charts`:
        default value: `volatility,convexity`

+ additional notes:

    + keep `/debug` as is!

    + focus on `app.py` and `/templates/**/*.html`

    + docker container `admiring_greider` available for you to test `app.py`
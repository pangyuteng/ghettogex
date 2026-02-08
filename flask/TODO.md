

In app.py we have a single page app with displaying multiple charts associated to 4 tickers.
Modify the front end code so code is more modular, continue using html,quart,jinja2 templating and htmx is necessary.
for plotting continue using uplot.js

the user will be using the application by providing the following params in the url, for example:

localhost/?tickers=SPX,SPY,QQQ,NDX&charts=convexity,volatility,price,gex,dexflow,gexflow,convexityflow,call-order-imbalance,put-order-imbalance,call-last-x-min,put-last-x-min

the page will display a 4x11 grid, the 4 represents the 4 tickers mentioned in the `tickers` param, ticker count could vary,
and the 11 will be the charts mentioned in the `charts` param.

the corresponding chart's query (in `utils/postgres_utils.py`) and html/js example is provided below:

+ convexity

    + query: INTERVAL_CONVEXITY_QUERY

    + html/js: templates/index-convexity.html, also refer to function `updateChartConvexitySPX`.


+ volatility

    + query: GREEKS_QUERY

    + html/js: templates/index-volatility.html, also refer to function `updateChartVolatility`.

+ price

    + query: CANDLE_1MIN_QUERY

    + html/js: templates/index-spx-vix.html, also refer to function `updateChartPrices`.

+ gex

    + query: LATEST_GEX_STRIKE_QUERY

    + html/js: index-gex.html, also refer to function `updateChartGex`.

+ dexflow

    + query: GEX_CONVEXITY_1DAY_QUERY

    + html/js: templates/index-dex.html, also refer to function `updateChartDex`.

+ gexflow

    + query: GEX_CONVEXITY_1DAY_QUERY

    + html/js: index-net-diff.html, also refer to function `updateChartGexDiff`.

+ convexityflow

    + query: GEX_CONVEXITY_1DAY_QUERY

    + html/js: index-net-diff.html, also refer to function `updateChartConvexityDiff`.

+ call-order-imbalance

    + query: ORDER_IMBALANCE_QUERY

    + html/js: templates/index-orderimbalance.html, also refer to function `updateChartCallOrderImbalance`.

+ put-order-imbalance

    + query: ORDER_IMBALANCE_QUERY

    + html/js: templates/index-orderimbalance.html, also refer to function `updateChartPutOrderImbalance`.

+ call-last-x-min

    + query: ORDER_IMBALANCE_LASTXMIN_QUERY

    + html/js: templates/index-orderimbalance.html , also refer to function `updateChartCallOrderImbalanceZoomIn`.

+ put-last-x-min

    + query: ORDER_IMBALANCE_LASTXMIN_QUERY

    + html/js: templates/index-orderimbalance.html, also refer to function `updateChartPutOrderImbalanceZoomIn`.



If you need a docker environment to spin up app.py, i can provide you a running docker container and app.py password
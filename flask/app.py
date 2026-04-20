import os
import json
import argparse
import traceback
import time
import asyncio
import pytz
import datetime
import tempfile
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import cm
import seaborn as sns
import matplotlib.dates as mdates
import base64

from jinja2 import Environment, FileSystemLoader

from quart import (
    Quart,
    websocket,
    render_template,
    request,
    jsonify,
    stream_with_context,
    redirect,
    url_for,
)

from quart_auth import (
    QuartAuth,
    AuthUser,
    current_user,
    login_required,
    login_user,
    logout_user,
    Unauthorized,
)

from utils.misc import (
    EXPECTED_HASH,
    check_password,
    get_market_open_close,
    nyse,
    is_market_open,
    now_in_new_york,
)

from utils.postgres_utils import (
    apostgres_execute,
    psycopg_pool,
    postgres_uri,
)

from utils.pg_queries import (
    LATEST_GEX_STRIKE_QUERY,
    CANDLE_1MIN_QUERY,
    CANDLE_1MIN_SINGLE_QUERY,
    CANDLE_1MIN_PRICE_QUERY,
    ORDER_IMBALANCE_QUERY,
    ORDER_IMBALANCE_LASTXMIN_QUERY,
    CANDLE_QC_QUERY,
    QUOTE_1MIN_QUERY,
    INTERVAL_CONVEXITY_QUERY,
    CONVEXITYDX_QUERY,
    GREEKS_QUERY,
    GEX_CONVEXITY_1DAY_QUERY,
    PRICE_1SEC_QUERY,
    VOLUME_1SEC_QUERY,
    PRICE_1MIN_QUERY,
    VOLUME_1MIN_QUERY,
    PRICE_5MIN_QUERY,
    VOLUME_5MIN_QUERY,
    CONTRACT_VOLUME_1MIN_QUERY,
)

from utils.data_tasty import (
    a_get_equity_data,
    a_get_equity_data_session_reuse
)

et_tz = "America/New_york"

TICKER_REGISTRY = {
    'SPX':  {'options_ticker': 'SPXW', 'companion': 'VIX'},
    'NDX':  {'options_ticker': 'NDXP', 'companion': 'VIX'},
    'SPY':  {'options_ticker': 'SPY',  'companion': 'VIX'},
    'QQQ':  {'options_ticker': 'QQQ',  'companion': 'VIX'},
}
VALID_CHARTS = ['price','convexity','volatility','gex','dexflow','gexflow',
                'convexityflow','call-order-imbalance','put-order-imbalance',
                'call-last-x-min','put-last-x-min']
DEFAULT_TICKERS = ['SPX','SPY','QQQ','NDX']
DEFAULT_CHARTS = VALID_CHARTS[:]

DEFAULT_MAIN_TICKER = 'SPX'
DEFAULT_MAIN_CHARTS = [
    'price','dexflow','gexflow','gex',
    'call-order-imbalance','put-order-imbalance',
    'call-last-x-min','put-last-x-min',
]
DEFAULT_OTHER_TICKERS = ['SPX','SPY','QQQ','NDX']
DEFAULT_OTHER_CHARTS = ['volatility','convexity']
DEFAULT_GRID = '4x8'

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(THIS_DIR,"templates")
STATIC_DIR = os.path.join(THIS_DIR,"static")

def render_html(html_file,**kwargs):
    j2_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    return j2_env.get_template(html_file).render(**kwargs)

app = Quart(__name__,
    static_url_path='/static',
    static_folder='static',
    template_folder='templates',
)
app.config["QUART_AUTH_MODE"]="cookie"
app.config["QUART_AUTH_COOKIE_SECURE"]=False
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.secret_key = "dLxWOjuwlk2z0n2I4NgxaQ" # import secrets ; secrets.token_urlsafe(16)
auth_manager = QuartAuth(app)


# --- Processing helper functions ---

def process_price_data(rows, ticker):
    """Process CANDLE_1MIN_PRICE_QUERY results for a single ticker."""
    if rows is None or len(rows) == 0:
        raise ValueError(f"found no price data for {ticker}!")

    df = pd.DataFrame([dict(x) for x in rows])
    df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
    df = df.replace({np.nan: None})

    lst = [df[i].tolist() for i in ['tstamp','vix1d_close','vix9d_close','vix_close','ticker_close']]

    ticker_close_col = df.ticker_close
    vix_close_col = df.vix_close

    ticker_price = ticker_close_col.iloc[-1]
    companion_price_idx = vix_close_col.last_valid_index()
    companion_price = vix_close_col[companion_price_idx] if companion_price_idx is not None else None

    vix_open_idx = vix_close_col.first_valid_index()
    vix_open = vix_close_col[vix_open_idx] if vix_open_idx is not None else 0
    vem = vix_open/np.sqrt(252) if vix_open else 0

    ticker_open_idx = ticker_close_col.first_valid_index()
    ticker_open = ticker_close_col[ticker_open_idx] if ticker_open_idx is not None else 0
    likey_close_price_list = np.array([-1*vem,1*vem])
    likey_close_price_list = ticker_open+likey_close_price_list*0.01*ticker_open
    likey_close_price_list = likey_close_price_list.astype(float).tolist()

    plus_prct = (1+vem*0.01*2.5)
    minus_prct = (1-vem*0.01*2.5)
    ticker_mean = ticker_close_col.mean()
    spot_max_lim = ticker_mean*plus_prct # used by gex plot
    spot_min_lim = ticker_mean*minus_prct

    misc_plus_prct = (1+vem*0.01*2)
    misc_minus_prct = (1-vem*0.01*2)
    ticker_close = ticker_close_col[ticker_close_col.last_valid_index()]
    max_lim = ticker_close*misc_plus_prct # used by convexity plot
    min_lim = ticker_close*misc_minus_prct

    return {
        'prices': lst,
        'ticker_price': ticker_price,
        'companion_price': companion_price,
        'likey_close_price_list': likey_close_price_list,
        'min_lim': min_lim,
        'max_lim': max_lim,
        'spot_min_lim': spot_min_lim,
        'spot_max_lim': spot_max_lim,
    }


def process_gex_data(rows, spot_min_lim, spot_max_lim):
    """Process LATEST_GEX_STRIKE_QUERY results."""
    df = pd.DataFrame([dict(x) for x in rows])
    if len(df) == 0:
        raise LookupError("null gex query!")

    df = df[(df.strike>spot_min_lim) & (df.strike<spot_max_lim)].reset_index()
    df.gex = df.gex/1e9
    df['pos_gex'] = df.gex.where(df.gex>0)
    df['neg_gex'] = df.gex.where(df.gex<=0)
    df = df.replace({np.nan: None})

    gex_list = [df[i].tolist() for i in ['strike','pos_gex','neg_gex']]
    major_pos_gex_strike = df["strike"].iloc[df.gex.argmax(skipna=True)]
    major_neg_gex_strike = df["strike"].iloc[df.gex.argmin(skipna=True)]

    return {
        'gex_list': gex_list,
        'major_pos_gex_strike': major_pos_gex_strike,
        'major_neg_gex_strike': major_neg_gex_strike,
    }


def process_convexity_data(rows, min_lim, max_lim):
    """Process INTERVAL_CONVEXITY_QUERY results."""
    df = pd.DataFrame([dict(x) for x in rows])
    df = df[(df.strike>min_lim) & (df.strike<max_lim)].reset_index()
    df['convexity'] = df.gamma*df.order_imbalance
    df['pos_convexity'] = df.convexity.where(df.convexity>0)
    df['neg_convexity'] = df.convexity.where(df.convexity<=0)

    major_pos_convexity = df["strike"].iloc[df.convexity.argmax(skipna=True)]
    major_neg_convexity = df["strike"].iloc[df.convexity.argmin(skipna=True)]

    df = df.replace({np.nan: 0}) # avoid uplot mouse hover jitter, we use 0
    convexity_list = [df[i].tolist() for i in ['strike','pos_convexity','neg_convexity']]

    return {
        'convexity_list': convexity_list,
        'major_pos_convexity': major_pos_convexity,
        'major_neg_convexity': major_neg_convexity,
    }


def process_volatility_data(rows, spot_min_lim, spot_max_lim, spot_price):
    """Process GREEKS_QUERY results."""
    df = pd.DataFrame([dict(x) for x in rows])
    df = df[(df.strike>spot_min_lim) & (df.strike<spot_max_lim)].reset_index()

    df.loc[(df.contract_type == 'C')&(df.strike<spot_price), 'volatility'] = 0
    df.loc[(df.contract_type == 'P')&(df.strike>spot_price), 'volatility'] = 0
    df = df.replace({np.nan: 0}) # avoid uplot mouse hover jitter, we use 0

    cdf = df[df.contract_type=='C']
    pdf = df[df.contract_type=='P']
    volatility_list = [cdf.strike.tolist(),cdf.dx_volatility.tolist(),pdf.dx_volatility.tolist()]
    return {'volatility_list': volatility_list}


def process_flow_data(rows, market_open):
    """Process GEX_CONVEXITY_1DAY_QUERY results."""
    df = pd.DataFrame([dict(x) for x in rows])

    if len(df) > 5:
        # ignore the first 2 minutes, as gex data fluctuates as events flow in.
        df = df[df.tstamp > market_open+datetime.timedelta(minutes=1)].reset_index()

    df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
    df.gex = df.gex/1e9
    df.dex = df.dex/1e9
    df.call_dex = df.call_dex/1e9
    df.put_dex = df.put_dex/1e9

    df['gex_diff'] = df.gex.diff()
    df['convexity_diff'] = df.convexity.diff()
    df = df.replace({np.nan: None})

    gex_lst = [df[i].tolist() for i in ['tstamp','gex_diff','gex']]
    convexity_lst = [df[i].tolist() for i in ['tstamp','convexity','convexity_diff']]
    dex_lst = [df[i].tolist() for i in ['tstamp','spot_price','dex','call_dex','put_dex']]

    return {
        'dex': dex_lst,
        'gexdiff': gex_lst,
        'convexitydiff': convexity_lst,
    }


def _build_order_imbalance_lists(df, prices_lst):
    """Shared binning logic for order imbalance data."""
    filter_list = [
        df.order_imbalance<-200,
        (df.order_imbalance>=-200)&(df.order_imbalance<-100),
        (df.order_imbalance>=-100)&(df.order_imbalance<-50),
        (df.order_imbalance>=-50)&(df.order_imbalance<-25),
        (df.order_imbalance>=-25)&(df.order_imbalance<-10),
        (df.order_imbalance>=-10)&(df.order_imbalance<0),
        (df.order_imbalance>=0)&(df.order_imbalance<10),
        (df.order_imbalance>=10)&(df.order_imbalance<25),
        (df.order_imbalance>=25)&(df.order_imbalance<50),
        (df.order_imbalance>=50)&(df.order_imbalance<100),
        (df.order_imbalance>=100)&(df.order_imbalance<200),
        df.order_imbalance>=200,
    ]
    call_list = [[],]
    put_list = [[],]
    for row_filter in filter_list:
        row_tstamp = df.tstamp[row_filter&(df.contract_type=="C")].to_list()
        row_strike = df.strike[row_filter&(df.contract_type=="C")].to_list()
        call_list.append([row_tstamp,row_strike,[]])
        row_tstamp = df.tstamp[row_filter&(df.contract_type=="P")].to_list()
        row_strike = df.strike[row_filter&(df.contract_type=="P")].to_list()
        put_list.append([row_tstamp,row_strike,[]])
    # add ticker price line
    call_list.append([ prices_lst[0], prices_lst[-1], [] ])
    put_list.append([ prices_lst[0], prices_lst[-1], [] ])
    return call_list, put_list


def process_order_imbalance_data(rows, spot_min_lim, spot_max_lim, prices_lst):
    """Process ORDER_IMBALANCE_QUERY results."""
    df = pd.DataFrame([dict(x) for x in rows])
    df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
    df = df[(df.strike>=spot_min_lim) & (df.strike<=spot_max_lim)]
    df = df.dropna()
    call_list, put_list = _build_order_imbalance_lists(df, prices_lst)
    return {'call': call_list, 'put': put_list}


def process_order_imbalance_zoom_data(rows, spot_min_lim, spot_max_lim, prices_lst):
    """Process ORDER_IMBALANCE_LASTXMIN_QUERY results."""
    df = pd.DataFrame([dict(x) for x in rows])
    df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
    df = df[(df.strike>=spot_min_lim) & (df.strike<=spot_max_lim)]
    df = df.dropna()
    # use last 5 price points for the zoom view
    offset = -5
    zoom_prices = [ prices_lst[0][offset:], prices_lst[-1][offset:], [] ]
    call_list, put_list = _build_order_imbalance_lists(df, [zoom_prices[0], zoom_prices[1]])
    return {'call': call_list, 'put': put_list}


def process_expected_move_data(rows, spot_price):
    """Process QUOTE_1MIN_QUERY results."""
    df = pd.DataFrame([dict(x) for x in rows])
    if len(df) == 0:
        raise LookupError("null quote query!")
    df['mid_price'] = (df.last_bid_price+df.last_ask_price)/2.0
    cdf = df[(df.contract_type=="C")&(df.strike>=spot_price)].reset_index().iloc[:3].reset_index()
    pdf = df[(df.contract_type=="P")&(df.strike<=spot_price)].reset_index().iloc[-3:].reset_index()
    expected_move = ( \
        0.6*cdf.at[0,'mid_price']+0.3*cdf.at[1,'mid_price']*0.1*cdf.at[2,'mid_price'] + \
        0.6*pdf.at[2,'mid_price']+0.3*pdf.at[1,'mid_price']*0.1*pdf.at[0,'mid_price'] )
    return {
        'plus': spot_price+expected_move,
        'minus': spot_price-expected_move,
    }


def process_qc_data(rows, tstamp_utc):
    """Process CANDLE_QC_QUERY results."""
    df = pd.DataFrame([dict(x) for x in rows])
    latest_data_tstamp = df.tstamp.min()
    latest_data_tstamp_str = latest_data_tstamp.replace(tzinfo=pytz.timezone("UTC")).astimezone(tz=pytz.timezone(et_tz)).strftime("%Y-%m-%d %H:%M:%S et")
    qc_comment = "***STALE TSTAMP!***" if (tstamp_utc.replace(tzinfo=None)-latest_data_tstamp).total_seconds() > 60 else ""
    return {
        'qc_comment': qc_comment,
        'data_tstamp': latest_data_tstamp_str,
    }


@app.route("/ping")
async def ping():
    return jsonify("pong")

@app.route("/login",methods=["GET","POST"])
async def login():
    if request.method == "POST":
        try:
            data = await request.json
            form = await request.form
            username = form["username"]
            password = form["password"]
            if check_password(password, EXPECTED_HASH):
                login_user(AuthUser(username))
                return redirect(url_for("home"))
            else:
                return jsonify("failed"), 400
            #token = auth_manager.dump_token(username)
            #return {"token": token}
        except:
            app.logger.error(traceback.format_exc())
    else:
        return await render_template("login.html")
        return """
        <form method="POST">
        <input name="username">
        <input name="password" type="password">
        <input type="submit" value="login">
        </form>
        """

@app.route("/logout")
async def logout():
    logout_user()
    return redirect(url_for("login"))

@app.errorhandler(Unauthorized)
async def redirect_to_login(*_):
    return redirect(url_for("login"))

@app.route("/about")
@login_required
async def about():
    order_imbalance_bin_str = "[0,10,25,50,100,200]"
    return await render_template("about.html",order_imbalance_bin_str=order_imbalance_bin_str)

# used only for testing  tastytrade api token - only for docker compose with .env file specified
@app.route("/equity")
@login_required
async def get_equity():
    try:
        ticker = request.args.get("ticker",None)
        session_reuse = True if request.args.get("session_reuse","false") == "true" else False

        if ticker is None:
            raise ValueError("ticker can't be None!")
        if session_reuse:
            data = await a_get_equity_data_session_reuse(ticker)
        else:
            data = await a_get_equity_data(ticker)
        return jsonify(dict(data))
    except:
        return jsonify(traceback.format_exc()),401

# scratch/exploratory, used to understand gex, gex-bot's convexity...
@app.route("/scratch")
@login_required
async def scratch():
    if not await current_user.is_authenticated:
        return redirect(url_for("login"))
    dstamp = request.args.get("dstamp",None)
    market_status = None
    load_data = True
    try:
        if dstamp is None:

            tstamp_et = now_in_new_york()
            tstamp_utc = tstamp_et.astimezone(tz=pytz.timezone('UTC')).replace(tzinfo=None)
            dstamp = tstamp_et.strftime("%Y-%m-%d")

            nyse_schedule = nyse.schedule(start_date=dstamp, end_date=dstamp)

            try:
                market_open,market_close = get_market_open_close(dstamp,no_tzinfo=True)
            except:
                market_open,market_close = None, None

            if len(nyse_schedule) == 0:
                market_status = f"market is closed for specified day! {dstamp}"
                load_data = False
            elif tstamp_utc < market_open:
                market_status = f"market not open yet today. please reload page once market opens. {dstamp}"
                load_data = False
            elif tstamp_utc > market_close:
                market_status = f"market closed already today. {dstamp}"
        else:

            nyse_schedule = nyse.schedule(start_date=dstamp, end_date=dstamp)

            if len(nyse_schedule) == 0:
                market_status = f"market is closed for specified day! {dstamp}"
                load_data = False
    except:
        app.logger.error(traceback.format_exc())
        market_status = "unexepcted error!"
        load_data = False

    #tickers_param = request.args.get("tickers", None)
    tickers_param = request.args.get("tickers", "SPX")
    tickers = [t.strip().upper() for t in tickers_param.split(",")] if tickers_param else DEFAULT_TICKERS
    tickers = [t for t in tickers if t in TICKER_REGISTRY]

    charts_param = request.args.get("charts", None)
    charts = [c.strip().lower() for c in charts_param.split(",")] if charts_param else DEFAULT_CHARTS
    charts = [c for c in charts if c in VALID_CHARTS]

    return await render_template("scratch.html",dstamp=dstamp,load_data=load_data,
        market_status=market_status,tickers=tickers,charts=charts)


def _parse_tickers_charts(args):
    """Parse tickers and charts from websocket/request args."""
    tickers_param = args.get("tickers", None)
    tickers = [t.strip().upper() for t in tickers_param.split(",")] if tickers_param else DEFAULT_TICKERS
    tickers = [t for t in tickers if t in TICKER_REGISTRY]

    charts_param = args.get("charts", None)
    charts = [c.strip().lower() for c in charts_param.split(",")] if charts_param else DEFAULT_CHARTS
    charts = [c for c in charts if c in VALID_CHARTS]

    return tickers, charts


def _needs_query_group(charts, group_charts):
    """Check if any of the group_charts are in the requested charts list."""
    return any(c in charts for c in group_charts)

@app.websocket('/ws-scratch')
@login_required
async def ws_scratch():
    try:
        message = None
        dstamp = websocket.args.get("dstamp")
        tickers, charts = _parse_tickers_charts(websocket.args)

        async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=5,open=False) as apool:
            while True:
                try:
                    ret_dict = {'tickers': {}, 'meta': {}}

                    early = nyse.schedule(start_date=dstamp, end_date=dstamp)
                    if len(early) == 0:
                        message = "break-while-loop"
                        raise ValueError(f"market not open! {dstamp}")

                    tstamp_et = now_in_new_york()
                    tstamp_utc = tstamp_et.astimezone(tz=pytz.timezone('UTC')).replace(tzinfo=None)
                    market_open,market_close = get_market_open_close(dstamp,no_tzinfo=True)

                    if tstamp_utc < market_open:
                        message = "break-while-loop"
                    if tstamp_utc > market_close:
                        message = "break-while-loop"
                        tstamp_utc = market_close

                    timea = time.time()

                    # Build query list dynamically per ticker
                    query_keys = []  # list of (ticker, query_group) tuples
                    query_list = []  # corresponding coroutines

                    need_flow = _needs_query_group(charts, ['dexflow','gexflow','convexityflow'])
                    need_ordim = _needs_query_group(charts, ['call-order-imbalance','put-order-imbalance'])
                    need_ordim_zoom = _needs_query_group(charts, ['call-last-x-min','put-last-x-min'])
                    need_gex = 'gex' in charts
                    need_convexity = 'convexity' in charts
                    need_volatility = 'volatility' in charts

                    for ticker in tickers:
                        reg = TICKER_REGISTRY[ticker]
                        options_ticker = reg['options_ticker']

                        # Always fetch price data (needed for limits used by other charts)
                        query_keys.append((ticker, 'price'))
                        query_list.append(apostgres_execute(apool, CANDLE_1MIN_PRICE_QUERY, (dstamp, ticker, dstamp, dstamp, dstamp)))

                        # QC data (always fetch for first ticker only, use SPX-like ticker)
                        if ticker == tickers[0]:
                            query_keys.append((ticker, 'qc'))
                            query_list.append(apostgres_execute(apool, CANDLE_QC_QUERY, (ticker, tstamp_utc, options_ticker, tstamp_utc)))

                        # GEX strike data
                        if need_gex:
                            query_keys.append((ticker, 'gex'))
                            query_list.append(apostgres_execute(apool, LATEST_GEX_STRIKE_QUERY, (tstamp_utc, tstamp_utc, ticker, tstamp_utc, tstamp_utc, ticker)))

                        # Convexity data
                        if need_convexity:
                            query_keys.append((ticker, 'convexity'))
                            #query_list.append(apostgres_execute(apool, INTERVAL_CONVEXITY_QUERY, (options_ticker, dstamp, dstamp, tstamp_utc, options_ticker, dstamp, dstamp)))
                            query_list.append(apostgres_execute(apool, CONVEXITYDX_QUERY, (options_ticker, dstamp, dstamp, options_ticker, dstamp, dstamp)))

                        # Volatility (greeks)
                        if need_volatility:
                            query_keys.append((ticker, 'volatility'))
                            query_list.append(apostgres_execute(apool, GREEKS_QUERY, (options_ticker, dstamp, dstamp)))

                        # Expected move (quote data) - needed by convexity, gex, volatility charts
                        if need_convexity or need_gex or need_volatility:
                            query_keys.append((ticker, 'expected_move'))
                            query_list.append(apostgres_execute(apool, QUOTE_1MIN_QUERY, (dstamp, options_ticker, tstamp_utc)))

                        # Order imbalance (full day)
                        if need_ordim:
                            query_keys.append((ticker, 'order_imbalance'))
                            query_list.append(apostgres_execute(apool, ORDER_IMBALANCE_QUERY, (dstamp, options_ticker)))

                        # Order imbalance zoom (last x min)
                        if need_ordim_zoom:
                            query_keys.append((ticker, 'order_imbalance_zoom'))
                            query_list.append(apostgres_execute(apool, ORDER_IMBALANCE_LASTXMIN_QUERY, (options_ticker, dstamp, tstamp_utc)))

                        # Flow data (dex, gex diff, convexity diff)
                        if need_flow:
                            query_keys.append((ticker, 'flow'))
                            query_list.append(apostgres_execute(apool, GEX_CONVEXITY_1DAY_QUERY, (ticker, dstamp)))

                    gathered_res = await asyncio.gather(*query_list)

                    timeb = time.time()
                    duration_time = timeb-timea

                    # Build result_map keyed by (ticker, query_group)
                    result_map = {}
                    for i, key in enumerate(query_keys):
                        result_map[key] = gathered_res[i]

                    # Process results per ticker
                    for ticker in tickers:
                        ticker_data = {}

                        # Price data (always needed)
                        try:
                            price_result = process_price_data(result_map.get((ticker, 'price')), ticker)
                            ticker_data['price'] = price_result
                        except:
                            ticker_data['price'] = {
                                'prices': [], 'ticker_price': None, 'companion_price': None,
                                'likey_close_price_list': [], 'min_lim': 0, 'max_lim': 0,
                                'spot_min_lim': 0, 'spot_max_lim': np.inf,
                            }
                            app.logger.error(traceback.format_exc())

                        price_data = ticker_data['price']
                        spot_min_lim = price_data['spot_min_lim']
                        spot_max_lim = price_data['spot_max_lim']
                        min_lim = price_data['min_lim']
                        max_lim = price_data['max_lim']
                        spot_price = price_data['ticker_price']

                        # Expected move
                        if (ticker, 'expected_move') in result_map:
                            try:
                                rows = result_map[(ticker, 'expected_move')]
                                if rows is not None and spot_price is not None:
                                    em_result = process_expected_move_data(rows, spot_price)
                                    ticker_data['expected_move'] = em_result
                                else:
                                    ticker_data['expected_move'] = {'plus': None, 'minus': None}
                            except:
                                ticker_data['expected_move'] = {'plus': None, 'minus': None}
                                app.logger.error(traceback.format_exc())

                        # GEX
                        if (ticker, 'gex') in result_map:
                            try:
                                rows = result_map[(ticker, 'gex')]
                                if rows is not None:
                                    ticker_data['gex'] = process_gex_data(rows, spot_min_lim, spot_max_lim)
                                else:
                                    ticker_data['gex'] = {'gex_list': [[],[],[]], 'major_pos_gex_strike': None, 'major_neg_gex_strike': None}
                            except:
                                ticker_data['gex'] = {'gex_list': [[],[],[]], 'major_pos_gex_strike': None, 'major_neg_gex_strike': None}
                                app.logger.error(traceback.format_exc())

                        # Convexity
                        if (ticker, 'convexity') in result_map:
                            try:
                                rows = result_map[(ticker, 'convexity')]
                                if rows is not None:
                                    ticker_data['convexity'] = process_convexity_data(rows, min_lim, max_lim)
                                else:
                                    ticker_data['convexity'] = {'convexity_list': [], 'major_pos_convexity': None, 'major_neg_convexity': None}
                            except:
                                ticker_data['convexity'] = {'convexity_list': [], 'major_pos_convexity': None, 'major_neg_convexity': None}
                                app.logger.error(traceback.format_exc())

                        # Volatility
                        if (ticker, 'volatility') in result_map:
                            try:
                                rows = result_map[(ticker, 'volatility')]
                                if rows is not None and spot_price is not None:
                                    ticker_data['volatility'] = process_volatility_data(rows, spot_min_lim, spot_max_lim, spot_price)
                                else:
                                    ticker_data['volatility'] = {'volatility_list': []}
                            except:
                                ticker_data['volatility'] = {'volatility_list': []}
                                app.logger.error(traceback.format_exc())

                        # Flow data (dex, gex diff, convexity diff)
                        if (ticker, 'flow') in result_map:
                            try:
                                rows = result_map[(ticker, 'flow')]
                                if rows is not None:
                                    flow_result = process_flow_data(rows, market_open)
                                    ticker_data['dexflow'] = {'data': flow_result['dex']}
                                    ticker_data['gexflow'] = {'data': flow_result['gexdiff']}
                                    ticker_data['convexityflow'] = {'data': flow_result['convexitydiff']}
                                else:
                                    ticker_data['dexflow'] = {'data': []}
                                    ticker_data['gexflow'] = {'data': []}
                                    ticker_data['convexityflow'] = {'data': []}
                            except:
                                ticker_data['dexflow'] = {'data': []}
                                ticker_data['gexflow'] = {'data': []}
                                ticker_data['convexityflow'] = {'data': []}
                                app.logger.error(traceback.format_exc())

                        # Order imbalance (full day)
                        if (ticker, 'order_imbalance') in result_map:
                            try:
                                rows = result_map[(ticker, 'order_imbalance')]
                                if rows is not None:
                                    oi_result = process_order_imbalance_data(rows, spot_min_lim, spot_max_lim, price_data['prices'])
                                    ticker_data['call-order-imbalance'] = {'data': oi_result['call']}
                                    ticker_data['put-order-imbalance'] = {'data': oi_result['put']}
                                else:
                                    ticker_data['call-order-imbalance'] = {'data': []}
                                    ticker_data['put-order-imbalance'] = {'data': []}
                            except:
                                ticker_data['call-order-imbalance'] = {'data': []}
                                ticker_data['put-order-imbalance'] = {'data': []}
                                app.logger.error(traceback.format_exc())

                        # Order imbalance zoom (last x min)
                        if (ticker, 'order_imbalance_zoom') in result_map:
                            try:
                                rows = result_map[(ticker, 'order_imbalance_zoom')]
                                if rows is not None:
                                    oi_zoom_result = process_order_imbalance_zoom_data(rows, spot_min_lim, spot_max_lim, price_data['prices'])
                                    ticker_data['call-last-x-min'] = {'data': oi_zoom_result['call']}
                                    ticker_data['put-last-x-min'] = {'data': oi_zoom_result['put']}
                                else:
                                    ticker_data['call-last-x-min'] = {'data': []}
                                    ticker_data['put-last-x-min'] = {'data': []}
                            except:
                                ticker_data['call-last-x-min'] = {'data': []}
                                ticker_data['put-last-x-min'] = {'data': []}
                                app.logger.error(traceback.format_exc())

                        ret_dict['tickers'][ticker] = ticker_data

                    # QC data (from first ticker)
                    qc_key = (tickers[0], 'qc') if tickers else None
                    if qc_key and qc_key in result_map:
                        try:
                            rows = result_map[qc_key]
                            if rows is not None:
                                qc_result = process_qc_data(rows, tstamp_utc)
                                ret_dict['meta']['qc_comment'] = qc_result['qc_comment']
                                ret_dict['meta']['data_tstamp'] = qc_result['data_tstamp']
                            else:
                                ret_dict['meta']['qc_comment'] = "***STALE TSTAMP!***"
                                ret_dict['meta']['data_tstamp'] = "null"
                        except:
                            ret_dict['meta']['qc_comment'] = "***STALE TSTAMP!***"
                            ret_dict['meta']['data_tstamp'] = "null"
                            app.logger.error(traceback.format_exc())

                    ret_dict['meta']['server_tstamp'] = datetime.datetime.utcnow().astimezone(tz=pytz.timezone(et_tz)).strftime("%Y-%m-%d %H:%M:%S et")
                    ret_dict['meta']['duration_time'] = f"{duration_time:0.3f}sec"
                    ret_dict['meta']['error_status'] = None
                except:
                    ret_dict = {
                        'tickers': {},
                        'meta': {
                            'qc_comment': "unexpected error!!! ffff likely missing data, services down!",
                            'duration_time': None,
                            'data_tstamp': None,
                            'server_tstamp': datetime.datetime.utcnow().astimezone(tz=pytz.timezone(et_tz)).strftime("%Y-%m-%d %H:%M:%S et"),
                            'error_status': 'traceback:'+traceback.format_exc(),
                        }
                    }
                    app.logger.error(traceback.format_exc())

                await websocket.send_json(ret_dict)
                await asyncio.sleep(1)

                if message is not None:
                    app.logger.error(message)
                    break

    except asyncio.CancelledError:
        app.logger.error(traceback.format_exc())
        app.logger.error('Client disconnected')
        raise

@app.route("/")
@login_required
async def home():
    if not await current_user.is_authenticated:
        return redirect(url_for("login"))
    dstamp = request.args.get("dstamp",None)
    interval = request.args.get("interval","1min")
    market_status = None
    load_data = True
    try:
        if dstamp is None:
            tstamp_et = now_in_new_york()
            tstamp_utc = tstamp_et.astimezone(tz=pytz.timezone('UTC')).replace(tzinfo=None)
            dstamp = tstamp_et.strftime("%Y-%m-%d")
            nyse_schedule = nyse.schedule(start_date=dstamp, end_date=dstamp)
            try:
                market_open,market_close = get_market_open_close(dstamp,no_tzinfo=True)
            except:
                market_open,market_close = None, None
            if len(nyse_schedule) == 0:
                market_status = f"market is closed for specified day! {dstamp}"
                load_data = False
            elif tstamp_utc < market_open:
                market_status = f"market not open yet today. please reload page once market opens. {dstamp}"
                load_data = False
            elif tstamp_utc > market_close:
                market_status = f"market closed already today. {dstamp}"
        else:
            nyse_schedule = nyse.schedule(start_date=dstamp, end_date=dstamp)
            if len(nyse_schedule) == 0:
                market_status = f"market is closed for specified day! {dstamp}"
                load_data = False

    except:
        app.logger.error(traceback.format_exc())
        market_status = "unexepcted error!"
        load_data = False

    grid_cols, grid_rows = 4, 8
    if interval == "1sec":
        colorbar_png_file = volume_colorbar_1sec_file
    elif interval == "1min":
        colorbar_png_file = volume_colorbar_1min_file
    elif interval == "5min":
        colorbar_png_file = volume_colorbar_5min_file
    else:
        raise NotImplementedError()
    return await render_template("index.html", 
        dstamp=dstamp, interval=interval, load_data=load_data,
        colorbar_png_file=colorbar_png_file,
        market_status=market_status, grid_cols=grid_cols, grid_rows=grid_rows, 
        div_id_list = ["chart-SPX-expectedmove","chart-SPX-volume","chart-SPX-volatility"],
        ticker_charts={'SPX':['expectedmove','volume','volatility']})

def process_contractvolume(rows):
    try:
        df = pd.DataFrame([dict(x) for x in rows])
        df.tstamp = df.tstamp.apply(lambda x: x.replace(tzinfo=pytz.timezone("UTC")).astimezone(tz=pytz.timezone(et_tz)))
    except:
        df = pd.DataFrame([])
    return df

def process_price_data_with_expected_move(rows, ticker):
    if rows is None or len(rows) == 0:
        raise ValueError(f"found no price data for {ticker}!")

    df = pd.DataFrame([dict(x) for x in rows])
    df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
    df = df.replace({np.nan: None})

    lst = [df[i].tolist() for i in ['tstamp','vix1d_close','vix9d_close','vix_close','expected_move','ticker_close']]

    try:
        ticker_price = df.ticker_close[df.ticker_close.last_valid_index()]
        ticker_mean = df.ticker_close.mean()
    except:
        ticker_price = None
        ticker_mean = 0

    try:
        vix_price = df.vix_close[df.vix_close.last_valid_index()]
    except:
        vix_price = None

    try:
        vix9d_price = df.vix9d_close[df.vix9d_close.last_valid_index()]
    except:
        vix9d_price = None

    try:
        vix1d_price = df.vix1d_close[df.vix1d_close.last_valid_index()]
    except:
        vix1d_price = None

    try:
        expected_move = df.expected_move[df.expected_move.last_valid_index()]
        expected_move = np.round(expected_move,2)
    except:
        expected_move = None

    try:
        vix_open_idx = df.vix_close.first_valid_index()
        vix_open = df.vix_close[vix_open_idx] if vix_open_idx is not None else 0
        vix_expected_move = vix_open/np.sqrt(252) if vix_open else 0
    except:
        vix_expected_move = 100

    plus_prct = (1+vix_expected_move*0.01*2.5)
    minus_prct = (1-vix_expected_move*0.01*2.5)
    spot_max_lim = ticker_mean*plus_prct # used by gex plot
    spot_min_lim = ticker_mean*minus_prct

    return {
        'prices': lst,
        'ticker_price':ticker_price,
        'vix_price':vix_price,
        'vix9d_price': vix9d_price,
        'vix1d_price': vix1d_price,
        'expected_move': expected_move,
        'min_lim': spot_min_lim,
        'max_lim': spot_max_lim,
    }


volumecmap = cm.get_cmap('hot')
volume_colorbar_1sec_file = os.path.join('static','colorbar_volume_1sec.png')
if not os.path.exists(volume_colorbar_1sec_file):
    fig = plt.figure()
    ax = fig.add_axes([0.05, 0.80, 0.9, 0.1])
    maxval = 100
    ticks = np.linspace(0,maxval,5)
    norm = mpl.colors.Normalize(vmin=0, vmax=maxval)
    cbar = mpl.colorbar.ColorbarBase(ax,orientation='horizontal',
        cmap=volumecmap,
        norm=mpl.colors.Normalize(0, maxval),
        ticks=ticks
    )
    ax.tick_params(colors='gray',grid_color='gray', grid_alpha=0.5)
    plt.savefig(volume_colorbar_1sec_file,bbox_inches='tight',transparent=True)
volume_colorbar_1min_file = os.path.join('static','colorbar_volume_1min.png')
if not os.path.exists(volume_colorbar_1min_file):
    fig = plt.figure()
    ax = fig.add_axes([0.05, 0.80, 0.9, 0.1])
    maxval = 2000
    ticks = np.linspace(0,maxval,5)
    norm = mpl.colors.Normalize(vmin=0, vmax=maxval)
    cbar = mpl.colorbar.ColorbarBase(ax,orientation='horizontal',
        cmap=volumecmap,
        norm=mpl.colors.Normalize(0, maxval),
        ticks=ticks
    )
    ax.tick_params(colors='gray',grid_color='gray', grid_alpha=0.5)
    plt.savefig(volume_colorbar_1min_file,bbox_inches='tight',transparent=True)
volume_colorbar_5min_file = os.path.join('static','colorbar_volume_5min.png')
if not os.path.exists(volume_colorbar_5min_file):
    fig = plt.figure()
    maxval = 2000*5
    ax = fig.add_axes([0.05, 0.80, 0.9, 0.1])
    ticks = np.linspace(0,maxval,5)
    norm = mpl.colors.Normalize(vmin=0, vmax=maxval)
    cbar = mpl.colorbar.ColorbarBase(ax,orientation='horizontal',
        cmap=volumecmap,
        norm=mpl.colors.Normalize(0, maxval),
        ticks=ticks
    )
    ax.tick_params(colors='gray',grid_color='gray', grid_alpha=0.5)
    plt.savefig(volume_colorbar_5min_file,bbox_inches='tight',transparent=True)


def getrgba(value,minval,maxval,alpha,cmap):
    norm_val = np.clip( ((float(value)-minval)/(maxval-minval)) ,0,1)
    r,g,b,_ = [int(x*255) for x in volumecmap(norm_val)]
    alpha = 0.5
    rgba_str = f"rgba({r},{g},{b},{alpha})"
    #app.logger.error(rgba_str)
    return rgba_str

def process_volume_data(rows, expectedmove_data, spot_min_lim, spot_max_lim,interval):
    if interval == '1sec':
        minval,maxval,alpha = 0.,100.,0.5
    elif interval == '1min':
        minval,maxval,alpha = 0.,2000.,0.5
    elif interval == '5min':
        minval,maxval,alpha = 0.,2000*5.,0.5
    else:
        raise NotImplementedError()
    df = pd.DataFrame([dict(x) for x in rows])
    df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
    df = df[(df.strike>=spot_min_lim) & (df.strike<=spot_max_lim)]
    df = df[['tstamp','strike','volume']]
    df = df.pivot(columns='strike',index='tstamp',values='volume')
    df = df.replace({np.nan: 0})
    for col in df.columns:
       df[col]=df[col].apply(lambda x: [-1,col,x,getrgba(x,minval,maxval,alpha,volumecmap)])
    # NOTE: `-1` # placehold to get time so you get (time,strike,volume,color)

    tstamp_list = df.index.to_list()
    data = df.to_numpy()

    start_list = [x[1] for x in data[:,0].tolist()] # grab array of min spot price
    end_list = [x[1] for x in data[:,-1].tolist()] # grab array of max spot price
    # NOTE: start_list,end_list ensures double click zooms back to Y min&max range.

    data_list = data.tolist()
    mylist = [tstamp_list,start_list,end_list,data_list]
    
    price_tstamp_list = expectedmove_data['prices'][0]
    price_list = expectedmove_data['prices'][-1]
    try:
        assert(len(tstamp_list)==len(price_tstamp_list))
        mylist = [tstamp_list,start_list,end_list,price_list,data_list]
    except:
        app.logger.error(f"{len(tstamp_list)} {len(price_tstamp_list)}")
        mylist = [tstamp_list,start_list,end_list,end_list,data_list]
    return {
        'data': mylist,
    }

@app.route("/foobar")
@login_required
async def foobar():
    return await render_template("foobar.html")

@app.route("/debug")
@login_required
async def debug():
    dstamp = "2026-04-17"
    ticker = "SPX"
    regi = TICKER_REGISTRY[ticker]
    options_ticker = regi['options_ticker']
    query_keys = []
    query_list = []
    interval = '5min'
    async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=5,open=False) as apool:
        query_keys.append((ticker, 'expectedmove'))
        query_list.append(apostgres_execute(apool, PRICE_5MIN_QUERY, (dstamp, ticker,dstamp, ticker, dstamp, dstamp, dstamp)))
        query_keys.append((ticker, 'volume'))
        query_list.append(apostgres_execute(apool, VOLUME_5MIN_QUERY, (dstamp, options_ticker)))
        gathered_res = await asyncio.gather(*query_list)
    result_map = {}
    for i, key in enumerate(query_keys):
        result_map[key] = gathered_res[i]


    chart_type = 'volume'
    if chart_type == 'expectedmove':
        ret_dict = {}
        ticker_data = {}
        try:
            source_data = result_map.get((ticker, 'expectedmove'))
            ticker_data['expectedmove'] = process_price_data_with_expected_move(source_data, ticker)
        except:
            ticker_data['expectedmove'] = {
                'prices': [],
                'ticker_price': None,
                'vix_price': None,
                'min_lim': 0,
                'max_lim': np.inf,
            }
            app.logger.error(traceback.format_exc())
        ret_dict['tickers']={ticker:ticker_data}
        return await render_template("debug-expectedmove.html", dstamp=dstamp, ret_dict=ret_dict)

    if chart_type == 'volume':

        ret_dict = {}
        ticker_data = {}
        try:
            source_data = result_map.get((ticker, 'expectedmove'))
            ticker_data['expectedmove'] = process_price_data_with_expected_move(source_data, ticker)
        except:
            ticker_data['expectedmove'] = {
                'prices': [],
                'ticker_price': None,
                'vix_price': None,
                'vix9d_price': None,
                'vix1d_price': None,
                'expected_move': None,
                'min_lim': 0,
                'max_lim': np.inf,
            }
            app.logger.error(traceback.format_exc())
        app.logger.error(f"{ticker_data['expectedmove']['min_lim']},{ticker_data['expectedmove']['max_lim']}")
        try:
            source_data = result_map[(ticker, 'volume')]
            volume_result = process_volume_data(
                source_data,
                ticker_data['expectedmove'],
                ticker_data['expectedmove']['min_lim'],
                ticker_data['expectedmove']['max_lim'],
                interval)
            ticker_data['volume'] = {'data': volume_result['data']}
        except:
            ticker_data['volume'] = {'data': []}
            app.logger.error(traceback.format_exc())

        ret_dict['tickers']={ticker:ticker_data}
        return await render_template("debug-volume.html", dstamp=dstamp, ret_dict=ret_dict)


@app.websocket('/ws-main')
@login_required
async def ws_main():
    try:
        message = None
        dstamp = websocket.args.get("dstamp")
        ticker = websocket.args.get("ticker","SPX")
        interval = websocket.args.get("interval","1min") # 1sec, 1min, 5min

        async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=5,open=False) as apool:
            while True:
                try:
                    ret_dict = {'tickers': {}, 'meta': {}}

                    early = nyse.schedule(start_date=dstamp, end_date=dstamp)
                    if len(early) == 0:
                        message = "break-while-loop"
                        raise ValueError(f"market not open! {dstamp}")

                    tstamp_et = now_in_new_york()
                    tstamp_utc = tstamp_et.astimezone(tz=pytz.timezone('UTC')).replace(tzinfo=None)
                    market_open,market_close = get_market_open_close(dstamp,no_tzinfo=True)
                    
                    is_market_open = True
                    if tstamp_utc < market_open:
                        message = "break-while-loop"
                        is_market_open = False
                    if tstamp_utc > market_close:
                        message = "break-while-loop"
                        is_market_open = False
                        tstamp_utc = market_close

                    timea = time.time()

                    regi = TICKER_REGISTRY[ticker]
                    options_ticker = regi['options_ticker']

                    query_keys = []
                    query_list = []

                    query_keys.append((ticker, 'qc'))
                    query_list.append(apostgres_execute(apool, CANDLE_QC_QUERY, (ticker, tstamp_utc, options_ticker, tstamp_utc)))
                    query_keys.append((ticker, 'volatility'))
                    query_list.append(apostgres_execute(apool, GREEKS_QUERY, (options_ticker, dstamp, dstamp)))
                    if interval == "1sec":
                        starttime = tstamp_utc - datetime.timedelta(minutes=5)
                        query_keys.append((ticker, 'expectedmove'))
                        query_list.append(apostgres_execute(apool, PRICE_1SEC_QUERY, {"ticker":ticker,"endtime":tstamp_utc,"starttime":starttime}))
                        query_keys.append((ticker, 'volume'))
                        query_list.append(apostgres_execute(apool, VOLUME_1SEC_QUERY, {"ticker":options_ticker,"endtime":tstamp_utc,"starttime":starttime}))
                    elif interval == "1min":
                        query_keys.append((ticker, 'expectedmove'))
                        query_list.append(apostgres_execute(apool, PRICE_1MIN_QUERY, (dstamp, ticker,dstamp, ticker, dstamp, dstamp, dstamp)))
                        query_keys.append((ticker, 'volume'))
                        query_list.append(apostgres_execute(apool, VOLUME_1MIN_QUERY, (dstamp, options_ticker)))
                    elif interval == "5min":
                        query_keys.append((ticker, 'expectedmove'))
                        query_list.append(apostgres_execute(apool, PRICE_5MIN_QUERY, (dstamp, ticker,dstamp, ticker, dstamp, dstamp, dstamp)))
                        query_keys.append((ticker, 'volume'))
                        query_list.append(apostgres_execute(apool, VOLUME_5MIN_QUERY, (dstamp, options_ticker)))
                    else:
                        raise NotImplementedError()

                    query_keys.append((ticker, 'contractvolume'))
                    query_list.append(apostgres_execute(apool, CONTRACT_VOLUME_1MIN_QUERY, (dstamp, options_ticker, dstamp, ticker)))


                    gathered_res = await asyncio.gather(*query_list)

                    timeb = time.time()
                    duration_time = timeb-timea

                    result_map = {}
                    for i, key in enumerate(query_keys):
                        result_map[key] = gathered_res[i]
                    
                    ticker_data = {}
                    try:
                        source_data = result_map.get((ticker, 'contractvolume'))
                        ticker_data['contractvolume'] = process_contractvolume(source_data).to_html()
                    except:
                        ticker_data['contractvolume'] = ""

                    # process price and expectedmove
                    try:
                        source_data = result_map.get((ticker, 'expectedmove'))
                        ticker_data['expectedmove'] = process_price_data_with_expected_move(source_data, ticker)
                    except:
                        ticker_data['expectedmove'] = {
                            'prices': [],
                            'ticker_price': None,
                            'vix_price': None,
                            'min_lim': 0,
                            'max_lim': np.inf,
                        }
                        app.logger.error(traceback.format_exc())

                    # process option contract volume
                    if (ticker, 'volume') in result_map:
                        try:
                            source_data = result_map[(ticker, 'volume')]
                            volume_result = process_volume_data(
                                source_data,
                                ticker_data['expectedmove'],
                                ticker_data['expectedmove']['min_lim'], ticker_data['expectedmove']['max_lim'], interval)
                            ticker_data['volume'] = volume_result
                        except:
                            ticker_data['volume'] = {'data': []}
                            app.logger.error(traceback.format_exc())

                    # process Volatility
                    if (ticker, 'volatility') in result_map:
                        try:
                            source_data = result_map[(ticker, 'volatility')]
                            ticker_data['volatility'] = process_volatility_data(
                                source_data, 
                                ticker_data['expectedmove']['min_lim'],
                                ticker_data['expectedmove']['max_lim'],
                                ticker_data['expectedmove']['ticker_price']
                            )
                        except:
                            ticker_data['volatility'] = {'volatility_list': []}
                            app.logger.error(traceback.format_exc())

                    ret_dict['tickers']={ticker:ticker_data}
                    qc_key = (ticker, 'qc')
                    try:
                        rows = result_map[qc_key]
                        if rows is not None:
                            qc_result = process_qc_data(rows, tstamp_utc)
                            ret_dict['meta']['qc_comment'] = qc_result['qc_comment']
                            ret_dict['meta']['data_tstamp'] = qc_result['data_tstamp']
                        else:
                            raise ValueError("no data!?")
                    except:
                        ret_dict['meta']['qc_comment'] = "***STALE TSTAMP!***"
                        ret_dict['meta']['data_tstamp'] = "null"
                        app.logger.error(traceback.format_exc())

                    ret_dict['meta']['server_tstamp'] = datetime.datetime.utcnow().astimezone(tz=pytz.timezone(et_tz)).strftime("%Y-%m-%d %H:%M:%S et")
                    ret_dict['meta']['duration_time'] = f"{duration_time:0.3f}sec"
                    ret_dict['meta']['error_status'] = None
                    ret_dict['meta']['is_market_open'] = is_market_open
                    
                except:
                    ret_dict = {
                        'tickers': {},
                        'meta': {
                            'qc_comment': "unexpected error!!! ffff likely missing data, services down!",
                            'duration_time': None,
                            'data_tstamp': None,
                            'server_tstamp': datetime.datetime.utcnow().astimezone(tz=pytz.timezone(et_tz)).strftime("%Y-%m-%d %H:%M:%S et"),
                            'error_status': 'traceback:'+traceback.format_exc(),
                            'is_market_open': False,
                        }
                    }
                    app.logger.error(traceback.format_exc())

                await websocket.send_json(ret_dict)
                await asyncio.sleep(1)

                if message is not None:
                    app.logger.error(message)
                    break

    except asyncio.CancelledError:
        app.logger.error(traceback.format_exc())
        app.logger.error('Client disconnected')
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("port",type=int)
    parser.add_argument('-d', '--debug',action='store_true')
    args = parser.parse_args()
    app.run(debug=args.debug,host="0.0.0.0",port=args.port)

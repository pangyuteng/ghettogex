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
import matplotlib.pyplot as plt
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

from utils.misc import check_password, CACHE_FOLDER, get_market_open_close, nyse
from utils.data_cache import (
    BTC_TICKER,
    CBOEX_TICKER_LIST,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    OTHER_TICKER_LIST,
    BTC_MSTR_TICKER_LIST,
    USMARKET_TICKER,
    USMARKET_TICKER_LIST,
    HOME_TICKER_LIST_OF_LIST,
    get_cache_latest,
    is_market_open,
    now_in_new_york,
)

from utils.compute import (
    get_gex_df,
    compute_btc_gex,
    compute_us_market_gex,
)
from utils.postgres_utils import (
    apostgres_execute,
    psycopg_pool,postgres_uri,
)
from utils.pg_queries import (
    EVENT_STATUS_QUERY,
    LATEST_GEX_STRIKE_QUERY,
    LATEST_DAY_GEX_NET_QUERY,
    LATEST_ONE_MIN_GEX_STRIKE_QUERY,
    GEX_NET_1MIN_QUERY,
)
et_tz = "America/New_york"

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(THIS_DIR,"templates")

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
app.secret_key = "dLxWOjuwlk2z0n2I4NgxaQ" # import secrets ; secrets.token_urlsafe(16)
auth_manager = QuartAuth(app)

@app.route("/ping")
async def ping():
    return jsonify("pong")

EXPECTED_HASH = "$2b$12$XwjiaKcS34vUvQx1A.eA7.bXKNj3LoRAb4mfooiiz6BctaoESGTCC"

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
async def about():
    return await render_template("about.html")

@app.route("/links")
@login_required
async def links():
    return await render_template("links.html")

@app.route("/gexbots")
@login_required
async def gexbots():
    return await render_template("gexbots.html")

@app.route("/")
@login_required
async def home():
    if not await current_user.is_authenticated:
        return redirect(url_for("login"))
    return await render_template("index.html",listoflist=HOME_TICKER_LIST_OF_LIST)

@app.route("/eod-gex")
@login_required
async def eod_gex():
    ticker = request.args.get("ticker")
    return await render_template("eod-gex.html",
        ticker=ticker,
        ticker_list=','.join(BTC_MSTR_TICKER_LIST))

@app.websocket('/ws-eod-gex')
@login_required
async def ws_eod_gex():
    try:

        ticker = websocket.args.get("ticker",BTC_TICKER)
        livespotprice = True if websocket.args.get("livespotprice",'false') == 'true' else False

        if ticker == 'SPX':
            ticker_alt = '^SPX'
        elif ticker == 'NDX':
            ticker_alt = '^NDX'
        elif ticker == 'VIX':
            ticker_alt = '^VIX'
        else:
            ticker_alt = ticker

        now_et = now_in_new_york()
        year_stamp = datetime.datetime.strftime(now_et,'%Y')
        if ticker == USMARKET_TICKER:
            cache_folder = os.path.join(CACHE_FOLDER,"^SPX",year_stamp)
        else:
            cache_folder = os.path.join(CACHE_FOLDER,ticker_alt,year_stamp)
        daystamp_list = sorted(os.listdir(cache_folder))
        daystamp_list = [daystamp_list[-1]]
        while True:
            for daystamp in daystamp_list:
                tstamp = datetime.datetime.strptime(daystamp,'%Y-%m-%d')
                server_tstamp = now_in_new_york().strftime("%Y-%m-%d-%H-%M-%S-%Z")

                if ticker == BTC_TICKER:
                    spot_price, strike_df, expiration_df, surf_df, daily_price_df, data_tstamp = compute_btc_gex(tstamp=tstamp,enable_live=livespotprice)
                elif ticker == USMARKET_TICKER:
                    spot_price, strike_df, expiration_df, surf_df, daily_price_df, data_tstamp = compute_us_market_gex(tstamp=tstamp)
                else:
                    spot_price, strike_df, expiration_df, surf_df, daily_price_df, data_tstamp = get_gex_df(ticker_alt,tstamp=tstamp)

                app.logger.info(f'spot_price {spot_price}')
                surf_df = surf_df.pivot(index='expiration',columns='strike',values='GEX')
                surf_df = surf_df.fillna(value="null")
                strike_list = surf_df.columns.to_list()
                expiration_list = [x.strftime("%Y-%m-%d") for x in surf_df.index.to_list()]
                surf_list = surf_df.values.tolist()
                data_str = render_html("ws-eod-gex.html",
                    ticker=ticker,
                    strike_df=strike_df,
                    surf_list=surf_list,
                    spot_price=spot_price,
                    strike_list=strike_list,
                    expiration_list=expiration_list,
                    data_tstamp=data_tstamp,
                    server_tstamp=server_tstamp,
                )

                await websocket.send(data_str)
                if livespotprice is False:
                    await websocket.close(1)
                await asyncio.sleep(10)
    except asyncio.CancelledError:
        app.logger.error('Client disconnected')
        raise

@app.websocket('/ws-prices')
@login_required
async def ws_prices():
    try:
        while True:
            mysec = 5
            tstamp = now_in_new_york().strftime("%Y-%m-%d-%H-%M-%S-%Z")
            mydict = {}
            for ticker in CBOEX_TICKER_LIST:
                underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker)
                mydict[ticker.replace("^","")] = underlying_dict
                data_tstamp = options_df.last_trade_time.max()

            underlying_dict,options_df,last_json_file,_ = get_cache_latest(BTC_TICKER)
            mydict[BTC_TICKER] = underlying_dict
            
            data_str = render_html("prices.html",
                mydict=mydict,
                data_tstamp=data_tstamp,tstamp=tstamp,market_open=is_market_open())

            await websocket.send(data_str)
            await websocket.close(1000)
            await asyncio.sleep(mysec)
    except asyncio.CancelledError:
        app.logger.error('Client disconnected')
        raise
    # no return, means connection is kept open.

@app.route("/ticker/overview")
@login_required
async def overview():
    ticker = request.args.get("ticker")
    return redirect(url_for("eod_gex",ticker=ticker))
    return await render_template("overview.html",ticker=ticker)

@app.websocket("/ticker/daily-ws-gex-strike")
@login_required
async def daily_ws_gex_strike():
    try:
        now_et = now_in_new_york()
        year_stamp = datetime.datetime.strftime(now_et,'%Y')
        while True:
            ticker = websocket.args.get("ticker")
            if ticker == 'SPX':
                ticker_alt = '^SPX'
            elif ticker == 'NDX':
                ticker_alt = '^NDX'
            elif ticker == 'VIX':
                ticker_alt = '^VIX'
            else:
                ticker_alt = ticker

            if ticker == USMARKET_TICKER:
                cache_folder = os.path.join(CACHE_FOLDER,"^SPX",year_stamp)
            else:
                cache_folder = os.path.join(CACHE_FOLDER,ticker_alt,year_stamp)
            daystamp_list = sorted(os.listdir(cache_folder))
            daystamp_list = [daystamp_list[-1]]

            mysec = 5
            div_name = "div-"+ticker.replace("^","")
            server_tstamp = now_in_new_york().strftime("%Y-%m-%d-%H-%M-%S-%Z")
            for daystamp in daystamp_list:
                tstamp = datetime.datetime.strptime(daystamp,'%Y-%m-%d')
                if ticker == BTC_TICKER:
                    spot_price, strike_df, expiration_df, surf_df, daily_price_df, data_tstamp = compute_btc_gex(tstamp=tstamp)
                elif ticker == USMARKET_TICKER:
                    spot_price, strike_df, expiration_df, surf_df, daily_price_df, data_tstamp = compute_us_market_gex(tstamp=tstamp)
                else:
                    spot_price, strike_df, expiration_df, surf_df, daily_price_df, data_tstamp = get_gex_df(ticker_alt,tstamp=tstamp)

                xlim = strike_df.gex.abs().max()*1.5

                if len(daily_price_df) > 70:
                    price_dict = {
                        'tstamp_str': str(daily_price_df.timestamp.to_list()),
                        'value_str': str(daily_price_df.last_price.to_list()),
                    }
                else:
                    price_dict = {
                        'tstamp_str': "[]",
                        'value_str': "[]",
                    }
                data_str = render_html("gex-strike.html",
                    ticker=ticker,
                    df=strike_df,
                    spot_price=spot_price,
                    price_dict=price_dict,
                    xlim=xlim,
                    data_tstamp=data_tstamp,
                    server_tstamp=server_tstamp,
                    div_name=div_name)
                await websocket.send(data_str)
                await websocket.close(1000)
                await asyncio.sleep(mysec)
    except asyncio.CancelledError:
        app.logger.error('Client disconnected')
        raise

@app.route("/sec-gex")
@login_required
async def sec_gex():
    ticker = request.args.get("ticker")
    return await render_template("sec-gex.html",ticker=ticker)

@app.websocket('/ws-status')
@login_required
async def ws_status():
    try:
        while True:

            mysec = 1
            eastern = pytz.timezone('US/Eastern')
            utc = pytz.timezone('UTC')
            tstamp_et = now_in_new_york()
            ws_tstamp_utc = tstamp_et.astimezone(tz=utc)

            query_str = """
            (select 'timeandsale' as event_type, count(timeandsale_id) as id_count from timeandsale where tstamp > now() - interval '2 second')
            union all (
            select 'candle' as event_type, count(candle_id) as id_count from candle where tstamp > now() - interval '3 second'
            ) union all (
            select 'quote' as event_type, count(quote_id) as id_count from quote where tstamp > now() - interval '3 second'
            ) union all (
            select 'greeks' as event_type, count(greeks_id) as id_count from greeks where tstamp > now() - interval '60 second'
            ) union all (
            select 'gex_net' as event_type, count(gex_net_id) as id_count from gex_net where tstamp > now() - interval '3 second'
            ) union all (
            select 'gex_strike' as event_type, count(gex_strike_id) as id_count from gex_strike where tstamp > now() - interval '3 second'
            )"""
            query_args = ()
            fetched = await apostgres_execute(None,query_str,query_args)
            columns = ['event_type','id_count']
            try:
                df = pd.DataFrame([x for x in fetched])
                event_status_dict = {row.event_type:row.id_count for n,row in df.iterrows()}
            except:
                event_status_dict = {}
                app.logger.error(traceback.format_exc())

            data_str = render_html("ws-status.html",event_status_dict=event_status_dict)
            await websocket.send(data_str)
            await asyncio.sleep(mysec)

    except asyncio.CancelledError:
        app.logger.error(traceback.format_exc())
        app.logger.error('Client disconnected')
        raise

@app.websocket('/ticker/ws-sec-gex')
@login_required
async def ws_sec_gex():
    try:
        while True:
            ticker = websocket.args.get("ticker")
            mysec = 1

            eastern = pytz.timezone('US/Eastern')
            utc = pytz.timezone('UTC')
            tstamp_et = now_in_new_york()
            ws_tstamp_utc = tstamp_et.astimezone(tz=utc)

            query_str = """
            select * from gex_net where ticker = %s
            order by tstamp desc limit 1
            """
            query_args = (ticker,)
            fetched = await apostgres_execute(None,query_str,query_args)
            try:
                df = pd.DataFrame([x for x in fetched])
                tstamp_utc = df.tstamp.to_list()[-1]
                day_stamp = tstamp_utc.strftime("%Y-%m-%d")
            except:
                tstamp_utc = None
                day_stamp = None
                spot_price = -1


            net_query_str = "select * from gex_net where ticker = %s and tstamp = %s order by tstamp"
            strike_query_str = "select * from gex_strike where ticker = %s and tstamp = %s order by tstamp"
            net_day_query_str = "select * from gex_net where ticker = %s and tstamp >= %s order by tstamp"
            strike_day_query_str = "select * from gex_strike where ticker = %s and tstamp >= %s order by tstamp"

            if tstamp_utc:
                # TODO: use 1 query per table if you want to do scatter plots.
                #'net-day': {'query_str':net_day_query_str,'query_args':(ticker,day_stamp)},
                #'strike-day': {'query_str':strike_day_query_str,'query_args':(ticker,day_stamp)},
                query_dict = {
                    'net': {'query_str':net_query_str,'query_args':(ticker,tstamp_utc)},
                    'strike': {'query_str':strike_query_str,'query_args':(ticker,tstamp_utc)},
                    'strike-1min': {'query_str':strike_query_str,'query_args':(ticker,tstamp_utc-datetime.timedelta(minutes=1))},
                    'strike-5min': {'query_str':strike_query_str,'query_args':(ticker,tstamp_utc-datetime.timedelta(minutes=5))},
                    'strike-10min': {'query_str':strike_query_str,'query_args':(ticker,tstamp_utc-datetime.timedelta(minutes=10))},
                    'strike-15min': {'query_str':strike_query_str,'query_args':(ticker,tstamp_utc-datetime.timedelta(minutes=15))},
                    'strike-30min': {'query_str':strike_query_str,'query_args':(ticker,tstamp_utc-datetime.timedelta(minutes=30))},
                }
            else:
                query_dict = {}

            lookback_keys = ['strike-1min','strike-5min','strike-10min','strike-15min','strike-30min']
            query_list = []
            for query_kind, item_dict in query_dict.items():
                query_str = item_dict["query_str"]
                query_args = item_dict["query_args"]
                query_func = apostgres_execute(None,query_str,query_args)
                query_list.append(query_func)
            gathered_res = await asyncio.gather(*query_list)

            for query_idx,query_kind in enumerate(query_dict.keys()):
                res = gathered_res[query_idx]
                if query_kind == 'net':
                    columns = ['ticker','tstamp','spot_price','volume_gex','state_gex','dex','convexity','vex','cex']
                    try:
                        df = pd.DataFrame([x for x in res],columns=columns)
                        df.volume_gex = df.volume_gex/1e9
                        df.state_gex = df.state_gex/1e9
                        df.convexity = df.convexity/1e9
                        df.dex = df.dex/1e9
                        df.vex = df.vex/1e9
                        df.cex = df.cex/1e9
                        spot_price = df.spot_price.to_list()[-1]
                    except:
                        df = pd.DataFrame([],columns=columns)
                        spot_price = -1
                        app.logger.error(traceback.format_exc())

                elif query_kind.startswith('strike'):
                    columns = ['ticker','tstamp','strike','volume_gex','state_gex','dex','convexity','vex','cex']
                    try:
                        df = pd.DataFrame([x for x in res],columns=columns)
                        df = df.replace({np.nan: None})
                        df.volume_gex = df.volume_gex/1e9
                        df.state_gex = df.state_gex/1e9
                        df.convexity = df.convexity/1e9
                        df.dex = df.dex/1e9
                        df.vex = df.vex/1e9
                        df.cex = df.cex/1e9
                    except:
                        df = pd.DataFrame([],columns=columns)
                        app.logger.error(traceback.format_exc())
                else:
                    raise NotImplementedError()

                query_dict[query_kind]["df"]=df

            try:
                latest_df = query_dict["strike"]["df"]
                max_state_gex = latest_df.at[latest_df.state_gex.argmax(),'strike']
                min_state_gex = latest_df.at[latest_df.state_gex.argmin(),'strike']
                max_convexity = latest_df.at[latest_df.convexity.argmax(),'strike']
                min_convexity = latest_df.at[latest_df.convexity.argmin(),'strike']
                xlimState = latest_df.state_gex.abs().max()*1.5
                xlimConvexity = latest_df.convexity.abs().max()*1.5
                xlimDex = latest_df.dex.abs().max()*1.5
                xlimCex = latest_df.cex.abs().max()*1.5
                xlimVex = latest_df.vex.abs().max()*1.5
            except:
                latest_df = pd.DataFrame([])
                max_state_gex = 100
                min_state_gex = -100
                max_convexity = 100
                min_convexity = -100
                xlimState = 999
                xlimConvexity = 999
                xlimDex = 999
                xlimCex = 999
                xlimVex = 999

            data_str = render_html("ws-sec-gex.html",
                ticker=ticker,
                spot_price=spot_price,
                df=latest_df,
                query_dict=query_dict,
                lookback_keys=lookback_keys,
                xlimState=xlimState,
                xlimConvexity=xlimConvexity,
                xlimDex=xlimDex,xlimCex=xlimCex,xlimVex=xlimVex,
                max_state_gex=max_state_gex,min_state_gex=min_state_gex,
                max_convexity=max_convexity,min_convexity=min_convexity,
                tstamp=tstamp_utc,ws_tstamp=ws_tstamp_utc)
            await websocket.send(data_str)
            await asyncio.sleep(mysec)

    except asyncio.CancelledError:
        app.logger.error(traceback.format_exc())
        app.logger.error('Client disconnected')
        raise

@app.websocket('/ticker/ws-sec-heatmap')
@login_required
async def ws_sec_heatmap():
    try:
        while True:
            ticker = websocket.args.get("ticker")
            mysec = 60

            eastern = pytz.timezone('US/Eastern')
            utc = pytz.timezone('UTC')
            tstamp_et = now_in_new_york()
            ws_tstamp_utc = tstamp_et.astimezone(tz=utc)
            market_open,market_close = get_market_open_close(ws_tstamp_utc,no_tzinfo=False)
            if tstamp_et > market_open and tstamp_et < market_open+datetime.timedelta(hours=2):
                min_tstamp = market_open
            elif tstamp_et < market_close:
                min_tstamp = ws_tstamp_utc-datetime.timedelta(hours=2)
            else:
                min_tstamp = market_open
            day_stamp = tstamp_et.strftime("%Y-%m-%d")

            net_day_query_str = "select * from gex_net where ticker = %s and tstamp::date = %s and tstamp > %s order by tstamp"
            strike_day_query_str = """
                SELECT DISTINCT ON (ticker,date_trunc('minute', tstamp),strike) 
                date_trunc('minute', tstamp) AS tstamp, ticker, strike,
                AVG(volume_gex) as volume_gex, AVG(state_gex) as state_gex,AVG(dex) as dex,
                AVG(convexity) as convexity, AVG(vex) as vex,AVG(cex) as cex
                FROM gex_strike 
                WHERE ticker = %s and tstamp::date = %s and tstamp > %s
                GROUP BY ticker,tstamp,strike
                ORDER BY tstamp, strike DESC
            """

            query_dict = {
                'net-day': {'query_str':net_day_query_str,'query_args':(ticker,day_stamp,min_tstamp)},
                'strike-day': {'query_str':strike_day_query_str,'query_args':(ticker,day_stamp,min_tstamp)},
            }
            
            query_list = []
            for query_kind, item_dict in query_dict.items():
                query_str = item_dict["query_str"]
                query_args = item_dict["query_args"]
                query_func = apostgres_execute(None,query_str,query_args)
                query_list.append(query_func)
            gathered_res = await asyncio.gather(*query_list)

            for query_idx,query_kind in enumerate(query_dict.keys()):
                res = gathered_res[query_idx]
                if query_kind == 'net-day':
                    columns = ['ticker','tstamp','spot_price','volume_gex','state_gex','dex','convexity','vex','cex']
                    try:
                        df = pd.DataFrame([x for x in res],columns=columns)
                        df.volume_gex = df.volume_gex/1e9
                        df.state_gex = df.state_gex/1e9
                        df.convexity = df.convexity/1e9
                        df.dex = df.dex/1e9
                        df.vex = df.vex/1e9
                        df.cex = df.cex/1e9
                        spot_price = df.spot_price.to_list()[-1]
                    except:
                        df = pd.DataFrame([],columns=columns)
                        spot_price = -1
                        app.logger.error(traceback.format_exc())

                elif query_kind == 'strike-day':
                    columns = ['ticker','tstamp','strike','volume_gex','state_gex','dex','convexity','vex','cex']
                    try:
                        df = pd.DataFrame([x for x in res],columns=columns)
                        df.volume_gex = df.volume_gex/1e9
                        df.state_gex = df.state_gex/1e9
                        df.convexity = df.convexity/1e9
                        df.dex = df.dex/1e9
                        df.vex = df.vex/1e9
                        df.cex = df.cex/1e9
                    except:
                        df = pd.DataFrame([],columns=columns)
                        app.logger.error(traceback.format_exc())
                else:
                    raise NotImplementedError()

                query_dict[query_kind]["df"]=df

            with tempfile.TemporaryDirectory() as tmpdir:
                net_ex_png_file = os.path.join(tmpdir,'net-ex.png')
                net_ex2_png_file = os.path.join(tmpdir,'net-ex2.png')
                heatmap_state_gex_png_file = os.path.join(tmpdir,'heatmap-ghetto-gex.png')
                heatmap_convexity_png_file = os.path.join(tmpdir,'heatmap-convexity.png')
                heatmap_dex_png_file = os.path.join(tmpdir,'heatmap-dex.png')
                heatmap_vex_png_file = os.path.join(tmpdir,'heatmap-vex.png')
                heatmap_cex_png_file = os.path.join(tmpdir,'heatmap-cex.png')

                gex_net_df = query_dict["net-day"]["df"]
                gex_strike_df = query_dict["strike-day"]["df"]

                ####################

                gex_net_df["tstamp_sec"] = gex_net_df.tstamp.apply(lambda x: x.replace(second=0))
                price_df = gex_net_df.groupby(['tstamp_sec']).agg(
                    spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                ).reset_index()
                
                gex_net_df = gex_net_df.groupby(['tstamp_sec']).agg(
                    spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                    volume_gex=pd.NamedAgg(column="volume_gex", aggfunc="median"),
                    state_gex=pd.NamedAgg(column="state_gex", aggfunc="median"),
                    convexity=pd.NamedAgg(column="convexity", aggfunc="median"),
                    dex=pd.NamedAgg(column="dex", aggfunc="median"),
                    vex=pd.NamedAgg(column="vex", aggfunc="median"),
                    cex=pd.NamedAgg(column="cex", aggfunc="median"),
                ).reset_index()

                plt.figure(1)
                plt.subplot(211)
                plt.plot(gex_net_df.tstamp_sec,gex_net_df["volume_gex"],label="volume_gex")
                plt.plot(gex_net_df.tstamp_sec,gex_net_df["state_gex"],label="state_gex")
                plt.legend()
                plt.grid(True)
                plt.subplot(212)
                plt.plot(gex_net_df.tstamp_sec,gex_net_df["convexity"],label="convexity")
                plt.legend()
                plt.grid(True)
                plt.tight_layout()
                plt.savefig(net_ex_png_file)
                plt.close()

                plt.figure(1)
                for n,x in enumerate(['dex','vex','cex']):
                    plt.subplot(311+n)
                    plt.plot(gex_net_df.tstamp_sec,gex_net_df[x],label=x)
                    plt.legend()
                    plt.grid(True)
                plt.tight_layout()
                plt.savefig(net_ex2_png_file)
                plt.close()

                ####################

                min_val,max_val = price_df.spot_price.min()*0.99,price_df.spot_price.max()*1.01
                df = gex_strike_df.copy()
                df=df[(df.strike<=max_val)&(df.strike>=min_val)]

                ####################

                myval = np.ceil(df.state_gex.abs().max())
                hue_norm = (-myval,myval)

                color_palette = "RdYlGn"
                plt.figure(1)
                ax=sns.scatterplot(data=df,x='tstamp',y='strike',hue='state_gex',
                    hue_norm=hue_norm,palette=sns.color_palette(color_palette, as_cmap=True),legend=False)

                norm = plt.Normalize(*hue_norm)
                sm = plt.cm.ScalarMappable(cmap=color_palette, norm=norm)
                ax.figure.colorbar(sm, ax=ax)

                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M-%S',tz=pytz.timezone(et_tz)))
                plt.xticks(rotation=30)

                plt.title(f"0DTE ghetto-gex ($bn/1%move)*\n{ticker} {tstamp_et}\n")
                ax = sns.lineplot(data=price_df,x='tstamp_sec',y='spot_price',color='green')
                plt.tight_layout()
                plt.savefig(heatmap_state_gex_png_file)
                plt.close()
                
                ####################

                hue_norm = (-2,2)
                myval = np.ceil(df.convexity.abs().max())
                hue_norm = (-myval,myval)

                color_palette = "cool_r"
                plt.figure(1)
                ax=sns.scatterplot(data=df,x='tstamp',y='strike',hue='convexity',
                    hue_norm=hue_norm,palette=sns.color_palette(color_palette, as_cmap=True),legend=False)

                norm = plt.Normalize(*hue_norm)
                sm = plt.cm.ScalarMappable(cmap=color_palette, norm=norm)
                ax.figure.colorbar(sm, ax=ax)

                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M-%S',tz=pytz.timezone(et_tz)))
                plt.xticks(rotation=30)

                plt.title(f"0DTE convexity ($bn/1%move)*\n{ticker} {tstamp_et}\n")
                ax = sns.lineplot(data=price_df,x='tstamp_sec',y='spot_price',color='green')
                plt.tight_layout()
                plt.savefig(heatmap_convexity_png_file)
                plt.close()

                ####################

                myval = df.dex.abs().max()
                hue_norm = (-myval,myval)

                color_palette = "RdYlGn"
                plt.figure(1)
                ax=sns.scatterplot(data=df,x='tstamp',y='strike',hue='dex',
                    hue_norm=hue_norm,palette=sns.color_palette(color_palette, as_cmap=True),legend=False)

                norm = plt.Normalize(*hue_norm)
                sm = plt.cm.ScalarMappable(cmap=color_palette, norm=norm)
                ax.figure.colorbar(sm, ax=ax)

                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M-%S',tz=pytz.timezone(et_tz)))
                plt.xticks(rotation=30)

                plt.title(f"0DTE dex ($bn/1%move)*\n{ticker} {tstamp_et}\n")
                ax = sns.lineplot(data=price_df,x='tstamp_sec',y='spot_price',color='green')
                plt.tight_layout()
                plt.savefig(heatmap_dex_png_file)
                plt.close()

                ####################

                myval = df.vex.abs().max()
                hue_norm = (-myval,myval)

                color_palette = "RdYlGn"
                plt.figure(1)
                ax=sns.scatterplot(data=df,x='tstamp',y='strike',hue='vex',
                    hue_norm=hue_norm,palette=sns.color_palette(color_palette, as_cmap=True),legend=False)

                norm = plt.Normalize(*hue_norm)
                sm = plt.cm.ScalarMappable(cmap=color_palette, norm=norm)
                ax.figure.colorbar(sm, ax=ax)

                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M-%S',tz=pytz.timezone(et_tz)))
                plt.xticks(rotation=30)

                plt.title(f"0DTE vex ($bn/1%move)*\n{ticker} {tstamp_et}\n")
                ax = sns.lineplot(data=price_df,x='tstamp_sec',y='spot_price',color='green')
                plt.tight_layout()
                plt.savefig(heatmap_vex_png_file)
                plt.close()

                ####################

                myval = df.cex.abs().max()
                hue_norm = (-myval,myval)

                color_palette = "RdYlGn"
                plt.figure(1)
                ax=sns.scatterplot(data=df,x='tstamp',y='strike',hue='cex',
                    hue_norm=hue_norm,palette=sns.color_palette(color_palette, as_cmap=True),legend=False)

                norm = plt.Normalize(*hue_norm)
                sm = plt.cm.ScalarMappable(cmap=color_palette, norm=norm)
                ax.figure.colorbar(sm, ax=ax)

                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M-%S',tz=pytz.timezone(et_tz)))
                plt.xticks(rotation=30)

                plt.title(f"0DTE cex ($bn/1%move)*\n{ticker} {tstamp_et}\n")
                ax = sns.lineplot(data=price_df,x='tstamp_sec',y='spot_price',color='green')
                plt.tight_layout()
                plt.savefig(heatmap_cex_png_file)
                plt.close()

                with open(net_ex_png_file,'rb') as f:
                    net_ex_binary = base64.b64encode(f.read()).decode("utf-8")

                with open(net_ex2_png_file,'rb') as f:
                    net_ex2_binary = base64.b64encode(f.read()).decode("utf-8")

                with open(heatmap_convexity_png_file,'rb') as f:
                    heatmap_convexity_binary = base64.b64encode(f.read()).decode("utf-8")

                with open(heatmap_state_gex_png_file,'rb') as f:
                    heatmap_state_gex_binary = base64.b64encode(f.read()).decode("utf-8")

                with open(heatmap_dex_png_file,'rb') as f:
                    dex_binary = base64.b64encode(f.read()).decode("utf-8")

                with open(heatmap_vex_png_file,'rb') as f:
                    vex_binary = base64.b64encode(f.read()).decode("utf-8")

                with open(heatmap_cex_png_file,'rb') as f:
                    cex_binary = base64.b64encode(f.read()).decode("utf-8")


            data_str = render_html("ws-sec-heatmap.html",
                ticker=ticker,
                net_ex_binary=net_ex_binary,
                net_ex2_binary=net_ex2_binary,
                heatmap_convexity_binary=heatmap_convexity_binary,
                heatmap_state_gex_binary=heatmap_state_gex_binary,
                dex_binary=dex_binary,
                vex_binary=vex_binary,
                cex_binary=cex_binary,
                ws_tstamp=ws_tstamp_utc
            )
            await websocket.send(data_str)
            await asyncio.sleep(mysec)

    except asyncio.CancelledError:
        app.logger.error(traceback.format_exc())
        app.logger.error('Client disconnected')
        raise

@app.websocket('/ws-ex')
@login_required
async def ws_ex_query():
    try:
        ticker = websocket.args.get("ticker")
        arg_tstamp = websocket.args.get("tstamp")
        if ticker == "None":
            raise ValueError("Invalid ticker None!")
        if arg_tstamp == "None":
            arg_tstamp = None

        async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=4,open=False) as apool:
            while True:
                try:

                    ret_dict = {}
                    if arg_tstamp is None:
                        tstamp_et = now_in_new_york()
                        tstamp_utc = tstamp_et.astimezone(tz=pytz.timezone('UTC'))
                    else:
                        tstamp_utc = datetime.datetime.strptime(arg_tstamp,'%Y-%m-%d-%H-%M-%S')

                    dstamp_utc = tstamp_utc.strftime("%Y-%m-%d")

                    # /uplot?ticker=SPX&tstamp=2025-06-20-19-59-58

                    early = nyse.schedule(start_date=dstamp_utc, end_date=dstamp_utc)
                    if len(early) == 0:
                        arg_tstamp = "break-while-loop"
                        raise ValueError("market closed!")
                    else:
                        market_open,market_close = get_market_open_close(tstamp_utc,no_tzinfo=False)
                        if tstamp_utc < market_open:
                            arg_tstamp = "break-while-loop"
                            raise ValueError("market closed!")

                        timea = time.time()
                        query_list = [
                            apostgres_execute(apool,EVENT_STATUS_QUERY,()),
                            apostgres_execute(apool,LATEST_DAY_GEX_NET_QUERY,(dstamp_utc,tstamp_utc,ticker,dstamp_utc,tstamp_utc)),
                            apostgres_execute(apool,LATEST_GEX_STRIKE_QUERY,(tstamp_utc,tstamp_utc,ticker,tstamp_utc,tstamp_utc,ticker,tstamp_utc,tstamp_utc,ticker)),
                            apostgres_execute(apool,GEX_NET_1MIN_QUERY,(dstamp_utc,ticker)),
                        ]

                        gathered_res = await asyncio.gather(*query_list)
                        timeb = time.time()
                        duration = timeb-timea

                        ret_dict = {}
                        if gathered_res[0] is not None:
                            df = pd.DataFrame([x for x in gathered_res[0]])
                            ret_dict['status']=json.dumps({row.event_type:f'{row.id_count} {row.tstamp}' for n,row in df.iterrows()})

                        if gathered_res[1] is not None:
                            #    'volume_gex','state_gex', 'dex', 'convexity', 'vex', 'cex', 'call_convexity',
                            #    'call_oi', 'call_dex', 'call_gex', 'call_vex', 'call_cex',
                            #    'put_convexity', 'put_oi', 'put_dex', 'put_gex', 'put_vex', 'put_cex', 
                            #    'vix_price'
                            df = pd.DataFrame([dict(x) for x in gathered_res[1]])
                            df.tstamp = df.tstamp.apply(lambda x: x.timestamp())

                            df.dex = df.dex.ffill()
                            df.volume_gex = df.volume_gex.ffill()
                            df.state_gex = df.state_gex.ffill()
                            df.convexity = df.convexity.ffill()
                            
                            df.dex = df.dex/1e9
                            df.volume_gex = df.volume_gex/1e9
                            df.state_gex = df.state_gex/1e9

                            df['dex_diff'] = df.dex.diff()
                            df['volume_gex_diff'] = df.volume_gex.diff()
                            df['state_gex_diff'] = df.state_gex.diff()
                            df = df.replace({np.nan: None})
                            spot_price = df["spot_price"].iloc[-1]
                            lst = [df[i].tolist() for i in ['tstamp','spot_price','volume_gex_diff','state_gex_diff']]
                            ret_dict['hgn'] = lst
                            lst = [df[i].tolist() for i in ['tstamp','spot_price','volume_gex','state_gex','convexity']]
                            ret_dict['hgn2'] = lst
                            ret_dict['spot_price'] = spot_price

                            app.logger.info(f'historical gex_net hgn {len(lst)}')

                        # gex_strike_id	ticker	tstamp	strike	volume_gex	state_gex	dex	convexity	
                        # vex	cex	call_convexity	call_oi	call_dex	call_gex	call_vex	call_cex	
                        # put_convexity	put_oi	put_dex	put_gex	put_vex	put_cex 
                        if gathered_res[2] is not None:
                            df = pd.DataFrame([dict(x) for x in gathered_res[2]])
                            df.state_gex = df.state_gex/1e9
                            df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
                            df['pos_gex'] = df.state_gex.where(df.state_gex>0)
                            df['neg_gex'] = df.state_gex.where(df.state_gex<=0)
                            df = df.replace({np.nan: None})
                            lst = [df[i].tolist() for i in ['strike','pos_gex','neg_gex']]
                            major_call_strike = df["strike"].iloc[df.call_gex.argmax()]
                            major_put_strike = df["strike"].iloc[df.put_gex.argmin()]

                            ret_dict['lgs'] = lst
                            ret_dict['major_call'] = major_call_strike
                            ret_dict['major_put'] = major_put_strike

                            app.logger.info(f'latest gex_strike lgs {len(lst)}')

                        if gathered_res[3] is not None: 
                            df = pd.DataFrame([dict(x) for x in gathered_res[3]])
                            df.volume_gex = df.volume_gex.ffill()
                            df.state_gex = df.state_gex.ffill()
                            #df.convexity = df.convexity.ffill()
                            df.state_gex = df.state_gex/1e9
                            df.volume_gex = df.volume_gex/1e9
                            df['volume_gex_diff'] = df.volume_gex.diff()
                            df['state_gex_diff'] = df.state_gex.diff()
                            df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
                            df = df.replace({np.nan: None})
                            #lst = [df[i].tolist() for i in ['tstamp','spot_price','volume_gex_diff','state_gex_diff']]
                            #ret_dict['hgn'] = lst
                            lst = [df[i].tolist() for i in ['tstamp','spot_price','volume_gex','state_gex']] # 'convexity'
                            ret_dict['hgn2'] = lst
                            app.logger.info(f'historical gex strike hgs {len(lst)}')


                        ret_dict['duration_sec']=duration
                        ret_dict['server_tstamp'] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                except:
                    ret_dict['server_tstamp'] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    ret_dict['status'] = 'traceback:'+traceback.format_exc()
                    app.logger.error(traceback.format_exc())

                await websocket.send_json(ret_dict)
                await asyncio.sleep(0.5)

                if arg_tstamp is not None:
                    break

    except asyncio.CancelledError:
        app.logger.warning(traceback.format_exc())
        app.logger.error('Client disconnected')
        raise

@app.route("/uplot")
@login_required
async def uplot_proto():
    ticker = request.args.get("ticker")
    tstamp = request.args.get("tstamp")
    return await render_template("uplot-proto.html",ticker=ticker,tstamp=tstamp)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("port",type=int)
    parser.add_argument('-d', '--debug',action='store_true')
    args = parser.parse_args()
    app.run(debug=args.debug,host="0.0.0.0",port=args.port)

"""

kubectl port-forward --address 0.0.0.0 svc/postgres -n gg 5432:5432
kubectl port-forward --address 0.0.0.0 svc/redis -n gg 6379:6379

-u $(id -u):$(id -g)
-v $PWD/tmp:/.local 

docker run -it \
    -e CACHE_FOLDER="/mnt/hd1/data/fi" \
    -e CACHE_TASTY_FOLDER="/mnt/hd1/data/tastyfi" \
    -e POSTGRES_URI="postgres://postgres:postgres@192.168.68.143:5432/postgres" \
    -e REDIS_URI="redis://192.168.68.143:6379/1" \
    -w $PWD -v /mnt:/mnt \
    -p 80:80 -p 8888:8888 fi-notebook:latest bash

python app.py 80

http://192.168.68.143/uplot?ticker=SPX

"""
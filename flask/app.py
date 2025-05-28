import os
import json
import argparse
import traceback
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

from utils.misc import check_password, CACHE_FOLDER
from utils.data_cache import (
    BTC_TICKER,
    CBOEX_TICKER_LIST,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    OTHER_TICKER_LIST,
    BTC_MSTR_TICKER_LIST,
    USMARKET_TICKER,
    USMARKET_TICKER_LIST,
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

et_tz = "America/New_york"

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(THIS_DIR,"templates")

def render_html(html_file,**kwargs):
    j2_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    return j2_env.get_template(html_file).render(**kwargs)

app = Quart(__name__,
    static_url_path='', 
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
                return redirect(url_for("index"))
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

@app.route("/eod-gex")
async def eod_gex():
    enable_live = request.args.get("live","false")
    ticker = request.args.get("ticker",BTC_TICKER)
    return await render_template("eod-gex.html",
        ticker=ticker,
        enable_live=enable_live,
        ticker_list=','.join(BTC_MSTR_TICKER_LIST))

@app.route("/")
@login_required
async def index():
    if not await current_user.is_authenticated:
        return redirect(url_for("login"))
    return await render_template("index.html")

@app.route("/about")
async def about():
    return await render_template("about.html")

@app.websocket('/ws-eod-gex')
async def ws_eod_gex():
    try:

        enable_live = True if websocket.args.get("live","false")=="true" else False
        ticker = websocket.args.get("ticker",BTC_TICKER)

        if ticker == 'SPX':
            ticker_alt = '^SPX'
        elif ticker == 'NDX':
            ticker_alt = '^NDX'
        else:
            ticker_alt = ticker

        now_et = now_in_new_york()
        year_stamp = datetime.datetime.strftime(now_et,'%Y')
        cache_folder = os.path.join(CACHE_FOLDER,'FBTC',year_stamp)
        if enable_live:
            daystamp_list = sorted(os.listdir(cache_folder))
        else:
            daystamp_list = sorted(os.listdir(cache_folder))[:-7]
        daystamp_list = [daystamp_list[-1]]
        while True:
            for daystamp in daystamp_list:
                tstamp = datetime.datetime.strptime(daystamp,'%Y-%m-%d')
                server_tstamp = now_in_new_york().strftime("%Y-%m-%d-%H-%M-%S-%Z")

                if ticker == BTC_TICKER:
                    spot_price, strike_df, expiration_df, surf_df, data_tstamp = compute_btc_gex(tstamp=tstamp,enable_live=True)
                elif ticker == USMARKET_TICKER:
                    spot_price, strike_df, expiration_df, surf_df, data_tstamp = compute_us_market_gex(tstamp=tstamp)
                else:
                    spot_price, strike_df, expiration_df, surf_df, data_tstamp = get_gex_df(ticker_alt,tstamp=tstamp)

                app.logger.info(f'spot_price {spot_price}')
                surf_df = surf_df.pivot(index='expiration',columns='strike',values='GEX')
                surf_df = surf_df.fillna(value="null")
                strike_list = surf_df.columns.to_list()
                expiration_list = [x.strftime("%Y-%m-%d") for x in surf_df.index.to_list()]
                surf_list = surf_df.values.tolist()
                data_str = render_html("ws-eod-guest.html",
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
                if enable_live is False:
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
                underlying_dict,options_df,last_json_file,data_tstamp = get_cache_latest(ticker)
                mydict[ticker.replace("^","")] = underlying_dict

            underlying_dict,options_df,last_json_file,data_tstamp = get_cache_latest(BTC_TICKER)
            mydict[BTC_TICKER] = underlying_dict

            data_str = render_html("prices.html",mydict=mydict,tstamp=tstamp,market_open=is_market_open())
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
    ticker = ticker.replace("^","")
    return await render_template("overview.html",ticker=ticker)

@app.websocket("/ticker/daily-ws-gex-strike")
@login_required
async def daily_ws_gex_strike():
    try:
        now_et = now_in_new_york()
        year_stamp = datetime.datetime.strftime(now_et,'%Y')
        cache_folder = os.path.join(CACHE_FOLDER,'FBTC',year_stamp)
        daystamp_list = sorted(os.listdir(cache_folder))[-5:]
        daystamp_list = [daystamp_list[-1]]
        while True:
            ticker = websocket.args.get("ticker")
            if ticker == 'SPX':
                ticker_alt = '^SPX'
            elif ticker == 'NDX':
                ticker_alt = '^NDX'
            else:
                ticker_alt = ticker
            mysec = 5
            div_name = "div-"+ticker.replace("^","")
            server_tstamp = now_in_new_york().strftime("%Y-%m-%d-%H-%M-%S-%Z")
            for daystamp in daystamp_list:
                tstamp = datetime.datetime.strptime(daystamp,'%Y-%m-%d')
                if ticker == BTC_TICKER:
                    spot_price, strike_df, expiration_df, surf_df, data_tstamp = compute_btc_gex(tstamp=tstamp)
                elif ticker == USMARKET_TICKER:
                    spot_price, strike_df, expiration_df, surf_df, data_tstamp = compute_us_market_gex(tstamp=tstamp)
                else:
                    spot_price, strike_df, expiration_df, surf_df, data_tstamp = get_gex_df(ticker_alt,tstamp=tstamp)

                data_str = render_html("gex-strike.html",
                    ticker=ticker,
                    df=strike_df,
                    spot_price=spot_price,
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
                    columns = ['ticker','tstamp','spot_price','naive_gex','true_gex']
                    try:
                        df = pd.DataFrame([x for x in res],columns=columns)
                        df.naive_gex = df.naive_gex/1e9
                        df.true_gex = df.true_gex/1e9
                        spot_price = df.spot_price.to_list()[-1]
                    except:
                        df = pd.DataFrame([],columns=columns)
                        spot_price = -1
                        app.logger.error(traceback.format_exc())

                else:
                    columns = ['ticker','tstamp','strike','naive_gex','true_gex']
                    try:
                        df = pd.DataFrame([x for x in res],columns=columns)
                        df = df.replace({np.nan: None})
                        df.naive_gex = df.naive_gex/1e9
                        df.true_gex = df.true_gex/1e9
                    except:
                        df = pd.DataFrame([],columns=columns)
                        app.logger.error(traceback.format_exc())

                query_dict[query_kind]["df"]=df

            try:
                latest_df = query_dict["strike"]["df"]
                max_true_gex = latest_df.at[latest_df.true_gex.argmax(),'strike']
                min_true_gex = latest_df.at[latest_df.true_gex.argmin(),'strike']
                max_naive_gex = latest_df.at[latest_df.naive_gex.argmax(),'strike']
                min_naive_gex = latest_df.at[latest_df.naive_gex.argmin(),'strike']
                xlimTrue = latest_df.true_gex.abs().max()*1.5
                xlimNaive = latest_df.naive_gex.abs().max()*1.5
            except:
                latest_df = pd.DataFrame([])
                max_true_gex = 100
                min_true_gex = -100
                max_naive_gex = 100
                min_naive_gex = -100
                xlimTrue = 999
                xlimNaive = 999

            data_str = render_html("ws-sec-gex.html",
                ticker=ticker,
                spot_price=spot_price,
                df=latest_df,
                query_dict=query_dict,lookback_keys=lookback_keys,
                xlimTrue=xlimTrue,
                xlimNaive=xlimNaive,
                max_true_gex=max_true_gex,min_true_gex=min_true_gex,
                max_naive_gex=max_naive_gex,min_naive_gex=min_naive_gex,
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
            day_stamp = tstamp_et.strftime("%Y-%m-%d")
            net_day_query_str = "select * from gex_net where ticker = %s and tstamp::date = %s order by tstamp"
            strike_day_query_str = """
                SELECT DISTINCT ON (ticker,date_trunc('minute', tstamp),strike) 
                date_trunc('minute', tstamp) AS tstamp, ticker, strike,
                AVG(naive_gex) as naive_gex, AVG(true_gex) as true_gex 
                FROM gex_strike 
                WHERE ticker = %s and tstamp::date = %s
                GROUP BY ticker,tstamp,strike
                ORDER BY tstamp, strike DESC
            """

            query_dict = {
                'net-day': {'query_str':net_day_query_str,'query_args':(ticker,day_stamp)},
                'strike-day': {'query_str':strike_day_query_str,'query_args':(ticker,day_stamp)},
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
                    columns = ['ticker','tstamp','spot_price','naive_gex','true_gex']
                    try:
                        df = pd.DataFrame([x for x in res],columns=columns)
                        df.naive_gex = df.naive_gex/1e9
                        df.true_gex = df.true_gex/1e9
                        spot_price = df.spot_price.to_list()[-1]
                    except:
                        df = pd.DataFrame([],columns=columns)
                        spot_price = -1
                        app.logger.error(traceback.format_exc())

                else:
                    columns = ['ticker','tstamp','strike','naive_gex','true_gex']
                    try:
                        df = pd.DataFrame([x for x in res],columns=columns)
                        df.naive_gex = df.naive_gex/1e9
                        df.true_gex = df.true_gex/1e9
                    except:
                        df = pd.DataFrame([],columns=columns)
                        app.logger.error(traceback.format_exc())

                query_dict[query_kind]["df"]=df

            with tempfile.TemporaryDirectory() as tmpdir:
                net_gex_png_file = os.path.join(tmpdir,'net-gex.png')
                heatmap_gex_png_file = os.path.join(tmpdir,'heatmap-gex.png')
                heatmap_true_gex_png_file = os.path.join(tmpdir,'heatmap-true-gex.png')
                heatmap_naive_gex_png_file = os.path.join(tmpdir,'heatmap-naive-gex.png')

                gex_net_df = query_dict["net-day"]["df"]
                gex_strike_df = query_dict["strike-day"]["df"]

                ####################

                gex_net_df["tstamp_sec"] = gex_net_df.tstamp.apply(lambda x: x.replace(second=0))
                price_df = gex_net_df.groupby(['tstamp_sec']).agg(
                    spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                ).reset_index()
                
                gex_net_df = gex_net_df.groupby(['tstamp_sec']).agg(
                    spot_price=pd.NamedAgg(column="spot_price", aggfunc="last"),
                    naive_gex=pd.NamedAgg(column="naive_gex", aggfunc="median"),
                    true_gex=pd.NamedAgg(column="true_gex", aggfunc="median"),
                ).reset_index()

                plt.figure(1)
                plt.plot(gex_net_df.tstamp_sec,gex_net_df.naive_gex,label='naive_gex')
                plt.plot(gex_net_df.tstamp_sec,gex_net_df.true_gex,label='true_gex')
                plt.grid(True)
                plt.legend()
                plt.tight_layout()
                plt.savefig(net_gex_png_file)
                plt.close()

                ####################

                min_val,max_val = price_df.spot_price.min()*0.99,price_df.spot_price.max()*1.01
                df = gex_strike_df.copy()
                df=df[(df.strike<=max_val)&(df.strike>=min_val)]

                ####################

                hue_norm = (-2,2)
                myval = np.ceil(df.true_gex.abs().max())
                hue_norm = (-myval,myval)

                color_palette = "RdYlGn"
                plt.figure(1)
                ax=sns.scatterplot(data=df,x='tstamp',y='strike',hue='true_gex',
                    hue_norm=hue_norm,palette=sns.color_palette(color_palette, as_cmap=True),legend=False)

                norm = plt.Normalize(*hue_norm)
                sm = plt.cm.ScalarMappable(cmap=color_palette, norm=norm)
                ax.figure.colorbar(sm, ax=ax)

                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M-%S',tz=pytz.timezone(et_tz)))
                plt.xticks(rotation=30)

                plt.title(f"0DTE truegex ($bn/1%move)*\n{ticker} {tstamp_et}\n")
                ax = sns.lineplot(data=price_df,x='tstamp_sec',y='spot_price',color='green')
                plt.tight_layout()
                plt.savefig(heatmap_true_gex_png_file)
                plt.close()
                
                ####################

                hue_norm = (-2,2)
                myval = np.ceil(df.naive_gex.abs().max())
                hue_norm = (-myval,myval)

                color_palette = "RdYlGn"
                plt.figure(1)
                ax=sns.scatterplot(data=df,x='tstamp',y='strike',hue='naive_gex',
                    hue_norm=hue_norm,palette=sns.color_palette(color_palette, as_cmap=True),legend=False)

                norm = plt.Normalize(*hue_norm)
                sm = plt.cm.ScalarMappable(cmap=color_palette, norm=norm)
                ax.figure.colorbar(sm, ax=ax)

                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H-%M-%S',tz=pytz.timezone(et_tz)))
                plt.xticks(rotation=30)

                plt.title(f"0DTE naivegex ($bn/1%move)*\n{ticker} {tstamp_et}\n")
                ax = sns.lineplot(data=price_df,x='tstamp_sec',y='spot_price',color='green')
                plt.tight_layout()
                plt.savefig(heatmap_naive_gex_png_file)
                plt.close()

                with open(net_gex_png_file,'rb') as f:
                    net_gex_binary = base64.b64encode(f.read()).decode("utf-8")

                with open(heatmap_naive_gex_png_file,'rb') as f:
                    heatmap_naive_gex_binary = base64.b64encode(f.read()).decode("utf-8")

                with open(heatmap_true_gex_png_file,'rb') as f:
                    heatmap_true_gex_binary = base64.b64encode(f.read()).decode("utf-8")
                
            data_str = render_html("ws-sec-heatmap.html",
                ticker=ticker,
                net_gex_binary=net_gex_binary,
                heatmap_naive_gex_binary=heatmap_naive_gex_binary,
                heatmap_true_gex_binary=heatmap_true_gex_binary,
                ws_tstamp=ws_tstamp_utc
            )
            await websocket.send(data_str)
            await asyncio.sleep(mysec)

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

"""

kubectl port-forward --address 0.0.0.0 svc/postgres -n gg 5432:5432

-u $(id -u):$(id -g)
-v $PWD/tmp:/.local 

docker run -it \
    -e CACHE_FOLDER="/mnt/hd1/data/fi" \
    -e CACHE_TASTY_FOLDER="/mnt/hd1/data/tastyfi" \
    -e POSTGRES_URI="postgres://postgres:postgres@192.168.68.143:5432/postgres" \
    -w $PWD -v /mnt:/mnt \
    -p 80:80 -p 8888:8888 fi-notebook:latest bash

python app.py 80

"""
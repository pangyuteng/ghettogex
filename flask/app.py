import os
import json
import argparse
import traceback
import asyncio
import pytz
import datetime
import numpy as np
import pandas as pd
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
    get_cache_latest,
    is_market_open,
    now_in_new_york,
)

from utils.compute import (
    get_gex_df,
    compute_btc_gex
)
from utils.postgres_utils import apostgres_execute

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

@app.route("/")
async def guest():
    enable_live = request.args.get("live")
    ticker = request.args.get("ticker",None)
    return await render_template("guest.html",
        ticker=ticker,
        enable_live=enable_live,
        ticker_list=','.join(BTC_MSTR_TICKER_LIST))

@app.route("/home")
async def home():
    if not await current_user.is_authenticated:
        return redirect(url_for("login"))
    return await render_template("index.html")

@app.route("/about")
async def about():
    return await render_template("about.html")

@app.websocket('/ws-guest')
async def ws_guest():
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
                else:
                    spot_price, strike_df, expiration_df, surf_df, data_tstamp = get_gex_df(ticker_alt,tstamp=tstamp)

                app.logger.info(f'spot_price {spot_price}')
                surf_df = surf_df.pivot(index='expiration',columns='strike',values='GEX')
                surf_df = surf_df.fillna(value="null")
                strike_list = surf_df.columns.to_list()
                expiration_list = [x.strftime("%Y-%m-%d") for x in surf_df.index.to_list()]
                surf_list = surf_df.values.tolist()
                data_str = render_html("guest-ws.html",
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
    if ticker == 'SPX':
        return await render_template("sample-gex.html",ticker=ticker)
    else:
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

@app.route("/gex")
@login_required
async def sample_gex():
    ticker = request.args.get("ticker")
    return await render_template("sample-gex.html",ticker=ticker)

@app.websocket('/ticker/ws-gex-sample')
@login_required
async def ws_gex_sample():
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
            fetched = await apostgres_execute(query_str,query_args)
            try:
                df = pd.DataFrame([x for x in fetched])
                tstamp_utc = df.tstamp.to_list()[-1]
            except:
                tstamp_utc = None
                spot_price = -1

            query_str = """
                select * from gex_net
                where ticker = %s
                and tstamp = %s
                order by tstamp
            """
            query_args = (ticker,tstamp_utc)
            fetched = await apostgres_execute(query_str,query_args)
            columns = ['ticker','tstamp','spot_price','naive_gex','volume_gex']
            try:
                ucdf = pd.DataFrame([x for x in fetched])
                spot_price = ucdf.spot_price.to_list()[-1]
            except:
                ucdf = pd.DataFrame([],columns=columns)
                spot_price = -1
            query_str = """
                select * from gex_strike
                where ticker = %s
                and tstamp = %s
                order by tstamp
            """
            query_args = (ticker,tstamp_utc)
            fetched = await apostgres_execute(query_str,query_args)
            columns = ['ticker','tstamp','strike','naive_gex','volume_gex']
            try:
                df = pd.DataFrame([x for x in fetched])
                df = df.replace({np.nan: None})
                df.naive_gex = df.naive_gex/1e9
            except:
                df = pd.DataFrame([],columns=columns)
            data_str = render_html("ws-sample-gex.html",
                ticker=ticker,df=df,spot_price=spot_price,
                tstamp=tstamp_utc,ws_tstamp=ws_tstamp_utc)
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
asdf asdfassd
"""
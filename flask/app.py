import os
import json
import argparse
import traceback
import asyncio
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

from utils.misc import check_password
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
async def home():
    if not await current_user.is_authenticated:
        return redirect(url_for("login"))
    else:
        return await render_template("index.html",ticker_list=BTC_MSTR_TICKER_LIST)

@app.route("/about")
@login_required
async def about():
    return await render_template("about.html")

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
                mydict[ticker] = underlying_dict

            underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(BTC_TICKER)
            mydict[BTC_TICKER] = underlying_dict

            data_str = render_html("prices.html",mydict=mydict,tstamp=tstamp,market_open=is_market_open())
            await websocket.send(data_str)
            await asyncio.sleep(mysec)
    except asyncio.CancelledError:
        app.logger.error('Client disconnected')
        raise
    # no return, means connection is kept open.

@app.route("/ticker/overview")
@login_required
async def overview():
    ticker = request.args.get("ticker")
    return await render_template("overview.html",ticker=ticker)

@app.websocket('/ticker/ws-gex-strike')
@login_required
async def ws_gex_strike():
    try:
        while True:
            ticker = websocket.args.get("ticker")
            mysec = 5
            div_name = "div-"+ticker.replace("^","")
            tstamp = now_in_new_york().strftime("%Y-%m-%d-%H-%M-%S-%Z")

            if ticker == BTC_TICKER:
                spot_price, df = compute_btc_gex()
                df = df.copy()
            else:
                spot_price, gex_by_strike, gex_by_expiration, gex_df = get_gex_df(ticker)
                df = gex_by_strike.copy()

            data_str = render_html("gex-strike.html",
                ticker=ticker,df=df,
                spot_price=spot_price,
                tstamp=tstamp,div_name=div_name)
            await websocket.send(data_str)
            await asyncio.sleep(mysec)
    except asyncio.CancelledError:
        app.logger.error('Client disconnected')
        raise

@app.websocket('/ticker/ws-gex-surf')
@login_required
async def ws_gex_surf():
    try:
        while True:
            ticker = websocket.args.get("ticker")
            mysec = 5
            div_name = "div-"+ticker.replace("^","")
            tstamp = now_in_new_york().strftime("%Y-%m-%d-%H-%M-%S-%Z")

            if ticker == BTC_TICKER:
                spot_price, df = compute_btc_gex()
                df = df.copy()
            else:
                spot_price, gex_by_strike, gex_by_expiration, gex_df = get_gex_df(ticker)
                df = gex_by_strike.copy()

            data_str = render_html("gex_surf.html",
                ticker=ticker,df=df,
                spot_price=spot_price,
                tstamp=tstamp,div_name=div_name)
            await websocket.send(data_str)
            await asyncio.sleep(mysec)
    except asyncio.CancelledError:
        app.logger.error('Client disconnected')
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("port",type=int)
    parser.add_argument('-d', '--debug',action='store_true')
    args = parser.parse_args()
    app.run(debug=args.debug,host="0.0.0.0",port=args.port)

"""
asdf asdfasdf
"""
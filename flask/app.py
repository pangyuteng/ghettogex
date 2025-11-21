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

from utils.misc import (
    CACHE_FOLDER,
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
    ORDER_IMBALANCE_QUERY,
    CANDLE_QC_QUERY,
    QUOTE_5MIN_QUERY,
    CONVEXITY_QUERY,
    CONVEXITY_1HOUR_QUERY,
    GREEKS_QUERY,
)

from utils.data_tasty import (
    a_get_equity_data, 
    a_get_equity_data_session_reuse
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
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.secret_key = "dLxWOjuwlk2z0n2I4NgxaQ" # import secrets ; secrets.token_urlsafe(16)
auth_manager = QuartAuth(app)

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

@app.route("/links")
@login_required
async def links():
    return await render_template("links.html")

@app.route("/gexbots2cols")
@login_required
async def gexbots2cols():
    return await render_template("gexbots-2cols.html")

@app.route("/gexbots3cols")
@login_required
async def gexbots3cols():
    return await render_template("gexbots-3cols.html")

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

@app.route("/")
@login_required
async def home():
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
    return await render_template("index.html",dstamp=dstamp,load_data=load_data,market_status=market_status)

@app.websocket('/ws-main-socket') # name so bad
@login_required
async def ws_main_socket():
    try:
        message = None
        ret_dict = {}
        dstamp = websocket.args.get("dstamp")
        async with psycopg_pool.AsyncConnectionPool(postgres_uri,min_size=5,open=False) as apool:
            while True:
                try:

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

                    ticker = 'SPX'
                    ticker_alt = 'SPXW'
                    ndx_ticker_alt = 'NDXP'

                    timea = time.time()
                    query_list = [
                        apostgres_execute(apool,CANDLE_1MIN_QUERY,(dstamp,dstamp,dstamp,dstamp,dstamp)),
                        apostgres_execute(apool,LATEST_GEX_STRIKE_QUERY,(tstamp_utc,tstamp_utc,ticker,tstamp_utc,tstamp_utc,ticker)),
                        apostgres_execute(apool,ORDER_IMBALANCE_QUERY,(dstamp,ticker_alt)),
                        apostgres_execute(apool,CANDLE_QC_QUERY,(ticker,tstamp_utc,ticker_alt,tstamp_utc)),
                        apostgres_execute(apool,QUOTE_5MIN_QUERY,(dstamp,ticker_alt,tstamp_utc)),
                        apostgres_execute(apool,CONVEXITY_QUERY,(ticker_alt,dstamp,dstamp,ticker_alt,dstamp,dstamp)),
                        #apostgres_execute(apool,CONVEXITY_1HOUR_QUERY,(ticker_alt,dstamp,dstamp,tstamp_utc,tstamp_utc,ticker_alt,dstamp,dstamp)),
                        apostgres_execute(apool,GREEKS_QUERY,(ticker_alt,dstamp,dstamp)),
                        apostgres_execute(apool,CONVEXITY_QUERY,(ndx_ticker_alt,dstamp,dstamp,ndx_ticker_alt,dstamp,dstamp)),
                        #apostgres_execute(apool,CONVEXITY_1HOUR_QUERY,(ndx_ticker_alt,dstamp,dstamp,tstamp_utc,tstamp_utc,ndx_ticker_alt,dstamp,dstamp)),
                    ]
                    app.logger.error(tstamp_utc)
                    gathered_res = await asyncio.gather(*query_list)

                    timeb = time.time()
                    duration_time = timeb-timea

                    spot_max_lim = np.inf
                    spot_min_lim = 0

                    if gathered_res[0] is not None:
                        df = pd.DataFrame([dict(x) for x in gathered_res[0]])
                        df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
                        df.ndx_close = df.ndx_close.round(decimals=2)
                        df = df.replace({np.nan: None})
                        lst = [df[i].tolist() for i in ['tstamp','vix_close','spx_close',]]
                        ret_dict['prices'] = lst
                        ret_dict['es_price'] = df.es_close.iloc[-1]
                        vix_price = df.vix_close.iloc[-1]
                        ret_dict['vix_price'] = vix_price
                        ret_dict['spx_price'] = df.spx_close.iloc[-1]
                        ret_dict['ndx_price'] = df.ndx_close.iloc[-1]
                        
                        ret_dict['vix1d_price'] = df.vix1d_close.iloc[-1]

                        if vix_price > 50: # vary lim by last vix price
                            plus_prct = 1.3
                            minus_prct = 0.7
                        elif vix_price > 30:
                            plus_prct = 1.1
                            minus_prct = 0.9
                        elif vix_price > 15:
                            plus_prct = 1.03
                            minus_prct = 0.97
                        else:
                            plus_prct = 1.01
                            minus_prct = 0.99

                        spot_max_lim = df.spx_close.max()*plus_prct # +100
                        spot_min_lim = df.spx_close.min()*minus_prct # -100

                        ret_dict['spot_min_lim'] = spot_min_lim
                        ret_dict['spot_max_lim'] = spot_max_lim
                        
                        ndx_max_lim = df.ndx_close.max()*plus_prct
                        ndx_min_lim = df.ndx_close.min()*minus_prct

                    if gathered_res[1] is not None:
                        df = pd.DataFrame([dict(x) for x in gathered_res[1]])
                        try:
                            if len(df) == 0:
                                raise LookupError("null gex query!")

                            df = df[(df.strike>spot_min_lim) & (df.strike<spot_max_lim)].reset_index()
                            df.state_gex = df.state_gex/1e9
                            df['pos_gex'] = df.state_gex.where(df.state_gex>0)
                            df['neg_gex'] = df.state_gex.where(df.state_gex<=0)
                            df = df.replace({np.nan: None})

                            gex_list = [df[i].tolist() for i in ['strike','pos_gex','neg_gex']]
                            major_pos_gex_strike = df["strike"].iloc[df.state_gex.argmax()]
                            major_neg_gex_strike = df["strike"].iloc[df.state_gex.argmin()]

                            ret_dict['gex_list'] = gex_list
                            ret_dict['major_pos_gex_strike'] = major_pos_gex_strike
                            ret_dict['major_neg_gex_strike'] = major_neg_gex_strike
                        except:
                            ret_dict['gex_list'] = [[],[],[]]
                            ret_dict['major_pos_gex_strike'] = None
                            ret_dict['major_neg_gex_strike'] = None
                            app.logger.error(traceback.format_exc())

                    if gathered_res[2] is not None:
                        df = pd.DataFrame([dict(x) for x in gathered_res[2]])
                        df.tstamp = df.tstamp.apply(lambda x: x.timestamp())
                        df = df[(df.strike>=spot_min_lim) & (df.strike<=spot_max_lim)]
                        df = df.dropna()
                        # NOTE: remember to update order_imbalance_bin_str
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
                        call_order_imbalance_list = [[],]
                        put_order_imbalance_list = [[],]
                        for row_filter in filter_list:
                            row_tstamp = df.tstamp[row_filter&(df.contract_type=="C")].to_list()
                            row_strike = df.strike[row_filter&(df.contract_type=="C")].to_list()
                            call_item = [row_tstamp,row_strike,[]]
                            call_order_imbalance_list.append(call_item)
                            row_tstamp = df.tstamp[row_filter&(df.contract_type=="P")].to_list()
                            row_strike = df.strike[row_filter&(df.contract_type=="P")].to_list()
                            put_item = [row_tstamp,row_strike,[]]
                            put_order_imbalance_list.append(put_item)
                        # add spx price
                        lst = ret_dict['prices']
                        call_order_imbalance_list.append([ lst[0],lst[-1],[] ])
                        put_order_imbalance_list.append([ lst[0],lst[-1],[] ])

                        ret_dict['call_order_imbalance'] = call_order_imbalance_list
                        ret_dict['put_order_imbalance'] = put_order_imbalance_list

                    if gathered_res[3] is not None:
                        df = pd.DataFrame([dict(x) for x in gathered_res[3]])
                        if len(df) > 0:
                            latest_data_tstamp = df.tstamp.min()
                            latest_data_tstamp_str = latest_data_tstamp.strftime("%Y-%m-%d %H:%M:%S")
                            qc_comment = "***STALE TSTAMP!***" if (tstamp_utc.replace(tzinfo=None)-latest_data_tstamp).total_seconds() > 60 else ""
                            ret_dict['qc_comment'] = qc_comment
                            ret_dict['data_tstamp'] = latest_data_tstamp_str
                        else:
                            qc_comment = "***STALE TSTAMP!***"
                            ret_dict['qc_comment'] = qc_comment
                            ret_dict['data_tstamp'] = "null"

                    if gathered_res[4] is not None:
                        df = pd.DataFrame([dict(x) for x in gathered_res[4]])
                        try:
                            if len(df) == 0:
                                raise LookupError("null quote query!")
                            spot_price = ret_dict['spx_price']
                            df['mid_price'] = (df.last_bid_price+df.last_ask_price)/2.0
                            cdf = df[(df.contract_type=="C")&(df.strike>=spot_price)].reset_index().iloc[:3].reset_index()
                            pdf = df[(df.contract_type=="P")&(df.strike<=spot_price)].reset_index().iloc[-3:].reset_index()
                            expected_move = ( \
                                0.6*cdf.at[0,'mid_price']+0.3*cdf.at[1,'mid_price']*0.1*cdf.at[2,'mid_price'] + \
                                0.6*pdf.at[2,'mid_price']+0.3*pdf.at[1,'mid_price']*0.1*pdf.at[0,'mid_price'] )
                            ret_dict['plus_expected_move'] = spot_price+expected_move
                            ret_dict['minus_expected_move'] = spot_price-expected_move
                        except:
                            ret_dict['plus_expected_move'] = None
                            ret_dict['minus_expected_move'] = None
                            app.logger.error(traceback.format_exc())

                    if gathered_res[5] is not None:
                        df = pd.DataFrame([dict(x) for x in gathered_res[5]])
                        df = df[(df.strike>spot_min_lim) & (df.strike<spot_max_lim)].reset_index()
                        df['convexity'] = df.gamma*df.order_imbalance
                        df['pos_convexity'] = df.convexity.where(df.convexity>0)
                        df['neg_convexity'] = df.convexity.where(df.convexity<=0)
                        df = df.replace({np.nan: None})

                        convexity_list = [df[i].tolist() for i in ['strike','pos_convexity','neg_convexity']]
                        major_pos_convexity = df["strike"].iloc[df.convexity.argmax()] # consider moving this up prior filter like ndx
                        major_neg_convexity = df["strike"].iloc[df.convexity.argmin()]

                        ret_dict['convexity_list'] = convexity_list
                        ret_dict['major_pos_convexity'] = major_pos_convexity
                        ret_dict['major_neg_convexity'] = major_neg_convexity

                    if gathered_res[6] is not None:
                        df = pd.DataFrame([dict(x) for x in gathered_res[6]])
                        df = df[(df.strike>spot_min_lim) & (df.strike<spot_max_lim)].reset_index()

                        df.volatility = df.volatility*100
                        cdf = df[df.contract_type=='C']
                        pdf = df[df.contract_type=='P']
                        volatility_list = [cdf.strike.tolist(),cdf.volatility.tolist(),pdf.volatility.tolist()]
                        ret_dict['volatility_list'] = volatility_list

                    if gathered_res[7] is not None:
                        df = pd.DataFrame([dict(x) for x in gathered_res[7]])
                        df['convexity'] = df.gamma*df.order_imbalance
                        major_pos_convexity = df["strike"].iloc[df.convexity.argmax()]
                        major_neg_convexity = df["strike"].iloc[df.convexity.argmin()]
                        ndx_max_lim = np.max([ndx_max_lim,major_pos_convexity+500,major_neg_convexity+500])
                        ndx_min_lim = np.min([ndx_min_lim,major_pos_convexity-500,major_neg_convexity-500])

                        df = df[(df.strike>ndx_min_lim) & (df.strike<ndx_max_lim)].reset_index()
                        
                        df['pos_convexity'] = df.convexity.where(df.convexity>0)
                        df['neg_convexity'] = df.convexity.where(df.convexity<=0)
                        df = df.replace({np.nan: None})

                        convexity_list = [df[i].tolist() for i in ['strike','pos_convexity','neg_convexity']]

                        ret_dict['ndx_convexity_list'] = convexity_list# asdf
                        ret_dict['ndx_major_pos_convexity'] = major_pos_convexity
                        ret_dict['ndx_major_neg_convexity'] = major_neg_convexity



                    ret_dict['server_tstamp'] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    ret_dict['duration_time'] = f"{duration_time:0.3f}sec"
                except:
                    ret_dict['qc_comment'] = "unexpected error!!! ffff likely missing data, services down!"
                    ret_dict['duration_time'] = None
                    ret_dict['data_tstamp'] = None
                    ret_dict['server_tstamp'] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    ret_dict['status'] = 'traceback:'+traceback.format_exc()
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

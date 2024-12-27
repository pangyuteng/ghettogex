import os
import argparse
import asyncio
import datetime
import numpy as np
from quart import (
    Quart,
    websocket,
    render_template,
    jsonify
)

from jinja2 import Environment, FileSystemLoader
THIS_DIR = os.path.dirname(os.path.abspath(__file__))

from utils.data_yahoo import (
    BTC_TICKER,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    OTHER_TICKER_LIST,
    BTC_MSTR_TICKER_LIST,
    get_cache_latest
)

CACHE_FOLDER = os.environ.get("CACHE_FOLDER")

app = Quart(__name__,
    static_url_path='', 
    static_folder='static',
    template_folder='qtemplates',
)

@app.route("/ping")
async def ping():
    return jsonify("pong")

template_folder = os.path.join(THIS_DIR,"qtemplates")
def render_html(html_file,**kwargs):
    j2_env = Environment(loader=FileSystemLoader(template_folder))
    return j2_env.get_template(html_file).render(**kwargs)

@app.websocket('/ws-random')
async def ws_random():
    try:
        while True:
            mysec = 5
            message = f"socket will send data every {mysec} seconds."
            tstamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S.%f")
            message += str(os.listdir(CACHE_FOLDER))
            mylist = []
            for n in range(100):
                myitem = (np.random.rand(100)*2).astype(float).tolist()
                mylist.append(myitem)
            data_str = render_html("random_refresh.html",mylist=mylist,tstamp=tstamp,message=message)
            await websocket.send(data_str)
            await asyncio.sleep(mysec)
    except asyncio.CancelledError:
        print('Client disconnected')
        raise
    # no return, means connection is kept open.

@app.route("/random")
async def home_random():
    return await render_template("random_surf.html")

@app.route("/")
async def home():
    return await render_template("index.html")

@app.websocket('/ws-prices')
async def ws_prices():
    try:
        while True:
            mysec = 5
            tstamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S.%f")
            mydict = {}
            for ticker in INDEX_TICKER_LIST:
                underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker)
                mydict[ticker]=underlying_dict
            underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(BTC_TICKER)
            mydict[BTC_TICKER]=underlying_dict
            data_str = render_html("prices.html",mydict=mydict,tstamp=tstamp)
            await websocket.send(data_str)
            await asyncio.sleep(mysec)
    except asyncio.CancelledError:
        print('Client disconnected')
        await websocket.send("ws-prices-error")
    # no return, means connection is kept open.


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("port",type=int)
    parser.add_argument('-d', '--debug',action='store_true')
    args = parser.parse_args()
    app.run(debug=args.debug,host="0.0.0.0",port=args.port)

"""

"""
import os
import sys
import argparse
import tempfile
import traceback
import json

from flask import (
    Flask, request, render_template, Response,
    redirect, url_for, jsonify, send_file
)

from utils.compute import get_iv_df
from utils.data_yahoo import INDEX_TICKER_LIST,BTC_TICKER_LIST

app = Flask(__name__,
    static_url_path='', 
    static_folder='static',
    template_folder='templates',
)

app.config["SECRET_KEY"] = "the random string"


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/overview', methods=['GET'])
def overview():
    ticker = request.args.get('ticker',None)
    if ticker is not None:
        ticker_list = [ticker]
    else:
        ticker_list = []
        ticker_list.extend(INDEX_TICKER_LIST)
        ticker_list.extend(BTC_TICKER_LIST)
    mylist = []
    for ticker in ticker_list:
        mylist.append(dict(
            ticker=ticker,
            iv_url=url_for('get_iv',ticker=ticker,option_type="C"),
            div_name=f'div-call-{ticker}',
            title=f'{ticker} call',
        ))
        mylist.append(dict(
            ticker=ticker,
            iv_url=url_for('get_iv',ticker=ticker,option_type="P"),
            div_name=f'div-put-{ticker}',
            title=f'{ticker} put',
        ))
    
    return render_template('overview.html',mylist=mylist)


@app.route('/iv', methods=['GET'])
def get_iv():
    try:
        ticker = request.args.get('ticker')
        option_type = request.args.get('option_type')
        with tempfile.TemporaryDirectory() as temp_dir:
            iv_df = get_iv_df(ticker,option_type,tstamp=None)
            data = []
            for n,row in iv_df.iterrows():
                myvalues = json.loads(row.to_json(orient='values'))
                data.append(myvalues)
            expirationTicks = list(iv_df.index)
            strikeTicks = list(iv_df.columns)
            mydict = dict(
                data=data,
                expirationTicks=expirationTicks,
                strikeTicks=strikeTicks,
            )
            mydict = json.loads(json.dumps(mydict))
            return jsonify(mydict)
    except:
        traceback.print_exc()
        return jsonify("error"),400


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--port",type=int,default=5000)
    args = parser.parse_args()
    app.run(debug=True,host="0.0.0.0",port=args.port)



"""

docker run -it -p 5000:5000 -u $(id -u):$(id -g) \
    --env-file=.env -w $PWD -v /mnt:/mnt \
    fi-flask bash

python app.py

"""


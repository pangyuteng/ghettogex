
import os
import json
import pandas as pd
import asyncio
import datetime

from tastytrade import DXLinkStreamer
from tastytrade.dxfeed import Candle
from tastytrade.instruments import Equity
from tastytrade.session import Session

username = os.environ.get('TASTYTRADE_USERNAME')
def get_session():
    password = os.environ.get('TASTYTRADE_PASSWORD')
    session = Session(username,password,remember_me=True)
    return session


# equity = Equity.get_equity(session, ticker)

async def main(session):
    async with DXLinkStreamer(session) as streamer:
        start_time = datetime.datetime(2024,4,17,9,30,0)
        end_time = datetime.datetime(2024,4,17,16,0,0)
        CANDLE_TYPE = 's'
        streamer_symbol = 'SPY'
        candle =  await streamer.subscribe_candle(
            [streamer_symbol], interval='s', start_time=start_time)
        print(candle)

if __name__ == "__main__":
    # json.
    session = get_session()
    if False:
        session_file = "session.json"
        if os.path.exists(session_file):
            with open(session_file,'r') as f:
                remember_token = json.loads(f.read())['remember_token']
            session = Session(username, remember_token=remember_token)
        else:
            with open(session_file,'w') as f:
                jd = json.dumps({"remember_token":session.remember_token})
                f.write(jd)

    asyncio.run(main(session))

"""

docker run -it -u $(id -u):$(id -g) --env-file=.env \
    -w $PWD -v /mnt:/mnt fi-notebook:latest bash

"""


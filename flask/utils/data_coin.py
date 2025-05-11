import logging
logger = logging.getLogger(__file__)
import sys
import traceback
import json
import os
import pandas as pd
import requests

def scrape_btcusd():
    mydict = {}
    try:
        url = "https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD"
        data = requests.get(url)
        mydict = data.json()
        mydict['last_price']=float(mydict['USD'])
    except ValueError:
        traceback.print_exc()
    return mydict

if __name__ == "__main__":
    mydict = scrape_btcusd()
    print(mydict)

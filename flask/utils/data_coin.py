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
        url = f"https://api.coindesk.com/v1/bpi/currentprice.json"
        data = requests.get(url)
        mydict = data.json()
        mydict['last_price']=float(mydict['bpi']['USD']['rate'].replace(',',''))
    except ValueError:
        traceback.print_exc()
    return mydict

if __name__ == "__main__":
    mydict = scrape_btcusd()
    print(mydict)

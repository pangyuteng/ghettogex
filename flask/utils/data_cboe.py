"""
source https://github.com/Matteo-Ferrara/gex-tracker
"""
import logging
logger = logging.getLogger(__file__)
import sys
import traceback
import json
import os
import re
from datetime import timedelta, datetime

import matplotlib.pyplot as plt
import pandas as pd
import requests
from matplotlib import dates

# Set plot style
#plt.style.use("seaborn-dark")
# for param in ["figure.facecolor", "axes.facecolor", "savefig.facecolor"]:
#     plt.rcParams[param] = "#212946"
# for param in ["text.color", "axes.labelcolor", "xtick.color", "ytick.color"]:
#     plt.rcParams[param] = "0.9"

from .misc import CACHE_FOLDER
TMP_FOLDER = os.path.join(CACHE_FOLDER,"tmp")

contract_size = 100


def run(ticker):
    info = scrape_underlying_data(ticker)
    print(info)
    spot_price, option_data = scrape_options_data(ticker)
    print(spot_price)
    total_gex_bn = compute_total_gex(spot_price, option_data)
    logger.info(f"Total notional GEX: ${total_gex_bn} Bn")
    compute_gex_by_strike(spot_price, option_data)
    compute_gex_by_expiration(option_data)
    compute_gex_surface(spot_price, option_data)

def scrape_underlying_data(ticker):
    """Scrape data from CBOE website"""
    # Request data and save it to file
    mydict = {}
    try:
        if ticker.startswith("^"):
            ticker = ticker.replace("^","")
            url = f"https://cdn.cboe.com/api/global/delayed_quotes/quotes/_{ticker}.json"
        else:
            url = f"https://cdn.cboe.com/api/global/delayed_quotes/quotes/{ticker}.json"
        resp = requests.get(url)
        if resp.status_code == 200:
            mydict = resp.json()
            mydict['last_price'] = mydict['data']['close']
        else:
            raise ValueError(resp.status_code)
    except ValueError:
        traceback.print_exc()
    
    return mydict 

def scrape_options_data(ticker):
    """Scrape data from CBOE website"""
    mydict = {}
    try:
        if ticker.startswith("^"):
            ticker = ticker.replace("^","")
            url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/_{ticker}.json"
        else:
            url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{ticker}.json"
        resp = requests.get(url)
        if resp.status_code == 200:
            mydict = resp.json()
        else:
            raise ValueError(resp.status_code)
    except ValueError:
        traceback.print_exc()

    # Convert json to pandas DataFrame
    try:
        if len(mydict) == 0:
            raise ValueError("empty dict!")
        data = pd.DataFrame.from_dict(mydict)
        spot_price = data.loc["current_price", "data"]
        option_data = pd.DataFrame(data.loc["options", "data"])
        option_data['spot_price']=spot_price
        return spot_price, fix_option_data(option_data)
    except:
        traceback.print_exc()
        print(mydict,'!!!')
        print("sleeping then will raise error")
        time.sleep(10)
        raise ValueError()


def fix_option_data(data):
    """
    Fix option data columns.

    From the name of the option derive type of option, expiration and strike price
    """
    #SPY270115P00900000
    data["option_type"] = data.option.str.extract(r"\d([A-Z])\d")
    #data["strike"] = data.option.str.extract(r"\d[A-Z](\d+)\d\d\d").astype(float)
    data["strike"] = data.option.str.extract(r"\d[A-Z](\d+)").astype(float)/1000
    data["expiration"] = data.option.str.extract(r"[A-Z](\d+)").astype(str)
    # Convert expiration to datetime format
    data["expiration"] = pd.to_datetime(data["expiration"], format="%y%m%d")
    return data



def compute_total_gex(spot, data):
    """Compute dealers' total GEX"""
    # Compute gamma exposure for each option
    data["GEX"] = spot * data.gamma * data.open_interest * contract_size * spot * 0.01

    # For put option we assume negative gamma, i.e. dealers sell puts and buy calls
    data["GEX"] = data.apply(lambda x: -x.GEX if x.option_type == "P" else x.GEX, axis=1)
    total_gex_bn = round(data.GEX.sum() / 10 ** 9, 4)
    return total_gex_bn


def compute_gex_by_strike(spot, data, ticker=None,save_png=False,lim='default'):
    """Compute and plot GEX by strike"""
    # Compute total GEX by strike
    gex_by_strike = data.groupby("strike")["GEX"].sum() / 10**9

    # Limit data to +- 15% from spot price
    if lim == 'large':
        limit_criteria = (gex_by_strike.index > spot * 0.85) & (gex_by_strike.index < spot * 1.15)
    elif lim == 'default':
        limit_criteria = (gex_by_strike.index > spot * 0.95) & (gex_by_strike.index < spot * 1.05)
    else:
        limit_criteria = (gex_by_strike.index > spot * 0.95) & (gex_by_strike.index < spot * 1.05)

    if save_png:
        # Plot GEX by strike
        plt.bar(
            gex_by_strike.loc[limit_criteria].index,
            gex_by_strike.loc[limit_criteria],
            color="#FE53BB",
            alpha=0.5,
        )
        plt.grid(color="#2A3459")
        plt.xticks(fontweight="heavy")
        plt.yticks(fontweight="heavy")
        plt.xlabel("Strike", fontweight="heavy")
        plt.ylabel("Gamma Exposure (Bn$ / %)", fontweight="heavy")
        plt.title(f"{ticker} GEX by strike", fontweight="heavy")
        plt.show()
        plt.savefig(os.path.join(TMP_FOLDER,f"{ticker}-gex-by-strike.png"))
        plt.close

    df = pd.DataFrame()
    df['strike']=gex_by_strike.loc[limit_criteria].index
    df['gex']=gex_by_strike.loc[limit_criteria].values
    return df

def compute_gex_by_expiration(data, ticker=None,save_png=False):
    """Compute and plot GEX by expiration"""
    # Limit data to one year
    selected_date = datetime.today() + timedelta(days=7)
    data = data.loc[data.expiration < selected_date]
    # Compute GEX by expiration date
    gex_by_expiration = data.groupby("expiration")["GEX"].sum() / 10**9
    if save_png:
        # Plot GEX by expiration
        plt.bar(
            gex_by_expiration.index,
            gex_by_expiration.values,
            color="#FE53BB",
            alpha=0.5,
        )
        plt.grid(color="#2A3459")
        plt.xticks(rotation=45, fontweight="heavy")
        plt.yticks(fontweight="heavy")
        plt.xlabel("Expiration date", fontweight="heavy")
        plt.ylabel("Gamma Exposure (Bn$ / %)", fontweight="heavy")
        plt.title(f"{ticker} GEX by expiration", fontweight="heavy")
        plt.show()
        plt.savefig(os.path.join(TMP_FOLDER,f"{ticker}-gex-by-expiration.png"))
        plt.close

    return gex_by_expiration

def compute_gex_surface(spot, data, ticker=None,save_png=False):
    """Plot 3D surface"""
    # Limit data to 1 year and +- 15% from ATM
    selected_date = datetime.today() + timedelta(days=365)
    limit_criteria = (
        (data.expiration < selected_date)
        & (data.strike > spot * 0.85)
        & (data.strike < spot * 1.15)
    )
    data = data.loc[limit_criteria]

    # Compute GEX by expiration and strike
    data = data.groupby(["expiration", "strike"])["GEX"].sum() / 10**6
    data = data.reset_index()
    if save_png:
        # Plot 3D surface
        fig = plt.figure()
        ax = fig.add_subplot(111, projection="3d")
        ax.plot_trisurf(
            data["strike"],
            dates.date2num(data["expiration"]),
            data["GEX"],
            cmap="seismic_r",
        )
        ax.yaxis.set_major_formatter(dates.AutoDateFormatter(ax.xaxis.get_major_locator()))
        ax.set_ylabel("Expiration date", fontweight="heavy")
        ax.set_xlabel("Strike Price", fontweight="heavy")
        ax.set_zlabel("Gamma (M$ / %)", fontweight="heavy")
        plt.title(f"ticker: {ticker}")
        plt.show()
        plt.savefig(os.path.join(TMP_FOLDER,f"{ticker}-gex-surface.png"))
        plt.close

    return data

if __name__ == "__main__":
    ticker = sys.argv[1]
    run(ticker)

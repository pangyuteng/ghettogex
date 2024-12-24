"""
source https://github.com/Matteo-Ferrara/gex-tracker
"""
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
plt.style.use("seaborn-dark")
for param in ["figure.facecolor", "axes.facecolor", "savefig.facecolor"]:
    plt.rcParams[param] = "#212946"
for param in ["text.color", "axes.labelcolor", "xtick.color", "ytick.color"]:
    plt.rcParams[param] = "0.9"

contract_size = 100


def run(ticker):
    spot_price, option_data = scrape_data(ticker)
    compute_total_gex(spot_price, option_data)
    compute_gex_by_strike(spot_price, option_data)
    compute_gex_by_expiration(option_data)
    print_gex_surface(spot_price, option_data)


MYFOLDER = os.environ.get("MYFOLDER","utils/tmp")
def scrape_data(ticker,save_as_json=False):
    """Scrape data from CBOE website"""
    # Check if data is already downloaded
    json_file = os.path.join(MYFOLDER,f"{ticker}.json")
    #if os.path.exists(json_file):
    if save_as_json:
        with open(json_file,"r") as f:
            data = pd.DataFrame.from_dict(json.loads(f.read()))
    else:
        # Request data and save it to file
        try:
            if ticker.startswith("^"):
                ticker = ticker.replace("^","")
                url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/_{ticker}.json"
            else:
                url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{ticker}.json"
            print(url)
            data = requests.get(url)
            if save_as_json:
                with open(json_file,"w") as f:
                    f.write(json.dumps(data.json()))

        except ValueError:
            traceback.print_exc()
            data = requests.get(
                f"https://cdn.cboe.com/api/global/delayed_quotes/options/{ticker}.json"
            )
            if save_as_json:
                with open(json_file,"w") as f:
                    f.write(json.dumps(data.json()))
        # Convert json to pandas DataFrame
        data = pd.DataFrame.from_dict(data.json())

    spot_price = data.loc["current_price", "data"]
    option_data = pd.DataFrame(data.loc["options", "data"])

    return spot_price, fix_option_data(option_data)




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
    data["GEX"] = data.apply(lambda x: -x.GEX if x.type == "P" else x.GEX, axis=1)
    print(f"Total notional GEX: ${round(data.GEX.sum() / 10 ** 9, 4)} Bn")


def compute_gex_by_strike(spot, data):
    """Compute and plot GEX by strike"""
    # Compute total GEX by strike
    gex_by_strike = data.groupby("strike")["GEX"].sum() / 10**9

    # Limit data to +- 15% from spot price
    limit_criteria = (gex_by_strike.index > spot * 0.85) & (gex_by_strike.index < spot * 1.15)

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
    plt.savefig(os.path.join(MYFOLDER,f"{ticker}-gex-by-strike.png"))
    plt.close

def compute_gex_by_expiration(data):
    """Compute and plot GEX by expiration"""
    # Limit data to one year
    selected_date = datetime.today() + timedelta(days=7)
    data = data.loc[data.expiration < selected_date]
    print(sorted(list(data.expiration.unique())))
    # Compute GEX by expiration date
    gex_by_expiration = data.groupby("expiration")["GEX"].sum() / 10**9

    # Plot GEX by expiration
    print(gex_by_expiration.index[0])
    print(type(gex_by_expiration.index[0]))
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
    plt.savefig(os.path.join(MYFOLDER,f"{ticker}-gex-by-expiration.png"))
    plt.close


def print_gex_surface(spot, data):
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
    plt.savefig(os.path.join(MYFOLDER,f"{ticker}-gex-surface.png"))
    plt.close

if __name__ == "__main__":
    ticker = sys.argv[1].upper()
    run(ticker)

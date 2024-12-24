import sys
import pathlib
import datetime
import traceback
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from py_vollib.ref_python.black_scholes_merton import black_scholes_merton
from .data_yahoo import (
    CACHE_FOLDER,
    BTC_TICKER,
    INDEX_TICKER_LIST,
    BTC_TICKER_LIST,
    get_cache_latest
)

def get_iv_df(ticker,option_type,tstamp=None,is_pivot=True):
    
    underlying_dict,options_df,last_json_file,last_csv_file = get_cache_latest(ticker,tstamp=tstamp)
    if "SPX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "SPXW" in x)]
    elif "NDX" in ticker:
        df = options_df[options_df.option.apply(lambda x: "NDXP" in x)]
    else:
        df = options_df
    df = df[df.option_type==option_type]
    # TODO: fill NA with theoretical IV
    df = df.sort_values(by=['expiration', 'strike','iv'])
    df = df[['expiration','strike','iv']]
    if is_pivot:
        try:
            iv_df = df.pivot(index='expiration', columns='strike', values='iv')
        except:
            traceback.print_exc()
        return iv_df
    else:
        return df

    #contractSymbol,lastTradeDate,strike,lastPrice,bid,ask,change,percentChange,
    #volume,openInterest,iv,inTheMoney,contractSize,currency,ticker,option_type,expiration

if __name__ == "__main__":
    # for ticker in INDEX_TICKER_LIST:
    # for option_type in ["C","P"]:
    ticker = sys.argv[1]
    option_type = sys.argv[2]

    df = get_iv_df(ticker,option_type,is_pivot=False)
    df.to_csv("ok.csv")
    for x in sorted(df.expiration.unique()):
        rowdf = df[df.expiration==x]
        plt.scatter(rowdf.strike,rowdf.iv,label=x)
    plt.grid(True)
    plt.legend()
    plt.title(f'{ticker} {option_type}')
    plt.savefig(f'ok-{ticker}-{option_type}')

"""

python -m utils.plotter MSTR C

"""
"""
{"address1": "1850 Towers Crescent Plaza", "city": "Tysons Corner", "state":
"VA", "zip": "22182", "country": "United States", "phone": "703 848 8600",
"fax": "703 848 8610", "website": "https://www.microstrategy.com", "industry":
"Software - Application", "industryKey": "software-application", "industryDisp": 
"Software - Application", "sector": "Technology", "sectorKey": "technology", 
"sectorDisp": "Technology", "longBusinessSummary": "MicroStrategy Incorporated provides artificial intelligence-powered enterprise analytics software and services in the United States, Europe, the Middle East, Africa, and internationally. It offers MicroStrategy ONE, which provides non-technical users with the ability to directly access novel and actionable insights for decision-making; and MicroStrategy Cloud for Government service, which offers always-on threat monitoring that meets the rigorous technical and regulatory needs of governments and financial institutions. The company also provides MicroStrategy Support that helps customers achieve their system availability and usage goals through highly responsive troubleshooting and assistance; MicroStrategy Consulting, which provides architecture and implementation services to help customers realize their desired results; and MicroStrategy Education that offers free and paid learning options. In addition, it engages in the development of bitcoin. The company offers its services through direct sales force and channel partners. It serves the U.S. government, state and local governments, and government agencies, as well as a range of industries, including retail, banking, technology, manufacturing, insurance, consulting, healthcare, telecommunications, and the public sector. The company was incorporated in 1989 and is headquartered in Tysons Corner, Virginia.", "fullTimeEmployees": 1637, 
"companyOfficers": [{"maxAge": 1, "name": "Mr. Michael J. Saylor", "age": 58, 
"title": "Executive Chairman", "yearBorn": 1965, "fiscalYear": 2023, "totalPay": 799670, "exercisedValue": 0,
"unexercisedValue": 204076000}, {"maxAge": 1, "name": "Mr. Phong Q. Le", "age": 46, 
"title": "President, CEO & Director", "yearBorn": 1977, "fiscalYear": 2023, "totalPay": 2073872, 
"exercisedValue": 8898232, "unexercisedValue": 41690848}, {"maxAge": 1, "name": "Mr. Andrew  Kang", "age": 47,
"title": "Senior EVP & CFO", "yearBorn": 1976, "fiscalYear": 2023, "totalPay": 1111067, 
"exercisedValue": 0, "unexercisedValue": 1628175},
{"maxAge": 1, "name": "Mr. Wei-Ming  Shao J.D.", 
"age": 54, "title": "Senior EVP, General Counsel & Secretary", "yearBorn": 1969,
"fiscalYear": 2023, "totalPay": 1117344, "exercisedValue": 2368344, "unexercisedValue": 9418450}, 
{"maxAge": 1, "name": "Ms. Jeanine J. Montgomery", "age": 61, "title": 
"Senior VP & Chief Accounting Officer", "yearBorn": 1962, "fiscalYear": 2023, "exercisedValue": 0,
"unexercisedValue": 0}, {"maxAge": 1, "name": "Ponna  Arumugam", 
"title": "Senior Executive VP & Chief Information Officer", "fiscalYear": 2023, 
"exercisedValue": 0, "unexercisedValue": 0}, {"maxAge": 1, "name": "Shirish  Jajodia", 
"title": "Senior Director of Treasury & Head of Investor Relations", "fiscalYear": 2023, 
"exercisedValue": 0, "unexercisedValue": 0}, {"maxAge": 1, "name": "Ms. Carla  Fitzgerald", 
"age": 58, "title": "Executive VP & Chief Marketing Officer", "yearBorn": 1965, "fiscalYear": 2023, 
"exercisedValue": 0, "unexercisedValue": 0}, {"maxAge": 1, "name": "Ms. Joty  Paparello", "title": 
"Executive VP & Chief Human Resource Officer", "fiscalYear": 2023, "exercisedValue": 0, "unexercisedValue": 0},
{"maxAge": 1, "name": "Mr. Saurabh  Abhyankar", "title": "Executive VP & Chief Product Officer", 
"fiscalYear": 2023, "exercisedValue": 0, "unexercisedValue": 0}], "auditRisk": 10, 
"boardRisk": 10, "compensationRisk": 5, "shareHolderRightsRisk": 10, "overallRisk": 10, 
"governanceEpochDate": 1733011200, "compensationAsOfEpochDate": 1703980800, 
"irWebsite": "http://ir.microstrategy.com/index.cfm", "maxAge": 86400, "priceHint": 2, 
"previousClose": 332.23, "open": 343.55, "dayLow": 342.7, "dayHigh": 355.8899,
"regularMarketPreviousClose": 332.23, "regularMarketOpen": 343.55, "regularMarketDayLow": 342.7,
"regularMarketDayHigh": 355.8899, "beta": 3.062, "forwardPE": -15810.511, "volume": 7616260, 
"regularMarketVolume": 7616260, "averageVolume": 26910801, "averageVolume10days": 26112720, 
"averageDailyVolume10Day": 26112720, "bid": 340.0, "ask": 374.05, "bidSize": 200, 
"askSize": 200, "marketCap": 87102439424, "fiftyTwoWeekLow": 43.874, "fiftyTwoWeekHigh": 543.0,
"priceToSalesTrailing12Months": 186.41786, "fiftyDayAverage": 320.1156, 
"twoHundredDayAverage": 190.94266, "currency": "USD", "enterpriseValue": 78026637312, 
"profitMargins": -0.87047994, "floatShares": 182475050, "sharesOutstanding": 225211008,
"sharesShort": 22880700, "sharesShortPriorMonth": 28329997, "sharesShortPreviousMonthDate": 1730332800, 
"dateShortInterest": 1732838400, "sharesPercentSharesOut": 0.099300005, "heldPercentInsiders": 0.00201, 
"heldPercentInstitutions": 0.42443, "shortRatio": 0.63, "shortPercentOfFloat": 0.1087,
"impliedSharesOutstanding": 244851008, "bookValue": 18.622, "priceToBook": 19.103024,
"lastFiscalYearEnd": 1703980800, "nextFiscalYearEnd": 1735603200, "mostRecentQuarter": 1727654400, 
"netIncomeToCommon": -406724992, "trailingEps": -2.49, "forwardEps": -0.43, "lastSplitFactor": "10:1",
"lastSplitDate": 1723075200, "enterpriseToRevenue": 166.994, "enterpriseToEbitda": -89.847, 
"52WeekChange": 4.5014987, "SandP52WeekChange": 0.2511797, "exchange": "NMS", 
"quoteType": "EQUITY", "symbol": "MSTR", "underlyingSymbol": "MSTR", 
"shortName": "MicroStrategy Incorporated", "longName": "MicroStrategy Incorporated", 
"firstTradeDateEpochUtc": 897571800, "timeZoneFullName": "America/New_York",
"timeZoneShortName": "EST", "uuid": "de1ea767-7a66-3fa0-88ae-72a03e814b2b", 
"messageBoardId": "finmb_384976", "gmtOffSetMilliseconds": -18000000, 
"currentPrice": 355.7365, "targetHighPrice": 650.0, "targetLowPrice": 212.11, 
"targetMeanPrice": 518.6789, "targetMedianPrice": 550.0, "recommendationMean": 1.33333,
"recommendationKey": "strong_buy", "numberOfAnalystOpinions": 9, "totalCash": 46343000, 
"totalCashPerShare": 0.229, "ebitda": -868441984, "totalDebt": 4269953024, 
"quickRatio": 0.56, "currentRatio": 0.646, "totalRevenue": 467243008, "debtToEquity": 113.152, 
"revenuePerShare": 2.509, "returnOnAssets": -0.09368, "returnOnEquity": -0.17629999, 
"freeCashflow": -5667349504, "operatingCashflow": -34524000, "revenueGrowth": -0.103, 
"grossMargins": 0.73564005, 
"ebitdaMargins": -1.8586501, "operatingMargins": -3.72687, "financialCurrency": "USD", "trailingPegRatio": null}

"""
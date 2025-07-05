
import os
import sys
import json
import traceback
import pathlib
import datetime
from datetime import timedelta
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt



def get_uw_df():

    pq_file = "uw.parquet.gzip"
    if not os.path.exists(pq_file):
        df = pd.read_csv("/mnt/hd2/data/finance/bot-eod/bot-eod-report-2024-12-31.csv")
        df.to_parquet(pq_file,compression='gzip',index=False)
    #underlying_symbol strike option_type underlying_price side size,premium,volume,open_interest,side
    return pd.read_parquet(pq_file)

def compare_contract(folder,uw_df):
    from cache_greeks import parse_symbol

    summary_folder = os.path.join(folder,'summary')
    timeandsale_folder = os.path.join(folder,'timeandsale')

    open_interest = None
    json_file_list = sorted([str(x) for x in sorted(pathlib.Path(summary_folder).rglob("*.json"))])
    
    oi_list = []
    for json_file in json_file_list:
        with open(json_file,'r') as f:
            content = json.loads(f.read())
            open_interest = content['open_interest']
            oi_list.append(open_interest)
    #print("summary file count and unique OI")
    #print(len(json_file_list),set(oi_list))

    tt_cols = ['type','aggressor_side','size','open_interest','event_symbol','json_file']
    mylist = []
    json_file_list = sorted([str(x) for x in sorted(pathlib.Path(timeandsale_folder).rglob("*.json"))])
    for json_file in json_file_list:
        with open(json_file,'r') as f:
            content = json.loads(f.read())
            print(json_file)
            print(content)
            content['json_file']=json_file
            content['open_interest']=open_interest
            item = {x:content[x] for x in tt_cols}
            mylist.append(item)
    tt_df = pd.DataFrame(mylist,columns=tt_cols)
    event_symbol = os.path.basename(folder)
    ticker,expiration,contract_type,strike = parse_symbol(event_symbol)
    
    # ask/buy size
    tt_ask_size = tt_df[tt_df.aggressor_side=='BUY']['size'].sum()
    tt_bid_size = tt_df[tt_df.aggressor_side=='SELL']['size'].sum()

    uw_symbol = f"{ticker}{expiration.strftime('%y%m%d')}{contract_type}0{int(strike)}000"
    item_uw_df = uw_df[uw_df.option_chain_id==uw_symbol]

    uw_ask_size = item_uw_df[item_uw_df.side=='ask']['size'].sum()
    uw_bid_size = item_uw_df[item_uw_df.side=='bid']['size'].sum()

    item = dict(
        event_type='summary',
        event_symbol=event_symbol,
        event_count=len(tt_df),
        tt_ask_size=tt_ask_size,
        tt_bid_size=tt_bid_size,
        uw_option_chain_id=uw_symbol,
        uw_count=len(item_uw_df),
        uw_ask_size=uw_ask_size,
        uw_bid_size=uw_bid_size,
    )
    print(item)
    return item


def compare_main():
    uw_df = get_uw_df()
    print('uw',uw_df.shape)

    csv_file = "merged.csv"
    if not os.path.exists(csv_file):
        day_root_folder = "/mnt/hd1/data/tastyfi/SPX/2024-12-31"
        folder_list = sorted([os.path.join(day_root_folder,x) for x in os.listdir(day_root_folder) if x.startswith('.SPXW')])
        mylist = []
        for folder in folder_list:
            myitem = compare_contract(folder,uw_df)
            mylist.append(myitem)

        df = pd.DataFrame(mylist)
        df.to_csv(csv_file,index=False)
    df = pd.read_csv(csv_file)
    print(df.shape)
    plt.scatter(df.uw_count,df.uw_count-df.event_count)
    plt.grid(True)
    plt.ylabel("(unusualwhales event count)-(dxlink event count)")
    plt.xlabel("unusualwhales event count")
    plt.title("event count comparison")
    plt.savefig("compare.png")
    plt.close()
    plt.scatter(df.uw_ask_size,df.uw_ask_size-df.tt_ask_size,label='side:ask')
    plt.scatter(df.uw_bid_size,df.uw_bid_size-df.tt_bid_size,label='side:bid')
    plt.legend()
    plt.ylabel("(unusualwhales bid/ask sum)-(dxlink bid/ask size sum)")
    plt.xlabel("(unusualwhales bid/ask sum)")
    plt.grid(True)
    plt.title("size sum diff for SPX 2024-12-31 expiry contracts \n on date 2024-12-31 between unusual whales vs dxLink")
    plt.tight_layout()
    plt.savefig("diff.png")
    plt.close()
    #event_type,event_symbol,event_count,
    #tt_ask_size,tt_bid_size,
    #uw_option_chain_id,uw_count,uw_ask_size,uw_bid_size


# /mnt/hd1/data/tastyfi/SPX/2024-12-31/.SPXW241231C8000/timeandsale/2024-12-31-12-39-16.502402-uid-63d71db356f94b28adf09584d5235d1f.json
TASTY_CACHE_ROOT = "/mnt/hd1/data/tastyfi"
def get_timeandsale(ticker,event_symbol,tstamp,freq):
    cols = ['aggressor_side', 'ask_price', 'bid_price', 'buyer', 'event_flags', 'event_symbol', 'event_time', 'exchange_code', 'exchange_sale_conditions', 'extended_trading_hours', 'index', 'price', 'seller', 'sequence', 'size', 'spread_leg', 'time', 'time_nano_part', 'trade_through_exempt', 'type', 'valid_tick']
    date_tstamp = tstamp.strftime("%Y-%m-%d")
    if freq == 's':
        time_tstamp = tstamp.strftime("%Y-%m-%d-%H-%M-%S")
    if freq == 'm':
        time_tstamp = tstamp.strftime("%Y-%m-%d-%H-%M")
    timeandsale_folder = os.path.join(TASTY_CACHE_ROOT,ticker,date_tstamp,event_symbol,"timeandsale")
    json_file_list = sorted([str(x) for x in pathlib.Path(timeandsale_folder).rglob(f"*{time_tstamp}*.json")])
    mylist = []
    for json_file in json_file_list:
        with open(json_file,'r') as f:
            mydict = json.loads(f.read())
        mylist.append(mydict)
    df = pd.DataFrame(mylist,columns=cols)
    return df

# /mnt/hd1/data/tastyfi/SPX/2024-12-31/.SPXW241231C1000/summary/2024-12-31-09-30-05.627393-uid-0fed1aa873fb4206b872a7819852906f.json
def get_summary(ticker,event_symbol,tstamp,freq):
    date_tstamp = tstamp.strftime("%Y-%m-%d")
    if freq == 's':
        time_tstamp = tstamp.strftime("%Y-%m-%d-%H-%M-%S")
    if freq == 'm':
        time_tstamp = tstamp.strftime("%Y-%m-%d-%H-%M")
    summary_folder = os.path.join(TASTY_CACHE_ROOT,ticker,date_tstamp,event_symbol,"summary")

    file_list = sorted([str(x) for x in pathlib.Path(summary_folder).rglob("*.json")])
    if len(file_list) > 0:
        json_file = file_list[0] # get first one
        with open(json_file,'r') as f:
            content = json.loads(f.read())
        return content
    else:
        return None

# oi/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv
# gex/$TICKER/YYYY-MM-DD/YYYY-MM-DD-HH-MM-SS.csv
TEST_CACHE_ROOT = "/mnt/hd1/data/test-cache"
def get_oi_file(ticker,event_symbol,tstamp,freq):
    
    date_tstamp = tstamp.strftime("%Y-%m-%d")
    if freq == 's':
        time_tstamp = tstamp.strftime("%Y-%m-%d-%H-%M-%S")
    if freq == 'm':
        time_tstamp = tstamp.strftime("%Y-%m-%d-%H-%M")

    json_file = os.path.join(TEST_CACHE_ROOT,ticker,date_tstamp,event_symbol,f"oi-{time_tstamp}.json")
    return json_file

def get_oi(ticker,event_symbol,tstamp,freq):
    oi_json_file = get_oi_file(ticker,event_symbol,tstamp,freq)
    if not os.path.exists(oi_json_file):
        return None
    try:
        with open(oi_json_file,'r') as f:
            content = json.loads(f.read())
    except:
        traceback.print_exc()
        print(oi_json_file)
        sys.exit(1)
    return content

def cache_oi(ticker,event_symbol,tstamp,is_init,freq):
    oi_json_file = get_oi_file(ticker,event_symbol,tstamp)
    os.makedirs(os.path.dirname(oi_json_file),exist_ok=True)
    if is_init:
        # grab and save prior OI
        summary = get_summary(ticker,event_symbol,tstamp,freq)
        oi_dict = {"open_interest":summary["open_interest"]}
        with open(oi_json_file,"w") as f:
            f.write(json.dumps(oi_dict))
    else:
        prior_tstamp = tstamp-timedelta(seconds=1)
        prior_oi = get_oi(ticker,event_symbol,prior_tstamp,freq)
        ts_df = get_timeandsale(ticker,event_symbol,tstamp,freq)
        # dilemma, OI have no bid side OI or ask side OI...
        if len(ts_df) == 0:
            oi_dict = {"open_interest":prior_oi["open_interest"]}
            with open(oi_json_file,"w") as f:
                f.write(json.dumps(oi_dict))
        else:
            open_interest = prior_oi["open_interest"]
            ts_ask_size = ts_df[ts_df.aggressor_side=='BUY']['size'].sum()
            ts_bid_size = ts_df[ts_df.aggressor_side=='SELL']['size'].sum()
            # TOFO: BAD ASSUMPTION
            updated_open_interest = int(open_interest+ts_ask_size-ts_bid_size)
            oi_dict = {"open_interest":updated_open_interest}
            with open(oi_json_file,"w") as f:
                f.write(json.dumps(oi_dict))

def cache_gex(event_symbol,tstamp,oi_dict,freq):
    #print("cache_gex",event_symbol,tstamp,len(oi_dict))
    pass

def get_gex(ticker,tstamp,freq):
    return None
    pass
    #print("get_gex",ticker,tstamp)

def cache_one_day_gex(ticker):
    root_folder = "/mnt/hd1/data/tastyfi"
    tstamp = datetime.datetime(2024,12,31)
    date_stamp_str = tstamp.strftime("%Y-%m-%d")
    day_root_folder = os.path.join(root_folder,ticker,date_stamp_str)
    underlyling_folder = os.path.join(root_folder,ticker,date_stamp_str,ticker)
    print(len(os.listdir(underlyling_folder)))
    event_symbol_list = sorted([x for x in os.listdir(day_root_folder) if x.startswith('.SPXW')])
    print(len(event_symbol_list))

    #freq = 's'
    freq = 'm'
    if freq =='s':
        tstamp_list = pd.date_range(start="2024-12-31 09:30:00",end="2024-12-31 16:30:00",freq='s')
    if freq == 'm':
        tstamp_list = pd.date_range(start="2024-12-31 09:30",end="2024-12-31 16:30",freq='m')

    for tstamp in tqdm(tstamp_list):
        oi_dict = {}
        for event_symbol in event_symbol_list:
            is_init = True if tstamp == tstamp_list[0] else False
            oi = get_oi(ticker,event_symbol,tstamp,freq)
            if oi is None:
                cache_oi(ticker,event_symbol,tstamp,is_init)
                oi = get_oi(ticker,event_symbol,tstamp,freq)
            if oi is None:
                raise ValueError()
            oi_dict['event_symbol']=oi
        gex = get_gex(ticker,tstamp,freq)
        if gex is None:
            cache_gex(ticker,tstamp,oi_dict,freq)

if __name__ == "__main__":
    # compare_main()
    ticker = "SPX"
    cache_one_day_gex(ticker)
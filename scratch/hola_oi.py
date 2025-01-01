
import os
import sys
import json
import pathlib
import pandas as pd


def get_uw_df():

    pq_file = "uw.parquet.gzip"
    if not os.path.exists(pq_file):
        df = pd.read_csv("/mnt/hd2/data/finance/bot-eod/bot-eod-report-2024-12-31.csv")
        df.to_parquet(pq_file,compression='gzip',index=False)
    #underlying_symbol strike option_type underlying_price side size,premium,volume,open_interest,side
    return pd.read_parquet(pq_file)

uw_df = get_uw_df()
print('uw',uw_df.shape)

def main(folder):
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
            content['json_file']=json_file
            content['open_interest']=open_interest
            item = {x:content[x] for x in tt_cols}
            mylist.append(item)
    tt_df = pd.DataFrame(mylist,columns=tt_cols)
    event_symbol = os.path.basename(folder)
    ticker,expiration,contract_type,strike = parse_symbol(event_symbol)

    uw_symbol = f"{ticker}{expiration.strftime('%y%m%d')}{contract_type}0{int(strike)}000"
    tmp_uw_df = uw_df[uw_df.option_chain_id==uw_symbol]

    item = dict(
        event_type='summary',
        event_symbol=event_symbol,
        event_count=len(tt_df),
        aggressor_side_BUY_count=int((tt_df.aggressor_side=='BUY').sum()),
        aggressor_side_SELL_count=int((tt_df.aggressor_side=='SELL').sum()),
        uw_option_chain_id=uw_symbol,
        uw_count=len(tmp_uw_df),
        uw_side_ask=int((tmp_uw_df.side=='ask').sum()),
        uw_side_bid=int((tmp_uw_df.side=='bid').sum()),
    )
    print(item)
    return item


from cache_greeks import parse_symbol

if __name__ == "__main__":

    day_root_folder = "/mnt/hd1/data/tastyfi/SPX/2024-12-31"
    folder_list = sorted([os.path.join(day_root_folder,x) for x in os.listdir(day_root_folder) if x.startswith('.SPXW')])
    mylist = []
    for folder in folder_list:
        myitem = main(folder)
        mylist.append(myitem)

    df = pd.DataFrame(mylist)
    df.to_csv("ok.csv",index=False)




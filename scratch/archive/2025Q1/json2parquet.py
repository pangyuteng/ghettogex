import os
import sys
import traceback
import pathlib
import datetime
import json
import numpy as np
import pandas as pd
import re
import asyncio
import aiofiles
import time
from tqdm import tqdm
import matplotlib.pyplot as plt
import PIL
from moviepy import editor



# sample streamer_symbol ".TSLA240927C105"
PATTERN = r"\.([A-Z]+)(\d{6})([CP])(\d+)"

def parse_symbol(streamer_symbol):
    matched = re.match(PATTERN,streamer_symbol)
    ticker = matched.group(1)
    expiration = datetime.datetime.strptime(matched.group(2),'%y%m%d').date()
    contract_type = matched.group(3)
    strike = float(matched.group(4))
    return ticker,expiration,contract_type,strike


async def get_json(json_file):

    event_dict = {"json_file":json_file}

    file_split = json_file.split("/")
    tstamp, uid = file_split[-1].replace(".json","").split("-uid-")
    event_type = file_split[-2]
    streamer_symbol = file_split[-3]
    ## TOFO: parse later?
    #tstamp = datetime.datetime.strptime(tstamp,'%Y-%m-%d-%H-%M-%S.%f')

    async with aiofiles.open(json_file, mode='r') as f:
        content = json.loads(await f.read())

    event_dict.update(content)
    if streamer_symbol.startswith("."):
        ticker,expiration,contract_type,strike = parse_symbol(streamer_symbol)
        event_dict['ticker']=ticker
        event_dict['expiration']=expiration
        event_dict['contract_type']=contract_type
        event_dict['strike']=strike
    else:
        event_dict['ticker']=streamer_symbol

    event_dict["streamer_symbol"]=streamer_symbol
    event_dict["event_type"]=event_type
    event_dict["uid"]=uid
    event_dict["tstamp"]=tstamp
    return event_dict

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

async def aggregate_json2parquet(ticker,tstamp,pq_file):
    date_stamp_str = tstamp.strftime("%Y-%m-%d")
    ticker_folder = f"/mnt/hd1/data/tastyfi/{ticker}/{date_stamp_str}"
    print(f"started at {time.strftime('%X')}")
    json_file_list = [str(x) for x in pathlib.Path(ticker_folder).rglob(f"*.json")]
    print('file count',len(json_file_list))
    print(f"find done. {time.strftime('%X')}")

    data_list = []
    """
    for json_file in tqdm(json_file_list):
        try:
            mydict = await get_json(json_file)
            data_list.append(mydict)
        except:
            print(json_file)
            traceback.print_exc()
    """
    chunk_n = 1000
    list_of_list = list(chunks(json_file_list, chunk_n))
    for mylist in tqdm(list_of_list):
        func_list = [get_json(x) for x in mylist]
        ret_list = await asyncio.gather(*func_list)
        data_list.extend(ret_list)
    
    print(f"parse done {time.strftime('%X')}")
    print("saving...")
    df = pd.DataFrame(data_list)
    df.to_parquet(pq_file,compression='gzip')
    print(f"finished at {time.strftime('%X')}")
    return df

oi_csv_file = "oi.csv"
greeks_csv_file = "greeks.csv"
spot_csv_file = "spot_price.csv"
def cache_oi(ticker,tstamp):
    date_stamp_str = tstamp.strftime("%Y-%m-%d")
    if all([os.path.exists(x) for x in [oi_csv_file,spot_csv_file]]):
        return

    if ticker == 'SPX':
        ticker_variants = ["SPX","SPXW"]
    else:
        ticker_variants = [ticker]

    os.makedirs('tmp/pngs',exist_ok=True)
    pq_file = f'tmp/{ticker}-{date_stamp_str}.parquet.gzip'

    tstamp_list = pd.date_range(start="2024-12-31 09:30:00",end="2024-12-31 16:30:00",freq='s')
    tstamp_list = sorted(list(set([x.replace(second=0) for x in tstamp_list])))

    if not os.path.exists(pq_file):
        df = asyncio.run(aggregate_json2parquet(ticker,tstamp,pq_file))
    else:
        df = pd.read_parquet(pq_file)

    options_df = df[df.strike.notnull()]
    event_symbol_list = sorted(list(options_df.event_symbol.unique()))

    df.tstamp = df.tstamp.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d-%H-%M-%S.%f'))
    df['tstamp_reduced'] = df.tstamp.apply(lambda x: x.replace(second=0,microsecond=0))

    df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    df['bid_volume'] = pd.to_numeric(df['bid_volume'], errors='coerce')
    df['ask_volume'] = pd.to_numeric(df['ask_volume'], errors='coerce')
    df['gamma'] = pd.to_numeric(df['gamma'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['size'] = pd.to_numeric(df['size'], errors='coerce')
    df['size_signed'] = df['size'].where(df.aggressor_side == 'BUY', other=-1*df['size']) #"BUY","SELL":
    df['bid_price'] = pd.to_numeric(df['bid_price'], errors='coerce')
    df['ask_price'] = pd.to_numeric(df['ask_price'], errors='coerce')
    df['mid_price'] = (df['bid_price']+df['ask_price'])/2
    df['gamma'] = pd.to_numeric(df['gamma'], errors='coerce')


    greeks_list = []
    for tstamp in tqdm(tstamp_list):
        g_df = df[(df.tstamp_reduced==tstamp)&(df.event_type=='greeks')&(df.ticker.apply(lambda x: x in ticker_variants))&(df.strike.notnull())]
        greeks_cols =         ['tstamp_reduced','event_symbol','event_type','expiration','contract_type','strike','gamma']
        groupby_greeks_cols = ['tstamp_reduced','event_symbol','event_type','expiration','contract_type','strike']
        g_df = g_df[greeks_cols]
        g_df = g_df.groupby(groupby_greeks_cols).last().reset_index()
        print(tstamp,len(g_df))
        greeks_list.append(g_df)
    greeks_df = pd.concat(greeks_list)
    greeks_df.to_csv(greeks_csv_file,index=False)

    price_list = []
    for tstamp in tqdm(tstamp_list):
        if False: # TOFO: unsure why no data?
            u_df = df[(df.tstamp_reduced==tstamp)&(df.event_type=='candle')&(df.event_symbol==ticker)&(df.strike.isnull())]
            price_list.append(u_df)
        q_df = df[(df.tstamp_reduced==tstamp)&(df.event_type=='quote')&(df.event_symbol==ticker)&(df.strike.isnull())]
        price_cols =         ['tstamp_reduced','event_symbol','mid_price']
        groupby_price_cols = ['tstamp_reduced','event_symbol']
        q_df = q_df[price_cols]
        q_df = q_df.groupby(groupby_price_cols).last().reset_index()
        price_list.append(q_df)
    price_df = pd.concat(price_list)
    price_df.to_csv(spot_csv_file,index=False)

    oi_list = []
    for event_symbol in tqdm(event_symbol_list):
        print(event_symbol)
        u_df = df[(df.event_type=='candle')&(df.event_symbol==ticker)&(df.strike.isnull())]
        print(len(u_df))
        c_df = df[(df.event_type=="candle")&(df.event_symbol==event_symbol)&(df.strike.notnull())]
        print(len(c_df))
        s_df = df[(df.event_type=="summary")&(df.event_symbol==event_symbol)&(df.strike.notnull())]
        print(len(s_df))
        ts_df = df[(df.event_type=="timeandsale")&(df.event_symbol==event_symbol)&(df.strike.notnull())]
        print(len(ts_df))
        if len(s_df) > 0:
            open_interest = s_df['open_interest'].to_numpy()[0]
        else:
            open_interest = 0

        print(event_symbol,open_interest)
        print(len(ts_df))

        if len(ts_df) > 0:
            # create new df
            tmp_cols = [
                'tstamp_reduced','tstamp','event_symbol','event_type','size_signed', # 'aggressor_side','size','uid','json_file'
                'expiration','contract_type','strike'
            ]
            tmp_df = ts_df.copy(deep=True).reset_index()
            tmp_df = tmp_df[tmp_cols]
            tmp_df = tmp_df.sort_values(["tstamp"],ascending=True)
            tmp_df['open_interest'] = open_interest
            tmp_df['latest_open_interest'] = tmp_df.size_signed.cumsum()+open_interest
            oi_cols =         ['tstamp_reduced','event_symbol','expiration','contract_type','strike','open_interest','latest_open_interest']
            groupby_oi_cols = ['tstamp_reduced','event_symbol','expiration','contract_type','strike']
            tmp_df = tmp_df[oi_cols]
            #open_interest
            tmp_df = tmp_df.groupby(groupby_oi_cols).last().reset_index()
            oi_list.append(tmp_df)

    oi_pd = pd.concat(oi_list)
    oi_pd.to_csv(oi_csv_file,index=False)

def gen_ani(ticker,tstamp):
    cache_oi(ticker,tstamp)
    spot_df = pd.read_csv(spot_csv_file)
    spot_df = spot_df.rename(columns={'mid_price':'spot_price'})
    spot_df = spot_df[['tstamp_reduced','spot_price']]

    oi_df = pd.read_csv(oi_csv_file)
    greeks_df = pd.read_csv(greeks_csv_file)

    spot_df.tstamp_reduced = spot_df.tstamp_reduced.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
    oi_df.tstamp_reduced = oi_df.tstamp_reduced.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
    oi_df = oi_df[['tstamp_reduced','event_symbol','expiration','contract_type','strike','latest_open_interest']]

    greeks_df.tstamp_reduced = greeks_df.tstamp_reduced.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
    greeks_df = greeks_df[['tstamp_reduced','event_symbol','expiration','contract_type','strike','gamma']]
    
    merged_df = spot_df.merge(oi_df,how='left',on=['tstamp_reduced'])
    merged_df = merged_df.merge(greeks_df,how='left',on=['tstamp_reduced','event_symbol','expiration','contract_type','strike'])
    merged_df['contract_type_int'] = merged_df.contract_type.apply(lambda x: -1 if x == 'P' else 1)
    merged_df['latest_open_interest'] = pd.to_numeric(merged_df['latest_open_interest'], errors='coerce')
    merged_df['gamma'] = pd.to_numeric(merged_df['gamma'], errors='coerce')
    merged_df['strike'] = pd.to_numeric(merged_df['strike'], errors='coerce')
    merged_df['gex'] = merged_df.gamma * merged_df.latest_open_interest * 100 * merged_df.spot_price * merged_df.spot_price * 0.01 * merged_df.contract_type_int / 1e9
    merged_df = merged_df[['tstamp_reduced','strike','spot_price','gex']]
    gex_df = merged_df.groupby(['tstamp_reduced','strike','spot_price']).sum().reset_index()
    
    max_strike,min_strike = np.max(merged_df.spot_price)*1.05,np.min(merged_df.spot_price)*.95
    max_gex = np.max(np.abs(merged_df.gex))
    max_gex = 3.5
    png_file_list = []
    for tstamp_reduced in sorted(gex_df.tstamp_reduced.unique()):
        sec_png_file = os.path.join('tmp/pngs',tstamp_reduced.strftime("%Y-%m-%d-%H-%M-%S")+".png")
        item_df = gex_df[gex_df.tstamp_reduced==tstamp_reduced]
        if len(item_df) > 0:
            spot_price = item_df.spot_price.to_list()[-1]
        else:
            spot_price = np.nan
        plt.figure()
        for n,row in item_df.iterrows():
            if row.gex > 0:
                color = 'green'
            else:
                color = 'red'
            plt.plot([0,row.gex],[row.strike,row.strike],color=color)

        plt.axhline(spot_price,color='blue',linestyle='--')
        print(tstamp_reduced,spot_price)
        plt.grid(True)
        message = f"ticker: {ticker}\ntstamp: {tstamp_reduced}\nprice: {spot_price:1.2f}"
        plt.title(message)
        plt.xlim(-max_gex,max_gex)
        plt.ylim(min_strike,max_strike)
        plt.ylabel("strike")
        plt.xlabel("naive gex ($Bn per 1% move)")
        plt.tight_layout()
        foot_note = """
        gamma - via greeks event
        prior day open interest - via  summary event
        oi changes (size,side) - via timeandsale event
        underlying spot price -via quote event
        """
        plt.text(-2,(min_strike+max_strike)/2,foot_note)

        plt.savefig(sec_png_file)
        plt.close()
        png_file_list.append(sec_png_file)

    file_list = []
    file_list.extend(png_file_list)
    fps = 10
    duration = (len(png_file_list))/10
    print(duration)
    time_list = list(np.arange(0,duration,1./fps))
    print(len(time_list))
    img_dict = {a:f for a,f in zip(time_list,file_list)}

    def make_frame(t):
        fpath= img_dict[t]
        im = PIL.Image.open(fpath)
        arr = np.asarray(im)
        return arr

    work_dir = 'tmp'
    gif_path = os.path.join(work_dir,f'ani.gif')
    video_file = os.path.join(work_dir,f"ani.mp4")
    print(video_file)
    clip = editor.VideoClip(make_frame, duration=duration)
    clip.write_gif(gif_path, fps=fps)
    clip.write_videofile(video_file, fps=fps)
    print(os.path.exists(video_file))


def main(ticker,tstamp):
    gen_ani(ticker,tstamp)

if __name__ == "__main__":

    ticker = 'SPX'
    tstamp = datetime.datetime(2024,12,31)
    main(ticker,tstamp)

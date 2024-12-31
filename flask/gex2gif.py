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


def hola_cboe():
    ticker = '^SPX'
    ticker_folder = f"/mnt/hd1/data/fi/{ticker}/2024"
    csv_file_list = sorted([str(x) for x in pathlib.Path(ticker_folder).rglob("*.csv")])
    json_file_list = sorted([str(x) for x in pathlib.Path(ticker_folder).rglob("*.json")])

    for json_file in json_file_list:
        with open(json_file,'r') as f:
            content = json.loads(f.read())
        print(json_file,content['last_price'])
        break
    for csv_file in csv_file_list:
        df = pd.read_csv(csv_file)
        print(csv_file,df.spot_price[0],df.open_interest.sum())


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
    ## TODO: parse later?
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

async def main(ticker,tstamp,pq_file):
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

min_prct,max_prct = 0.96,1.04
def get_gex(df,tstamp_reduced,ticker,ticker_variants,mean_price):
    
    # ['candle','quote']
    # underlying spot price via candle events
    udf = df[(df.tstamp_reduced==tstamp_reduced)&(df.event_type=='candle')&(df.strike.isnull())&(df.streamer_symbol==ticker)]
    udf = udf.sort_values(['tstamp']).reset_index()
    spot_price = np.nan
    if len(udf) > 0:
        spot_price = float(udf.iloc[-1].close)
        # print(tstamp_reduced,spot_price)

    # ['candle','greeks','profile','trade','summary']
    # sum options volumes via candle events
    event_type = 'candle'
    cdf = df[(df.tstamp_reduced==tstamp_reduced)&(df.event_type==event_type)&(df.strike.notnull())&(df.ticker.apply(lambda x: x in ticker_variants))]
    cdf = cdf[["streamer_symbol","strike","ticker", "expiration", "contract_type","volume","ask_volume","bid_volume"]]
    cdf = cdf.groupby(["streamer_symbol","strike","ticker", "expiration", "contract_type"]).sum()
    cdf = cdf.reset_index()

    event_type = 'greeks'
    gdf = df[(df.tstamp_reduced==tstamp_reduced)&(df.event_type==event_type)&(df.strike.notnull())&(df.ticker.apply(lambda x: x in ticker_variants))]
    gdf = gdf.sort_values(['tstamp'])
    gdf = gdf[["streamer_symbol","strike","ticker", "expiration", "contract_type","gamma"]]
    gdf = gdf.groupby(["streamer_symbol","strike","ticker", "expiration", "contract_type"]).last()
    gdf = gdf.reset_index()

    event_type = 'summary'
    sdf = df[(df.tstamp_reduced==tstamp_reduced)&(df.event_type==event_type)&(df.strike.notnull())&(df.ticker.apply(lambda x: x in ticker_variants))]
    sdf = sdf.sort_values(['tstamp'])
    sdf = sdf[["streamer_symbol","strike","ticker", "expiration", "contract_type","open_interest"]]
    sdf = sdf.groupby(["streamer_symbol","strike","ticker", "expiration", "contract_type"]).last()
    sdf = sdf.reset_index()

    idf = gdf.merge(cdf,how='left',on=["streamer_symbol","strike","ticker", "expiration", "contract_type"]).copy(deep=True)
    idf = idf.merge(sdf,how='left',on=["streamer_symbol","strike","ticker", "expiration", "contract_type"]).copy(deep=True)

    idf['contract_type_int'] = idf.contract_type.apply(lambda x: 1 if x=='C' else -1)
    idf['gex_volume'] = idf['gamma'].astype(np.float64) * idf['volume'].astype(np.float64) * 100 * spot_price * spot_price * 0.01 * idf['contract_type_int']
    idf['gex_oi'] = idf['gamma'].astype(np.float64) * idf['open_interest'].astype(np.float64) * 100 * spot_price * spot_price * 0.01 * idf['contract_type_int']

    # https://perfiliev.com/blog/how-to-calculate-gamma-exposure-and-zero-gamma-level
    # A crude approximation is that the dealers are long the calls and short the puts,

    #Ask Price - lowest price that sellers are willing to sell at
    #Bid Price - highest price that buyers are willing to pay for
    #Ask volume refers to transactions that happen at the ask price
    #Bid volume refers to transactions that happen at the bid price

    #assumption is dealer long call, 1, (near ask)
    #assumption is dealer short put, -1 (near bid)

    # i think below is what we want:
    # contract_type_int 1 , contract_type C, factor_bid: 1 factor_ask: 1
    # contract_type_int -1 , contract_type P, factor_bid: -1 factor_ask: 1
    idf['factor_bid_volume'] = idf.contract_type.apply(lambda x: -1 if x=='P' else 1)
    idf['factor_ask_volume'] = idf.contract_type.apply(lambda x: 1 if x=='C' else -1)
    idf['gex_bid_ask_volume'] = \
        idf['gamma'].astype(np.float64) * idf['ask_volume'].astype(np.float64) * 100 * spot_price * spot_price * 0.01 * idf['factor_ask_volume'] + \
        idf['gamma'].astype(np.float64) * idf['bid_volume'].astype(np.float64) * 100 * spot_price * spot_price * 0.01 * idf['factor_bid_volume']

    idf['gex_bid_ask_volume'] = idf['gex_bid_ask_volume']/10**9
    idf['gex_volume'] = idf['gex_volume']/10**9
    idf['gex_oi'] = idf['gex_oi']/10**9
    
    tmpdf = idf[ (idf.strike > spot_price*min_prct) & (idf.strike < spot_price*max_prct) ]
    naive_oi_gex = tmpdf.gex_oi[tmpdf.gex_oi.notnull()].sum()
    naive_vol_gex = tmpdf.gex_volume[tmpdf.gex_volume.notnull()].sum()
    bidask_vol_gex = tmpdf.gex_bid_ask_volume[tmpdf.gex_bid_ask_volume.notnull()].sum()

    idf = gdf.merge(cdf,how='left',on=["streamer_symbol","strike","ticker", "expiration", "contract_type"])
    
    sec_png_file = os.path.join('tmp/pngs',tstamp_reduced.strftime("%Y-%m-%d-%H-%M-%S")+".png")
    if bidask_vol_gex != naive_oi_gex:
        tmpdf = tmpdf[["strike","gex_oi"]]
        tmpdf = tmpdf.groupby(["strike"]).sum().reset_index()
        
        for n,row in tmpdf.iterrows():
            plt.plot([0,row.gex_oi],[row.strike,row.strike],color='blue')
        plt.axhline(spot_price,color='red')
        plt.xlim(-1,1) # $1BN on each side
        plt.ylim(mean_price*0.96,mean_price*1.04) # use mean price +/- 10%
        plt.grid(True)
        plt.title(f"{ticker} {spot_price}\ntotal GEX {naive_oi_gex:1.2f}\n{tstamp_reduced.strftime('%Y-%m-%d-%H-%M-%S')}")
        plt.ylabel("strike")
        plt.xlabel("Spot Gamma Exposure ($ BN per 1% move)")
        plt.savefig(sec_png_file)
        plt.close()

    return dict(
        tstamp_reduced=tstamp_reduced,
        spot_price=spot_price,
        naive_oi_gex=naive_oi_gex,
        naive_vol_gex=naive_vol_gex,
        bidask_vol_gex=bidask_vol_gex,
    )

def hola_tasty():

    ticker = 'SPX'
    tstamp = datetime.datetime(2024,12,30)
    date_stamp_str = tstamp.strftime("%Y-%m-%d")

    if ticker == 'SPX':
        ticker_variants = ["SPX","SPXW"]
    else:
        ticker_variants = [ticker]

    os.makedirs('tmp/pngs',exist_ok=True)
    pq_file = f'tmp/{ticker}-{date_stamp_str}.parquet.gzip'
    gex_csv_file = f'tmp/{ticker}-{date_stamp_str}-gex.csv'
    gex_png_file = f'tmp/{ticker}-{date_stamp_str}-gex.png'

    if not os.path.exists(gex_csv_file):

        if not os.path.exists(pq_file):
            df = asyncio.run(main(ticker,tstamp,pq_file))
        else:
            df = pd.read_parquet(pq_file)

        df.tstamp = df.tstamp.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d-%H-%M-%S.%f'))
        df['tstamp_reduced'] = df.tstamp.apply(lambda x: x.replace(second=0,microsecond=0))

        df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['bid_volume'] = pd.to_numeric(df['bid_volume'], errors='coerce')
        df['ask_volume'] = pd.to_numeric(df['ask_volume'], errors='coerce')
        df['gamma'] = pd.to_numeric(df['gamma'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')

        udf = df[(df.event_type=='candle')&(df.strike.isnull())&(df.streamer_symbol==ticker)]
        mean_price = udf.close.mean(skipna=True)
        print('mean_price',mean_price)

        mylist = []
        for tstamp_reduced in sorted(df.tstamp_reduced.unique()):
            try:
                row_dict = get_gex(df,tstamp_reduced,ticker,ticker_variants,mean_price)
                print(row_dict)
                mylist.append(row_dict)
            except KeyboardInterrupt:
                sys.exit(1)
            except:
                traceback.print_exc()
                pass
        gex_df = pd.DataFrame(mylist)
        gex_df.to_csv(gex_csv_file,index=False)

    if not os.path.exists(gex_png_file):
        print("generaing gif")
        gex_df = pd.read_csv(gex_csv_file)
        

        df = pd.read_parquet(pq_file)
        df.tstamp = df.tstamp.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d-%H-%M-%S.%f'))
        df['tstamp_reduced'] = df.tstamp.apply(lambda x: x.replace(second=0,microsecond=0))

        png_file_list = []
        for tstamp_reduced in sorted(df.tstamp_reduced.unique()):  
            sec_png_file = os.path.join('tmp/pngs',tstamp_reduced.strftime("%Y-%m-%d-%H-%M-%S")+".png")
            if os.path.exists(sec_png_file):
                png_file_list.append(sec_png_file)

        print('found pngs')
        print(len(png_file_list))

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

        gex_df = gex_df[gex_df.naive_oi_gex!=0]
        gex_df['tstamp'] = gex_df.tstamp_reduced.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        plt.subplot(311)
        plt.plot(gex_df.tstamp,gex_df.spot_price)
        plt.grid(True)
        plt.subplot(312)
        plt.plot(gex_df.tstamp,gex_df.naive_oi_gex)
        plt.grid(True)
        plt.subplot(313)
        plt.plot(gex_df.tstamp,gex_df.naive_oi_gex)
        plt.grid(True)
        plt.savefig(gex_png_file)

    
if __name__ == "__main__":

    action = sys.argv[1]

    if action == "tasty":
        hola_tasty()
    elif action == "cboe":
        hola_cboe()
    else:
        print("na")
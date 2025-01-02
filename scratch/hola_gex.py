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


def cache_data():

    ticker = 'SPX'
    tstamp = datetime.datetime(2024,12,31)
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
        

if __name__ == "__main__":
    cache_data()
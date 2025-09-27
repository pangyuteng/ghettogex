
import os
import sys
import json
import re
import datetime
import pathlib
import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import PIL
from moviepy import editor

# sample event_symbol ".TSLA240927C105"
PATTERN = r"\.([A-Z]+)(\d{6})([CP])(\d+)"

def parse_symbol(event_symbol):
    matched = re.match(PATTERN,event_symbol)
    ticker = matched.group(1)
    expiration = datetime.datetime.strptime(matched.group(2),'%y%m%d').date()
    contract_type = matched.group(3)
    strike = float(matched.group(4))
    return ticker,expiration,contract_type,strike

def process(json_file):
    event_dict = {}
    file_split = json_file.split("/")
    tstamp, uid = file_split[-1].replace(".json","").split("-uid-")
    event_type = file_split[-2]
    streamer_symbol = file_split[-3]
    with open(json_file, 'r') as f:
        content = json.loads(f.read())

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

def datetime_to_float(d):
    return d.timestamp()

def plot_iv(pq_file,ticker):
    
    df = pd.read_parquet(pq_file)
    print(df.columns)
    # 'delta', 'event_flags', 'event_symbol', 'event_time', 'gamma', 'index',
    #    'price', 'rho', 'sequence', 'theta', 'time', 'vega', 'volatility',
    #    'ticker', 'expiration', 'contract_type', 'strike', 'streamer_symbol',
    #    'event_type', 'uid', 'tstamp']
    

    cols = ['tstamp','contract_type','expiration','strike','volatility']
    df.tstamp = df.tstamp.apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d-%H-%M-%S.%f'))
    df.tstamp = df.tstamp.apply(lambda x: x.replace(second=0,microsecond=0))
    df.expiration = df.expiration.apply(lambda x: datetime.datetime.combine(x,datetime.datetime.min.time()))
    df.expiration = df.expiration.apply(lambda x: datetime_to_float(x))
    df = df[cols]
    print(type(df.expiration[0]))
    print(type(df.volatility[0]))
    print(type(df.strike[0]))
    df['volatility'] = pd.to_numeric(df['volatility'], errors='coerce')
    df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
    df['expiration'] = pd.to_numeric(df['expiration'], errors='coerce')
    print(type(df.expiration[0]))
    print(type(df.volatility[0]))
    print(type(df.strike[0]))
    print(df.columns)
    contract_type = 'C'
    df = df[df.contract_type==contract_type]
    print(df.contract_type)
    print(df.shape)
    png_file_list = []
    for tstamp in sorted(df.tstamp.unique()):
        tmp = df[df.tstamp==tstamp].reset_index()
        print(tstamp,len(tmp),len(df))
        png_file = os.path.join("tmp",f"iv-{ticker}-{tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.png")
        # Plot 3D surface
        fig = plt.figure()
        ax = fig.add_subplot(111, projection="3d")
        ax.plot_trisurf(
            tmp.strike,
            tmp.expiration,
            tmp.volatility,
            cmap="seismic_r",
        )
        #ax.yaxis.set_major_formatter(dates.AutoDateFormatter(ax.xaxis.get_major_locator()))
        ax.set_ylabel("Expiration date", fontweight="heavy")
        ax.set_xlabel("Strike Price", fontweight="heavy")
        ax.set_zlabel("Implied Volatility", fontweight="heavy")
        plt.title(f"ticker: {ticker}")
        plt.show()
        plt.savefig(png_file)
        plt.close
        png_file_list.append(png_file)

    return png_file_list

def main():

    ticker = 'SPX'
    pq_file = f'{ticker}.parquet.gzip'

    if not os.path.exists(pq_file):
        root_folder = os.path.join("tmp",ticker)
        json_file_list = [str(x) for x in pathlib.Path(root_folder).rglob("*.json")]
        json_file_list = [x for x in json_file_list if 'greeks' in x]

        print(len(json_file_list))
        mylist = []
        for json_file in tqdm(json_file_list):
            myitem = process(json_file)
            mylist.append(myitem)
        
        df = pd.DataFrame(mylist)
        df.to_parquet(pq_file,compression='gzip',index=False)

    png_file_list = plot_iv(pq_file,ticker)

    file_list = []
    file_list.extend(png_file_list)
    fps = 5
    duration = (len(png_file_list))/5
    print(duration)
    time_list = list(np.arange(0,duration,1./fps))
    print(len(time_list))
    img_dict = {a:f for a,f in zip(time_list,file_list)}
    print(img_dict)
    def make_frame(t):
        fpath= img_dict[t]
        im = PIL.Image.open(fpath)
        arr = np.asarray(im)
        return arr
    work_dir = 'tmp'
    gif_path = os.path.join(work_dir,f'ani.gif')
    video_file = os.path.join(work_dir,f"ani.mp4")
    clip = editor.VideoClip(make_frame, duration=duration)
    clip.write_gif(gif_path, fps=fps)
    clip.write_videofile(video_file, fps=fps)
    print(os.path.exists(video_file))
if __name__ == "__main__":
    main()
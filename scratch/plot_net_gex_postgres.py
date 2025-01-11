
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
from moviepy import ImageClip, concatenate_videoclips, VideoFileClip

sys.path.append("../flask")
from utils.postgres_utils import postgres_execute

work_dir = 'tmp'

def plot_iv(ticker,day_stamp):

    png_file_list = []

    query_str = """
    select * from gex_net where ticker = %s and tstamp::date = %s
    order by tstamp
    """
    query_args = (ticker,day_stamp)
    fetched = postgres_execute(query_str,query_args)

    ndf = pd.DataFrame(fetched)
    ndf = ndf[['tstamp','spot_price']]

    query_str = """
    select * from gex_strike where ticker = %s and tstamp::date = %s
    order by tstamp
    """
    query_args = (ticker,day_stamp)
    fetched = postgres_execute(query_str,query_args)

    df = pd.DataFrame(fetched)
    df = df.merge(ndf,how='left',on=['tstamp'])
    df.naive_gex = df.naive_gex/1e9

    print(df.columns)
    print(df.shape)
    df = df[['strike','naive_gex','spot_price''tstamp']]
    df = df.dropna()
    print(df.shape)


    tstamp_list = sorted(list(df.tstamp.unique()))
    for tstamp in tqdm(tstamp_list):

        tmp = df[df.tstamp==tstamp].reset_index()

        try:
            spot_price = tmp.spot_price.to_list()[-1]
        except:
            spot_price = np.nan
        print(tstamp,spot_price)

        png_file = os.path.join(work_dir,"pngs",f"gex-{ticker}-{tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.png")
        # Plot 3D surface
        fig = plt.figure()
        if False:
            ax = fig.add_subplot(111, projection="3d")
            ax.plot_trisurf(
                tmp.strike,
                tmp.expiration,
                tmp.naive_gex,
                cmap="seismic_r",
            )
            #ax.yaxis.set_major_formatter(dates.AutoDateFormatter(ax.xaxis.get_major_locator()))
            ax.set_ylabel("Expiration date", fontweight="heavy")
            ax.set_xlabel("Strike Price", fontweight="heavy")
            ax.set_zlabel("Naive GEX", fontweight="heavy")
        if True:
            strike_list = [[x,x] for x in tmp.strike.to_numpy()]
            naive_gex_list = [[0,x] for x in tmp.naive_gex.to_numpy()]
            for x,y in zip(naive_gex_list,strike_list):
                if x[-1] > 0:
                    color = 'green'
                else:
                    color = 'red'
                plt.plot(x,y,color=color)
        plt.axhline(spot_price,color='blue')
        plt.grid(True)
        plt.ylabel("strike")
        plt.xlabel("net naive gex ($Bn/%Move)")
        plt.title(f"ticker: {ticker} price {spot_price:1.2f}\n{tstamp}")
        plt.ylim(5500,6500)
        plt.xlim(-3,3)
        plt.show()
        plt.savefig(png_file)
        plt.close()
        png_file_list.append(png_file)

    return png_file_list

def main():

    ticker = 'SPX'
    day_stamp = datetime.date(2025,1,7)
    png_file_list = sorted([str(x) for x in pathlib.Path("tmp/pngs").rglob("*.png")])
    if len(png_file_list) == 0:
        png_file_list = plot_iv(ticker,day_stamp)
    else:
        print(len(png_file_list))

    
    gif_file = os.path.join(work_dir,f'ani.gif')
    mp4_file = os.path.join(work_dir,f"ani.mp4")   

    fps = 24
    clips = [ImageClip(m).with_duration(0.1) for m in png_file_list]
    concat_clip = concatenate_videoclips(clips, method="compose")
    concat_clip.write_videofile(mp4_file, fps=fps)
    print(os.path.exists(mp4_file))
    clip=VideoFileClip(mp4_file)
    clip.write_gif(gif_file)


if __name__ == "__main__":
    main()

"""

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-554bc784bf-xrgkg 5432:5432

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres


"""
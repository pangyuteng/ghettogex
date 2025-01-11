
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

sys.path.append("../flask")
from utils.postgres_utils import postgres_execute

def plot_iv(ticker,day_stamp):

    png_file_list = []

    query_str = """
    select * from gex_strike where ticker = %s and tstamp::date = %s
    order by tstamp
    """
    query_args = (ticker,day_stamp)
    fetched = postgres_execute(query_str,query_args)

    df = pd.DataFrame(fetched)
    df.naive_gex = df.naive_gex/1e9
    print(df.shape)
    print(df.columns)
    for tstamp in tqdm(sorted(df.tstamp.unique())):
        tmp = df[df.tstamp==tstamp].reset_index()
        png_file = os.path.join("tmp","pngs",f"gex-{ticker}-{tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.png")
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
                plt.plot(x,y)
        
        plt.xlabel("strike")
        plt.ylabel("net naive gex ($Bn/%Move)")
        plt.title(f"ticker: {ticker}")
        plt.xlim(5500,6500)
        plt.ylim(-3,3)
        plt.show()
        plt.savefig(png_file)
        plt.close()
        png_file_list.append(png_file)

    return png_file_list

def main():

    ticker = 'SPX'
    day_stamp = datetime.date(2025,1,7)
    png_file_list = plot_iv(ticker,day_stamp)

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

"""

kubectl port-forward --address 0.0.0.0 fi-postgres-deployment-554bc784bf-xrgkg 5432:5432

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres


"""
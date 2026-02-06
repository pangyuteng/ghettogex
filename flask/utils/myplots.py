import os
import pytz
import asyncio
import datetime
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from .misc import get_market_open_close, is_market_open
from .postgres_utils import apostgres_execute

async def generate_volume_plot(workdir):

    tstamp = datetime.datetime.now()
    strike_volume_png_file = os.path.join(workdir,f"strike-volume-{tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.png")
    total_volume_png_file = os.path.join(workdir,f"total-volume-{tstamp.strftime('%Y-%m-%d-%H-%M-%S')}.png")

    offset = 100
    ticker = "SPX"
    ticker_alt = "SPXW"
    day_stamp = tstamp.strftime("%Y-%m-%d")

    market_open,market_close = get_market_open_close(day_stamp,no_tzinfo=True)
    expiration = market_open.strftime("%Y-%m-%d")

    fetched = await apostgres_execute(
        None,
        "select * from candle_1min where event_symbol= %s and tstamp::date = %s",
        (ticker,expiration,)
    )
    sdf = pd.DataFrame([dict(x) for x in fetched])

    fetched = await apostgres_execute(
        None,
        "select * from candle_1min where ticker = %s and expiration = %s and tstamp::date = %s",
        (ticker_alt,expiration,expiration)
    )
    cdf = pd.DataFrame([dict(x) for x in fetched])
    if len(cdf) == 0:
        return None, None

    cdf['tstamp_1min'] = cdf.tstamp.apply(lambda x: x.replace(second=0,microsecond=0))
    cdf.ask_volume = cdf.ask_volume.fillna(0)
    cdf.bid_volume = cdf.bid_volume.fillna(0)

    ndf = cdf.groupby(['tstamp_1min','strike']).agg(
        ask_volume=pd.NamedAgg(column="ask_volume", aggfunc="sum"),
        bid_volume=pd.NamedAgg(column="bid_volume", aggfunc="sum"),
    ).reset_index()
    ndf['volume'] = ndf.ask_volume+ndf.bid_volume
    hue_norm = (0,ndf.volume.max()*0.25)

    plt.style.use('dark_background')
    palette = 'rocket'
    norm = plt.Normalize(*hue_norm)
    sm = plt.cm.ScalarMappable(cmap=palette, norm=norm)
    sm.set_array([])
    fig, ax = plt.subplots(figsize=(5,4))

    sns.scatterplot(ndf,x='tstamp_1min',y='strike',
                    hue='volume',size=1,palette=palette,legend=False,alpha=1,hue_norm=hue_norm,edgecolor=None,ax=ax)
    sns.lineplot(sdf,x='tstamp',y='close',color='w',ax=ax)

    plt.grid(True)
    ax.figure.colorbar(sm, ax=ax)
    plt.xlabel("tstamp utc")
    plt.title(f"{ticker} option volume")
    et_tz = "America/New_york"
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S',tz=pytz.timezone(et_tz)))
    ax.xaxis.set_tick_params(rotation=40)
    plt.xlim(market_open,market_close)
    plt.ylim(sdf.close.min()-offset,sdf.close.max()+offset)

    plt.xlabel("tstamp(et)\n `x`: 1min volume > 1000")
    high_volume = ndf[ndf.volume > 1000]
    plt.scatter(high_volume.tstamp_1min,high_volume.strike,s=10,marker='x',c=None,facecolor='w',linewidth=0.5,alpha=1)

    tdf = ndf.groupby(['tstamp_1min',]).agg(
        volume=pd.NamedAgg(column="volume", aggfunc="sum"),
    ).reset_index()
    plt.tight_layout()
    plt.show()
    plt.savefig(strike_volume_png_file)
    plt.close()
    
    fig, ax = plt.subplots(figsize=(4.5,2))
    ax.plot(tdf.tstamp_1min,tdf.volume,color='red')
    xfmt = mdates.DateFormatter('%H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)
    ax.xaxis.set_tick_params(rotation=40)
    plt.grid(True)
    ax.set_title("1-min volume across all contracts")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S',tz=pytz.timezone(et_tz)))
    ax.xaxis.set_tick_params(rotation=40)
    plt.tight_layout()
    plt.show()
    plt.savefig(total_volume_png_file)

    return strike_volume_png_file, total_volume_png_file


if __name__ == "__main__":
    try:
        asyncio.run(generate_volume_plot("."))
    except (KeyboardInterrupt, SystemExit):
        logger.info("stopped by user")


import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    spx_df = pd.read_csv("SPX_dailyOHLC.csv")
    # tradeday_ID,Symbol,Open,High,Low,Close,Date,Weekday
    spx_df['tstamp'] = spx_df.Date.apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d"))
    spx_df = spx_df.rename(columns={"Open":"spx_open","High":"spx_high","Low":"spx_low","Close":"spx_close"})
    cols = ['tstamp','spx_open','spx_high','spx_low','spx_close']
    spx_df = spx_df[cols]
    print(spx_df.shape)

    # DATE,OPEN,HIGH,LOW,CLOSE
    # 01/02/1990
    vix_df = pd.read_csv("VIX_dailyOHLC.csv")
    vix_df['tstamp'] = vix_df.Date.apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d"))
    vix_df = vix_df.rename(columns={"Open":"vix_open","High":"vix_high","Low":"vix_low","Close":"vix_close"})
    cols = ['tstamp','vix_high','vix_open','vix_low','vix_close']
    vix_df = vix_df[cols]
    print(vix_df.shape)

    df = vix_df.merge(spx_df,how='left',on=['tstamp'])
    df = df.dropna()
    min_tstamp = df.tstamp.min().strftime('%Y-%m-%d')
    max_tstamp = df.tstamp.max().strftime('%Y-%m-%d')
    print(df.head())
    print(df.shape)

    df['prct_change'] = 100*(df.spx_close-df.spx_open)/df.spx_open
    df.to_csv("history.csv",index=False)

    # https://www.tastylive.com/concepts-strategies/implied-volatility-rank-percentile
    def iv_rank(w):
        return 100* ( w.iloc[-1] - np.min(w) ) / (np.max(w)-np.min(w))

    df['iv_rank'] =df.vix_open.rolling(252).apply(iv_rank)

    df['prior_day_vix_close']=df.vix_close.shift()
    df['prior_day_vix_open']=df.vix_open.shift()
    df['prior_day_vix_prct_change']=(df.vix_open-df.prior_day_vix_open)/df.prior_day_vix_open
    print(df.prior_day_vix_prct_change.min(),df.prior_day_vix_prct_change.max())
    df = df.dropna()

    fig = plt.figure()
    ax = fig.add_subplot(2,1,1)
    # palette = sns.color_palette("bwr", as_cmap=True) # red is vol spike
    # sns.scatterplot(df,x='vix_open',y='prct_change',palette=palette,markers='+',
    #     hue='prior_day_vix_prct_change',hue_norm=(-0.5,0.5),alpha=0.5,size=1,ax=ax,legend=False)
    sns.scatterplot(df,x='vix_open',y='prct_change',alpha=0.1,size=0.2,ax=ax,markers=['+']*len(df))
    ax.set_yscale('symlog')
    plt.xlabel('vix open price')
    plt.ylabel('spx daily prct change')
    plt.title(f"n={len(df)}, {min_tstamp} to {max_tstamp}")
    plt.grid(True)

    ax = fig.add_subplot(2,1,2)
    sns.scatterplot(df,x='iv_rank',y='prct_change',alpha=0.5,size=0.2,ax=ax)
    ax.set_yscale('symlog')
    plt.xlabel('IV rank')
    plt.ylabel('spx daily prct change')
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("prct_change.png")
    plt.close()


    fig = plt.figure()
    plt.subplot(311)
    plt.plot(df.tstamp,df.spx_close)
    plt.title("SPX")
    plt.grid(True)
    plt.subplot(312)
    plt.plot(df.tstamp,df.vix_close)
    plt.title("VIX")
    plt.grid(True)
    plt.subplot(313)
    plt.plot(df.tstamp,df.iv_rank)
    plt.title("IV rank")
    plt.grid(True)
    plt.savefig("price.png")
    plt.close()


if __name__ == "__main__":
    main()

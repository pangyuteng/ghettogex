import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    # Date,Open,High,Low,Close,Adj Close,Volume
    # 1927-12-30,17.660000,17.660000,17.660000,17.660000,17.660000,0
    spx_df = pd.read_csv("SPX.csv")
    spx_df['tstamp'] = spx_df.Date.apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%d"))
    spx_df = spx_df.rename(columns={"Open":"spx_open","High":"spx_high","Low":"spx_low","Close":"spx_close"})
    cols = ['tstamp','spx_open','spx_high','spx_low','spx_close']
    spx_df = spx_df[cols]
    print(spx_df.shape)

    # DATE,OPEN,HIGH,LOW,CLOSE
    # 01/02/1990
    vix_df = pd.read_csv("VIX_History.csv")
    vix_df['tstamp'] = vix_df.DATE.apply(lambda x: datetime.datetime.strptime(x,"%m/%d/%Y"))
    vix_df = vix_df.rename(columns={"OPEN":"vix_open","HIGH":"vix_high","LOW":"vix_low","CLOSE":"vix_close"})
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

    # https://www.tastylive.com/concepts-strategies/implied-volatility-rank-percentile
    def iv_rank(w):
        return 100* ( w.iloc[-1] - np.min(w) ) / (np.max(w)-np.min(w))

    df['iv_rank'] =df.vix_open.rolling(252).apply(iv_rank)
    df = df.dropna()

    fig = plt.figure()
    ax = fig.add_subplot(2,1,1)
    sns.scatterplot(df,x='vix_open',y='prct_change',alpha=0.5,size=0.2,ax=ax)
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

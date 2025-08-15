import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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
print(df.head())
print(df.shape)

df['prct_change'] = 100*(df.spx_close-df.spx_open)/df.spx_open

fig = plt.figure()
ax = fig.add_subplot(1,1,1)
sns.scatterplot(df,x='vix_open',y='prct_change',alpha=0.5,ax=ax)
#ax.set_yscale('symlog')
plt.xlabel('vix open price')
plt.ylabel('spx daily prct change')
plt.plot([12.5,12.5],[-0.41,0.45])
plt.plot([17.5,17.5],[-0.66,0.66])
plt.plot([22.5,22.5],[-0.99,0.92])
plt.plot([27.5,27.5],[-1.19,1.10])
plt.plot([32.5,32.5],[-1.19,1.10])
plt.plot([37.5,37.5],[-1.56,1.62])

plt.title(f"n={len(df)}, {df.iloc[0,:].tstamp} to {df.iloc[-1,:].tstamp}")
plt.grid(True)
plt.savefig("prct_change.png")
plt.close()

fig = plt.figure()
plt.subplot(211)
plt.plot(df.tstamp,df.spx_close)
plt.subplot(212)
plt.plot(df.tstamp,df.vix_close)
plt.grid(True)
plt.savefig("price.png")
plt.close()


import pandas as pd

pq_file = "/mnt/hd1/data/uw-options-cache/SPY/2025-04-17.parquet.gzip"
df = pd.read_parquet(pq_file)
print(pq_file)
print(len(df.tstamp_sec.unique()),df.tstamp_sec.min(),df.tstamp_sec.max())

pq_file = "/mnt/hd1/data/uw-options-cache/SPX/2025-04-17.parquet.gzip"
df = pd.read_parquet(pq_file)

# typically trading hr
# ET: 9:30 to 16:00
# UTC: 13:30 to 20:00
# totals to 23400 seconds for full trading day 6.5*60*60 
print(pq_file)
print(len(df.tstamp_sec.unique()),df.tstamp_sec.min(),df.tstamp_sec.max())


# get list of contract relevant contract.
# compute ddoi - dealer directional open interest.
# for each second
#  get estimated underlying price
#  (from uw, use percentage change from SPY, alternatively compute from theoretical)
#  get oi per contract.
#  get gamm (from uw, alternatively compute from theoretical)
#  gex per strike with gamma from 
# 
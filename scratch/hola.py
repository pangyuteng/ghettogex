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

"""
'executed_at', 'underlying_symbol', 'option_chain_id', 'side', 'strike',
'option_type', 'expiry', 'underlying_price', 'nbbo_bid', 'nbbo_ask',
'ewma_nbbo_bid', 'ewma_nbbo_ask', 'price', 'size', 'premium', 'volume',
'open_interest', 'implied_volatility', 'delta', 'theta', 'gamma',
'vega', 'rho', 'theo', 'sector', 'exchange', 'report_flags', 'canceled',
'upstream_condition_detail', 'equity_type', 'tstamp', 'tstamp_sec'
"""
# df.option_chain_id
# df.underlying_symbol
# df.expiry # %Y-%m-%d
# df.option_type # put call
# df.strike



# get list of contract relevant contract.
# compute ddoi - dealer directional open interest.
# for each second
#  get estimated underlying price
#  (from uw, use percentage change from SPY, alternatively compute from theoretical)
#  get oi per contract.
#  get gamm (from uw, alternatively compute from theoretical)
#  gex per strike with gamma from 
# 
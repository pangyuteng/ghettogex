quote_query_str = """
select distinct 'quote' as event_type,event_symbol,ticker,expiration,contract_type,strike,
    last(ask_price,tstamp) as ask_price,last(bid_price,tstamp) as bid_price,last(tstamp,tstamp) as tstamp
FROM quote WHERE
tstamp <= %s
AND tstamp > %s - interval '180 second'
AND ticker = %s AND expiration = %s 
GROUP BY event_symbol,contract_type,ticker,strike,expiration
"""
query_args = (utc_tstamp,utc_tstamp,ticker_alt,expiration) # quote
oq = cpostgres_execute(aconn,quote_query_str,query_args)

all_groups = await asyncio.gather(uv,uc,oc,os,og,ot,oq)
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=FutureWarning)
    pd_list = [pd.DataFrame(x,columns=columns) for x in all_groups if x is not None]
    df = pd.concat(pd_list,ignore_index=True)


# time_till_exp ####################################
merged_df['time_till_exp'] = np.nan
try:
    expiration_mapper = {x:get_expiry_tstamp(x.strftime("%Y-%m-%d")) for x in list(merged_df.expiration.unique())}
    merged_df['time_till_exp'] = merged_df.expiration.apply(lambda x: (expiration_mapper[x]-tstamp).total_seconds()/TOTAL_SECONDS_ONE_YEAR )
    epsilon = 1e-5
    merged_df.loc[merged_df.time_till_exp==0,'time_till_exp'] = epsilon
except:
    logger.error(traceback.format_exc())
    raise ValueError()


# delta gamma charm vanna exposure ####################################
# greeks ####################################
try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        compute_greeks(merged_df,spot_price,spot_volatility,price_column='price')
    if not first_minute:
        #merged_df['volatility'] = merged_df['bsm_iv'] disable for now, too volatile.
        merged_df['delta'] = merged_df['bsm_delta']
        merged_df['gamma'] = merged_df['bsm_gamma']
except:
    logger.error(traceback.format_exc())

# delta gamma charm vanna exposure ####################################
try:

    merged_df['convexity'] = 0.0
    merged_df['state_gex'] = 0.0
    merged_df['dex'] = 0.0
    merged_df['vex'] = 0.0
    merged_df['cex'] = 0.0
    compute_exposure(merged_df,spot_price,spot_volatility)

except:
    logger.error(traceback.format_exc())
    raise ValueError()

merged_df.volume_gex = merged_df.volume_gex.fillna(value=0)
merged_df.state_gex = merged_df.state_gex.fillna(value=0)
merged_df.convexity = merged_df.convexity.fillna(value=0)
merged_df.dex = merged_df.dex.fillna(value=0)
merged_df.vex = merged_df.vex.fillna(value=0)
merged_df.cex = merged_df.cex.fillna(value=0)


############# queries
query_str = "INSERT INTO gex_net (ticker,tstamp) VALUES (%s,%s) ON CONFLICT DO NOTHING;"
query_args = (ticker,utc_tstamp)
await cpostgres_execute(aconn,query_str,query_args,is_commit=True)

gex_net_query_str = """
    COPY gex_net (ticker,tstamp,volume_gex,state_gex,spot_price,dex,convexity,vex,cex,
        call_convexity,call_oi,call_dex,call_gex,call_vex,call_cex,
        put_convexity,put_oi,put_dex,put_gex,put_vex,put_cex) FROM STDIN
"""
async def insert_gex_net(row):
    query_args = [row.ticker,row.tstamp,row.volume_gex,row.state_gex,row.spot_price,row.dex,row.convexity,row.vex,row.cex,
    row.call_convexity,row.call_oi,row.call_dex,row.call_gex,row.call_vex,row.call_cex,
    row.put_convexity,row.put_oi,row.put_dex,row.put_gex,row.put_vex,row.put_cex,
    ]
    return query_args

query_dict[gex_net_query_str] = await asyncio.gather(*(insert_gex_net(row) for n,row in net_gex_df.iterrows()))

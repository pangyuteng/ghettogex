
create index candle_tstamp_event_symbol_index on candle using brin (tstamp,event_symbol) WITH (timescaledb.transaction_per_chunk);
create index quote_tstamp_event_symbol_index on quote using brin (tstamp,event_symbol) WITH (timescaledb.transaction_per_chunk);

create index candle_tstamp_ticker_index on candle using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index event_tstamp_ticker_index on event using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index greeks_tstamp_ticker_index on greeks using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index profile_tstamp_ticker_index on profile using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index quote_tstamp_ticker_index on quote using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index summary_tstamp_ticker_index on summary using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index theoprice_tstamp_ticker_index on theoprice using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index timeandsale_tstamp_ticker_index on timeandsale using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index trade_tstamp_ticker_index on trade using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index underlying_tstamp_ticker_index on underlying using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);

create index event_contract_tstamp_ticker_index on event_contract using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index event_strike_tstamp_ticker_index on event_strike using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);
create index event_underlying_tstamp_ticker_index on event_underlying using brin (tstamp,ticker) WITH (timescaledb.transaction_per_chunk);

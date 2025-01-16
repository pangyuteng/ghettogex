
CREATE TRIGGER candle_trigger BEFORE insert or update on candle FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER event_trigger BEFORE insert or update on event FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER greeks_trigger BEFORE insert or update on greeks FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER quote_trigger BEFORE insert or update on quote FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER summary_trigger BEFORE insert or update on summary FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER theoprice_trigger BEFORE insert or update on theoprice FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER timeandsale_trigger BEFORE insert or update on timeandsale FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER trade_trigger BEFORE insert or update on trade FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER underlying_trigger BEFORE insert or update on underlying FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER watchlist_trigger BEFORE insert or update on watchlist FOR EACH ROW execute procedure notify_trigger();





create index candle_tstamp_index on candle using brin (tstamp);
create index event_tstamp_index on event using brin (tstamp);
create index greeks_tstamp_index on greeks using brin (tstamp);
create index profile_tstamp_index on profile using brin (tstamp);
create index quote_tstamp_index on quote using brin (tstamp);
create index summary_tstamp_index on summary using brin (tstamp);
create index theoprice_tstamp_index on theoprice using brin (tstamp);
create index timeandsale_tstamp_index on timeandsale using brin (tstamp);
create index trade_tstamp_index on trade using brin (tstamp);
create index underlying_tstamp_index on underlying using brin (tstamp);

create index gex_strike_tstamp_index on gex_strike using brin (tstamp);
create index gex_net_tstamp_index on gex_net using brin (tstamp);

create index event_agg_dstamp_index on event_agg using brin (dstamp);
create index event_agg_event_symbol_index on event_agg using brin (event_symbol);

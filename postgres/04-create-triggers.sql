
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



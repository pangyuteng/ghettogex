

-- Function: notify_trigger()

-- DROP FUNCTION notify_trigger();

CREATE OR REPLACE FUNCTION notify_trigger()
    RETURNS trigger AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pg_notify(TG_TABLE_NAME, '{"new":{},"old":'||row_to_json(OLD)::text||'}');
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        PERFORM pg_notify(TG_TABLE_NAME, '{"new":'||row_to_json(NEW)::text||',"old":'||row_to_json(OLD)::text||'}');
        RETURN NEW;        
    ELSIF TG_OP = 'INSERT' THEN
        PERFORM pg_notify(TG_TABLE_NAME, '{"new":'||row_to_json(NEW)::text||',"old":{}}');
        RETURN NEW;            
    END IF;

END;
$BODY$
    LANGUAGE plpgsql VOLATILE
    COST 100;
ALTER FUNCTION notify_trigger()
    OWNER TO postgres;


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



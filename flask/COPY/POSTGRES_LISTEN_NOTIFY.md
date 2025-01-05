


#### Database updates: Creating notify functions

```
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

```



```
CREATE TRIGGER greeks_trigger BEFORE insert or update on greeks FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER tradeandsale_trigger BEFORE insert or update on tradeandsale FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER quote_trigger BEFORE insert or update on quote FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER summary_trigger BEFORE insert or update on summary FOR EACH ROW execute procedure notify_trigger();
CREATE TRIGGER summary_trigger BEFORE insert or update on summary FOR EACH ROW execute procedure notify_trigger();
```



--- OLD 

* create notify_trigger
	```
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
	  OWNER TO polling;
	```

* setup notify_trigger for below tables.
	```
	CREATE TRIGGER greeks_trigger BEFORE insert or update or delete on greeks FOR EACH ROW execute procedure notify_trigger();
	```
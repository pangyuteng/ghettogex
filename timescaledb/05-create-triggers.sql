
CREATE TRIGGER watchlist_trigger BEFORE insert or update on watchlist FOR EACH ROW execute procedure notify_trigger();


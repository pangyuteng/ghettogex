
ALTER SYSTEM SET max_connections TO '2000';
ALTER SYSTEM SET shared_buffers TO '72MB';
SELECT pg_reload_conf();

INSERT INTO settings (from_scratch) VALUES (NULL);
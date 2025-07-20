

https://stackoverflow.com/questions/582336/how-do-i-profile-a-python-script

https://github.com/psycopg/psycopg/issues/1101


python -m cProfile -o program.prof pg_profiling.py ok

docker build -t snakeviz:latest .

docker run -it -p 5000:5000 -w /workdir -v $PWD:/workdir snakeviz:latest \
    bash -c "snakeviz -p 5000 --hostname="*" --server program.prof"

http://192.168.68.143:5000/snakeviz/


https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query


replaced upsert with copy
f3258109359d8ec79adf0b5ac6fcb19f9e69ee0c...0a566ca868409cca0599b58b6558fa0a430d8b98

delete from event_agg where tstamp::date = '2025-07-18';
delete from gex_strike where tstamp::date = '2025-07-18';
delete from gex_net where tstamp::date = '2025-07-18';


execute may be faster than executemany 
but 2000 rows of insert still takes .4 to 0.6 sec
with copy copy_rows, you get down to .05 seconds.
at the expense of not being able to use upsert* 

* INSERT INTO gex_strike (xxx) VALUES (xxx) ON CONFLICT (xx) DO UPDATE set x = x



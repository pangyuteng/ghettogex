

memory leak observed after postgres switched to timescaledb
https://github.com/pangyuteng/ghettogex.aigonewrong.com/compare/579038a...c02081f

several changes were made including lib version update were made.


initial suspected psycopg2.
maybe memory leak a psycopg2 issue
unable to replicate memory leak due
with psycogp2
and timescaledb 
in this folder - create.py insert.py 


revert psycopg2
2d704494575edb62dcd4e3c875af9ebc97606757


update celery version
ee291bcd17fa836f65781bfff246ae5d74c26f3c

so now we are left with tastytrade version update.
downgrade from tastytrade==10.2.3
to tastytrade==9.9
e38f28896013c9f1b68f03f36e23fa6e7ce4fd3e

--

in `insert.py` now using 
both apostgres_execute_many and apostgres_execute
seems to be observing leaks during `insert.py`...

nevermind...
fluctuatin around 2.82G and 3.1G, 2.89G
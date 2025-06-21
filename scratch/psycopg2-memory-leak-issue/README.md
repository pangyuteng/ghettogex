

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

*** but... we need 9.11 for refresh_interval of 0.5 !!! ***
https://github.com/tastyware/tastytrade/releases/tag/v9.11
maybe we always had a memory leak it just never ate up 20GB of ram....

another reason to get tastytrade>=10.2.2
https://github.com/tastyware/tastytrade/issues/253

remember to change to 10.2.3 behavior `equity = await Equity.a_get(session, ticker)`

--

maybe related??
https://github.com/sqlalchemy/sqlalchemy/discussions/10270

nice gc memory print example 
https://stackoverflow.com/questions/70214871/is-there-a-memory-leak-or-do-i-not-understand-garbage-collection-and-memory-mana

--

in `insert.py` now disabled apostgres_execute_many 
using only apostgres_execute
seems to be observing leaks during `insert.py`???

psycopg[binary,pool]==3.2.3

https://www.psycopg.org/psycopg3/docs/news_pool.html
https://www.psycopg.org/psycopg3/docs/advanced/pool.html

in 3.2.3 NO GOD DAMN LEAKS!!!

--
now we try it with celery

kubectl port-forward --address 0.0.0.0 svc/redis -n gg 6379:6379
kubectl port-forward --address 0.0.0.0 svc/rabbitmq -n gg 5672:5672


ssh hawktuah
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:latest-pg17

docker run -it \
-e CACHE_FOLDER="/mnt/hd1/data/fi" -e CACHE_TASTY_FOLDER="/mnt/hd1/data/tastyfi" \
-e POSTGRES_URI="postgres://postgres:password@192.168.68.156:5432/postgres" \
-e AMQP_URI="amqp://192.168.68.143:5672" \
-e REDIS_URI="redis://192.168.68.143:6379/1" \
-e C_FORCE_ROOT=true \
-w $PWD -v /mnt:/mnt  fi-notebook:latest bash


lib version history

https://github.com/pangyuteng/ghettogex.aigonewrong.com/commits/main/flask/requirements.txt

### recollection of what triggered memory leak

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

*** but... we need 9.11+ for refresh_interval !!! ***

https://github.com/tastyware/tastytrade/discussions/214
https://github.com/tastyware/tastytrade/discussions/223

https://github.com/tastyware/tastytrade/releases/tag/v9.11
maybe we always had a memory leak it just never ate up 20GB of ram....

another reason to get tastytrade>=10.2.2
https://github.com/tastyware/tastytrade/issues/253

remember to change to 10.2.3 behavior `equity = await Equity.a_get(session, ticker)`

--

### some links

maybe related??
https://github.com/sqlalchemy/sqlalchemy/discussions/10270

nice gc memory print example 
https://stackoverflow.com/questions/70214871/is-there-a-memory-leak-or-do-i-not-understand-garbage-collection-and-memory-mana

--

### attempting to recreate memory leak

memory likely increased with psycopg[binary,pool]==3.2.9 have memory leak ???

in `insert.py` now disabled apostgres_execute_many 
using only apostgres_execute
seems to be observing leaks during `insert.py`???

--

psycopg downgraded to psycopg[binary,pool]==3.2.3

https://www.psycopg.org/psycopg3/docs/news_pool.html
https://www.psycopg.org/psycopg3/docs/advanced/pool.html

in 3.2.3 NO GOD DAMN LEAKS!!!

--

now we try it with celery
see COMMANDS.md

no leaks observed with celery + psycopg3

psycopg[binary,pool]==3.2.3
celery==5.5.3


--
TODO:

[x]  old NOTE if above setup still have memory leak, downgrade tastytrtade to confirm 
    increased event was caused the trigger for memory leak
    but likley not the root cause.

    tastytrade==9.9 have memory leak in prod
    
    2hr about 5GB RAM increase.

+ tasty 9.9 have oom
+ tasty 10.2.3 also have oom 

+ [ ] locating memory leak with tracemalloc

https://docs.python.org/3/library/tracemalloc.html
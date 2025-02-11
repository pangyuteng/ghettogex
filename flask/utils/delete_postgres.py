import sys
from .postgres_utils import (
    postgres_execute, postgres_uri,
)


if __name__ == "__main__":
    daystamp = sys.argv[1]

    table_list = 'timeandsale,summary,quote,greeks,event,candle,profile,theoprice,trade,underlying'.split(",")
    for tablestr in table_list:
        query_str = "delete from "+tablestr+" where tstamp < %s"
        query_args = (daystamp,)
        postgres_execute(query_str,query_args,is_commit=True)



"""

python -m utils.delete_postgres 2025-02-11

"""
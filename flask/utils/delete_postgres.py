import sys
from .postgres_utils import (
    postgres_execute, postgres_uri,
)

daystamp = sys.argv[1]
# query_str = "delete from candle tstamp < %s"
# query_args = (daystamp,)
# postgres_execute(query_str,query_args,is_commit=True)

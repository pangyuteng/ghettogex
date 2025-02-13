import sys
from .postgres_utils import (
    postgres_execute, postgres_uri,
)


if __name__ == "__main__":
    daystamp = sys.argv[1]
    table_kind = sys.argv[2]
    
    events_list = 'timeandsale,summary,quote,greeks,event,candle,profile,theoprice,trade,underlying'.split(",")
    gex_list = 'event_agg,gex_strike,gex_net'.split(",")
    table_list = []
    if table_kind == "all":
        table_list.extend(events_list)
        table_list.extend(gex_list)
    elif table_kind == "events":
        table_list.extend(events_list)
    elif table_kind == "gex":
        table_list.extend(gex_list)
    else:
        raise NotImplementedError()
    
    for tablestr in table_list:
        query_str = "delete from "+tablestr+" where tstamp < %s"
        query_args = (daystamp,)
        postgres_execute(query_str,query_args,is_commit=True)



"""

python -m utils.delete_postgres 2025-02-14 gex

"""
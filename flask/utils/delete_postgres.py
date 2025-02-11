from .postgres_utils import (
    postgres_execute, postgres_uri,
)

# query_str = "delete from  tstamp < %s"
# query_args = (self,ticker,self.tstamp)
# postgres_execute(query_str,query_args,is_commit=True)
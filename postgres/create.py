
from jinja2 import Environment
from tastytrade import dxfeed
from decimal import Decimal
from typing import Optional


CREATE_TEMPLATE = """
{% for table in table_list %}
CREATE TABLE IF NOT EXISTS {{ table.table_name }} (
    {% for col_str in table.col_list %}
    {{ col_str }}{% endfor %}
    tstamp TIMESTAMP default (now() at time zone 'utc')
);

{% endfor %}

"""

type_mapper = {
    Decimal: "double precision",
    int: "numeric",
    str: "text",
    bool: "boolean",
    type(None): "text",
    Optional[Decimal]: "double precision",
    Optional[int]: "numeric",
    Optional[str]: "text",
    Optional[bool]: "boolean",
}

table_list = []
for x in dxfeed.__all__:
    table = {}
    event_class = getattr(dxfeed,x)
    table_name = event_class.__name__
    table['table_name']=table_name.lower()
    col_list = []
    col_list.append(f"{table_name.lower()}_id SERIAL PRIMARY KEY,")
    for col_name,col_info in event_class.model_fields.items():
        col_type = type_mapper[col_info.annotation]
        if col_name == "event_symbol":
            col_list.append("event_symbol text NOT NULL,")
        else:
            col_list.append(f"{col_name} {col_type},")
    col_list.append("ticker text,")
    col_list.append("expiration TIMESTAMP,")
    col_list.append("contract_type text,")
    col_list.append("strike double precision,")
    table['col_list']=col_list
    table_list.append(table)

with open("create-events.sql","w") as f:
    content = Environment().from_string(CREATE_TEMPLATE).render(table_list=table_list)
    f.write(content)

with open("create-watchlist.sql","w") as f:
    content = """
    CREATE TABLE IF NOT EXISTS watchlist (
        watchlist_id SERIAL PRIMARY KEY, 
        ticker text UNIQUE
    );
    INSERT INTO watchlist(ticker) VALUES('SPX');
    INSERT INTO watchlist(ticker) VALUES('VIX');
    """
    f.write(content)


"""

docker run -it -u $(id -u):$(id -g) -v /mnt:/mnt pangyuteng/ghetto-gex-live:latest bash


"""
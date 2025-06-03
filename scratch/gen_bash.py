import pathlib
import datetime
import pandas as pd
from jinja2 import Environment

market_open = datetime.datetime(2024,12,1)
market_close = datetime.datetime.now()
mylist = pd.date_range(start=market_open,end=market_close,freq='d')

SH_TEMPLATE="""
{% for x in date_list %}
# python uw_gex_utils.py SPY {{x}}
# python uw_gex_utils.py QQQ {{x}}
# python uw_gex_utils.py NDX {{x}}
python uw_gex_utils.py SPX {{x}}{% endfor %}
"""
date_list = [x.strftime('%Y-%m-%d') for x in mylist]

with open('hola.sh','w') as f:
    content = Environment().from_string(SH_TEMPLATE).render(date_list=date_list)
    f.write(content)

HTML_TEMPLATE = """
<html>
<head>
</head>
<body>
{% for x in mylist %}
<br>
{{ x }}:<br>
<img src="{{ x }}"><br>
{% endfor %}
</body>
</html>
"""


png_file_list = sorted([str(x) for x in pathlib.Path("tmp").rglob("*heatmap.png")])
with open('gex-heatmap.html','w') as f:
    content = Environment().from_string(HTML_TEMPLATE).render(mylist=png_file_list)
    f.write(content)



"""

python gen_bash.py

"""

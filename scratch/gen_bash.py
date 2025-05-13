import datetime
import pandas as pd
market_open = datetime.datetime(2024,12,1)
market_close = datetime.datetime(2025,5,12)
mylist = pd.date_range(start=market_open,end=market_close,freq='d')
for item in mylist:
    tstamp = item.strftime('%Y-%m-%d')
    print(f"python uw_gex_utils.py SPX {tstamp}")


"""

python gen_bash.py > hola.sh

"""
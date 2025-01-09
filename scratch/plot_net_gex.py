

import os
import sys
import pytz
import datetime
import pandas as pd
import matplotlib.pyplot as plt
sys.path.append("/opt/fi/flask")
from utils.postgres_utils import postgres_execute


def main():

    postgres_query = "select * from gex_net order by tstamp"
    postgres_args = ()
    fetched = postgres_execute(postgres_query,postgres_args)
    df = pd.DataFrame(fetched)
    utc = pytz.timezone('US/Eastern')
    df.tstamp = df.tstamp.apply(lambda x: x.replace(tzinfo=utc).astimezone(tz="US/Eastern"))
    print(df.shape)

    postgres_query = "select * from gex_strike order by tstamp"
    postgres_args = ()
    fetched = postgres_execute(postgres_query,postgres_args)
    sdf = pd.DataFrame(fetched)
    utc = pytz.timezone('US/Eastern')
    sdf.tstamp = sdf.tstamp.apply(lambda x: x.replace(tzinfo=utc).astimezone(tz="US/Eastern"))
    print(sdf.shape)


    plt.subplot(211)
    plt.plot(df.tstamp,df.spot_price)
    plt.grid(True)
    plt.subplot(212)
    plt.plot(df.tstamp,df.naive_gex)
    plt.grid(True)

    plt.savefig('gex_net.png')

if __name__ == "__main__":
    main()



"""

export POSTGRES_URI=postgres://postgres:postgres@192.168.68.143:5432/postgres

python -m plot_net_gex.py

"""
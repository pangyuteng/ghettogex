import pytz
import datetime

def now_in_new_york():
    now_utc = datetime.datetime.now(pytz.utc)
    eastern = pytz.timezone('US/Eastern')
    now_et = now_utc.astimezone(eastern)
    return now_et
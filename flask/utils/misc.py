import pytz
import datetime
import pandas_market_calendars as mcal

nyse = mcal.get_calendar('NYSE')
def is_market_open(tstamp=None):
    if tstamp is None:
        tstamp = now_in_new_york()
    today = tstamp.strftime("%Y-%m-%d")
    early = nyse.schedule(start_date=today, end_date=today)
    if len(early) == 0:
        return False
    hour_list = [
        list(early.to_dict()['market_open'].values())[0],
        list(early.to_dict()['market_close'].values())[0]
    ]
    eastern = pytz.timezone('US/Eastern')
    logger.debug(f'{tstamp},{hour_list[0].astimezone(eastern)},{hour_list[1].astimezone(eastern)}')
    if tstamp > min(hour_list) and tstamp < max(hour_list):
        return True
    else:
        return False

def now_in_new_york():
    now_utc = datetime.datetime.now(pytz.utc)
    eastern = pytz.timezone('US/Eastern')
    now_et = now_utc.astimezone(eastern)
    return now_et
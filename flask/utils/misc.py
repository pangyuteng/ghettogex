import logging
logger = logging.getLogger(__file__)

import pytz
import datetime
import pandas_market_calendars as mcal
import bcrypt

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

def get_market_open_close(day_stamp,no_tzinfo=True):
    early = nyse.schedule(start_date=day_stamp, end_date=day_stamp)
    if len(early) == 0:
        raise LookupError("market not open today!")
    market_open = list(early.to_dict()['market_open'].values())[0]
    market_close = list(early.to_dict()['market_close'].values())[0]
    if no_tzinfo:
        return market_open.replace(tzinfo=None),market_close.replace(tzinfo=None)
    else:
        return market_open,market_close

def timedelta_from_market_open(now_tstamp_et):
    if now_tstamp_et is None:
        tstamp = now_in_new_york()
    today = now_tstamp_et.strftime("%Y-%m-%d")
    early = nyse.schedule(start_date=today, end_date=today)
    if len(early) == 0:
        raise LookupError("market not open today!")
    market_open_tstamp = list(early.to_dict()['market_open'].values())[0]
    return now_tstamp_et - market_open_tstamp, market_open_tstamp

def now_in_new_york():
    now_utc = datetime.datetime.now(pytz.utc)
    eastern = pytz.timezone('US/Eastern')
    now_et = now_utc.astimezone(eastern)
    return now_et

def get_hashed_password(plain_text_password):
    # Hash a password for the first time
    #   (Using bcrypt, the salt is saved into the hash itself)
    return bcrypt.hashpw(plain_text_password.encode('utf-8'), bcrypt.gensalt())

def check_password(plain_text_password, hashed_password_str):
    # Check hashed password. Using bcrypt, the salt is saved into the hash itself
    return bcrypt.checkpw(plain_text_password.encode('utf-8'), hashed_password_str.encode('utf-8'))

EXPECTED_HASH = os.environ.get("EXPECTED_HASH")
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
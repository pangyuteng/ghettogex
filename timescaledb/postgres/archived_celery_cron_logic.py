--
flask/utils/celeryconfig.py

   'trigger_table_partition': {
       'task': 'tasks.trigger_table_partition',
       'schedule': crontab(0, 0, day_of_month='1'), # once per month
       'options': {'queue': 'default'},
       'args': [],
   },

--
flask/utils/tasks.py

from utils.postgres_utils import manage_table_partition

@celery_app.task
def trigger_table_partition(*args,**kwargs):
    utc_tstamp = datetime.datetime.now(datetime.timezone.utc)
    manage_table_partition(utc_tstamp)

--
flask/utils/postgres_utils.py

def manage_table_partition(utc_tstamp):

    table_list = 'candle,event,greeks,profile,quote,summary,theoprice,timeandsale,trade,underlying,gex_strike,gex_net,event_agg'.split(",")

    month_stamp = f"{utc_tstamp.strftime('%Y')}-{utc_tstamp.strftime('%m')}-01"
    next_month_utc_tstamp = utc_tstamp+datetime.timedelta(weeks=5)
    next_month_stamp = f"{next_month_utc_tstamp.strftime('%Y')}-{next_month_utc_tstamp.strftime('%m')}-01"

    for table_name in table_list:
        new_part_table_name = f"{table_name}_{utc_tstamp.strftime('%Y')}_{utc_tstamp.strftime('%m')}"

        try:
            print(f"creating {new_part_table_name}")
            create_query = f"""
                CREATE TABLE {new_part_table_name} PARTITION OF {table_name}
                FOR VALUES FROM ('{month_stamp}') TO ('{next_month_stamp}');
            """
            postgres_execute(create_query,(),is_commit=True)
            print("done")
        except:
            traceback.print_exc()

    for table_name in table_list:
        old_part_table_name = f"{table_name}_{utc_tstamp.year-1}_{utc_tstamp.strftime('%m')}"
        try:
            print(f"dropping {old_part_table_name}")
            drop_query = f"""
            ALTER TABLE {table_name} DETACH PARTITION {old_part_table_name};
            DROP TABLE {old_part_table_name};
            """
            postgres_execute(drop_query,(),is_commit=True)
            print("done")
        except:
            traceback.print_exc()
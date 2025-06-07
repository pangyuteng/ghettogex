import os
from kombu import Queue
from celery.schedules import crontab

broker_url = os.environ.get("AMQP_URI","NA")
result_backend = os.environ.get("REDIS_URI","NA")

broker_transport_options = {'visibility_timeout': 86400}  # 86400 seconds = 24 hours
result_backend_transport_options = {'visibility_timeout': 86400}
task_acks_late = True
worker_prefetch_multiplier = 1
broker_heartbeat = 0
task_serializer = 'json'
result_serializer = 'json'
accept_content=['json','pickle','application/json']

timezone = 'UTC'
enable_utc = True
task_default_queue = 'default'
task_queue_max_priority = 10
task_default_priority = 5
task_queues = (
    Queue('default', routing_key='default-routing-key', queue_arguments={'x-max-priority': 10}),
    Queue('stream', routing_key='stream-routing-key', queue_arguments={'x-max-priority': 10}),
)

beat_schedule = {
    'manage_subscriptions': {
        'task': 'tasks.manage_subscriptions',
        'schedule': crontab(minute='*'),
        'relative': True,
        'options': {'queue': 'stream'},
        'args': [],
    },
   'trigger_gex_cache': {
       'task': 'tasks.trigger_gex_cache',
       'schedule': 1, # every second!
       'relative': True, # rounded to the resolution of the interval
       'options': {'queue': 'default'},
       'args': [],
   },
   'trigger_table_partition': {
       'task': 'tasks.trigger_table_partition',
       'schedule': crontab(0, 0, day_of_month='1'), # once per month
       'options': {'queue': 'default'},
       'args': [],
   },
   'trigger_vaccum_full': {
       'task': 'tasks.trigger_vaccum_full',
       'schedule': crontab(minute=0, hour=0, day_of_week='sat'), # once on saturday midnight
       'options': {'queue': 'default'},
       'args': [],
   },
   'trigger_cache_cboe': {
       'task': 'tasks.trigger_cache_cboe',
       'schedule': crontab(minute=1, hour=0), # utc midnight, ~7pm et
       'options': {'queue': 'default'},
       'args': [],
   },
}

"""
   'trigger_shutdown': {
       'task': 'tasks.trigger_shutdown',
       'schedule': crontab(minute=1, hour=13), # utc midnight, ~7pm et
       'options': {'queue': 'default'},
       'args': [],
   },
"""
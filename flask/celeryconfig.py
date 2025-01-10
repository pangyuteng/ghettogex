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

timezone = 'US/Pacific'
enable_utc = True
task_default_queue = 'default'
task_queue_max_priority = 10
task_default_priority = 5
task_queues = (
    Queue('default', routing_key='default-routing-key', queue_arguments={'x-max-priority': 10}),
)

beat_schedule = {
    'manage_subscriptions': {
        'task': 'tasks.manage_subscriptions',
        'schedule': crontab(minute='*'),
        'relative': True,
        'options': {'queue': 'default'},
        'args': [],
    },
    'trigger_gex_cache': {
        'task': 'tasks.trigger_gex_cache',
        'schedule': 1, # every second!
        'relative': True, # rounded to the resolution of the interval
        'options': {'queue': 'default'},
        'args': [],
    },
}


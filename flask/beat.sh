#!/bin/bash

export workername=worker-$(openssl rand -hex 5)
echo -n $workername > /tmp/workername.txt

celery -A tasks beat \
    --loglevel INFO \
    --schedule /tmp/celerybeat-schedule \
    --pidfile /tmp/celeryworker.pid


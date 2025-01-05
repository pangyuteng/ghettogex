#!/bin/bash

export WORKERNAME=worker-$(openssl rand -hex 5)
export LOGFILE=/tmp/workerlog-${WORKERNAME}.txt
echo -n $WORKERNAME > /tmp/workername.txt

# celery -A tasks worker -Q default \
#     --hostname=$WORKERNAME \
#     --logfile=$LOGFILE \
#     --pidfile /tmp/celeryworker.pid \
#     --without-gossip --loglevel=INFO \
#     --concurrency=1 --pool=gevent


celery -A tasks worker -Q default \
    --hostname=$WORKERNAME \
    --logfile=$LOGFILE \
    --pidfile /tmp/celeryworker.pid \
    --pool=prefork \
    --concurrency=1 -Ofair --max-tasks-per-child=1 \
    --prefetch-multiplier=1 --without-gossip --loglevel=INFO


#--pool=prefork \
#--concurrency=1 -Ofair --max-tasks-per-child=1 \
#--prefetch-multiplier=1 --without-gossip --loglevel=INFO


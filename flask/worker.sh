#!/bin/bash
export QUEUENAME=$1
export WORKERNAME=worker-$(openssl rand -hex 5)
export LOGFILE=/tmp/workerlog-${WORKERNAME}.txt
echo -n $WORKERNAME > /tmp/workername.txt

celery -A tasks worker -Q $QUEUENAME \
    --hostname=$WORKERNAME \
    --pidfile /tmp/celeryworker.pid \
    --pool=solo \
    --concurrency=1 -Ofair --max-tasks-per-child=1 \
    --prefetch-multiplier=1 --without-gossip --loglevel=INFO


#    --logfile=$LOGFILE \
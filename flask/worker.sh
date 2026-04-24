#!/bin/bash
export QUEUENAME=$1
export WORKERNAME=worker-$(openssl rand -hex 5)
export LOGFILE=/tmp/workerlog-${WORKERNAME}.txt
echo -n $WORKERNAME > /tmp/workername.txt

celery -A tasks worker -Q $QUEUENAME \
    --hostname=$WORKERNAME \
    --pidfile /tmp/celeryworker.pid \
    --pool=prefork --concurrency=10 \
    --prefetch-multiplier=1 --loglevel=INFO \
    --without-mingle --without-gossip

# -Ofair --pool=solo --concurrency=1 --max-tasks-per-child=1 \
#    --logfile=$LOGFILE \
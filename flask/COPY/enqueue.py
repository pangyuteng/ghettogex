import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler("/tmp/enqueue.debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__file__)

import os
import sys
import traceback
import datetime
import psycopg2
import psycopg2.extensions
from psycopg2.extras import DictCursor
from select import select
import argparse
import json
import time
from . import app,celeryconfig

import celery
from tasks import task_foo,task_bar
LISTEM_TABLE_ = []

class EnqueueCore():
    """
    ref. https://gist.github.com/pangyuteng/7de87d413655f4961b57c1b99274a91e
    """
    timeout = 3 # sec for postgres
    def __init__(self, environment, db_id, cfg):
        """The dsn is passed here. This class requires the psycopg2 driver."""

        self.postgresurl = os.environ.get()
        
        if db_id == 'postgres':
            self. = self.secret_helper._result['ibisDBurl']
        else:
            raise NotImplementedError()
            
        self.environment = environment
        self.db_id = db_id
        self.cfg = cfg
        
        self.postgres_conn = None
        self._connect()
        self._listening = False
        self.is_log = True # todo logging
        self.queue_dict={}
                
    def _connect(self):
        self.postgres_conn = psycopg2.connect(self.postgresurl)
        self.postgres_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.curs = self.postgres_conn.cursor(cursor_factory=DictCursor)
        logger.info('connecting to postgres')
        
    def _listen(self):
        if self._listening:
            return 'already listening!'
        else:
            self._listening= True
            while self._listening:
                try:
                    if select([self.postgres_conn],[],[],self.timeout) != ([],[],[]):
                        self.postgres_conn.poll()
                        while self.postgres_conn.notifies:
                            notify = self.postgres_conn.notifies.pop()
                            self.gotNotify(notify)
                except:
                    traceback.print_exc()
                    raise
                    
    def addNotify(self, notify):
        """Subscribe to a PostgreSQL NOTIFY"""
        sql = "LISTEN %s" % notify
        self.curs.execute(sql)
        
    def removeNotify(self, notify):
        """Unsubscribe a PostgreSQL LISTEN"""
        sql = "UNLISTEN %s" % notify
        self.curs.execute(sql)

    def stop(self):
        """Call to stop the listen thread"""
        self._listening = False
            
    def run(self):
        self._subscribe()
        """Start listening NO THREAD, docker will take care of restart."""
        self._listen()
        
    # subscribe to table of interests
    def _subscribe(self):
        for table in LISTEM_TABLE_LIST:
            self.addNotify(table)
            logger.info('subscribing {}'.format(table))
            print('subscribing {}'.format(table))
            
    def __del__(self):
        pass
            
    def gotNotify(self, notify):
        """Called whenever a notification subscribed to by addNotify() is detected."""
        
        pid = notify.pid
        table_name = notify.channel
        payload = notify.payload

        if self.is_log:
            logger.info('pid: %r \n channel: %r \n payload: %r \n' % (notify.pid,notify.channel,notify.payload))
        try:
            payload = json.loads(json.dumps(payload, indent=4, sort_keys=True, default=str))
            ...
            ..task_... .apply_async(args=(payload,),
                queue=None,routing_key=None,
                priority=None,countdown=None
            )
        except:
            traceback.print_exc()
            logger.error(f'{traceback.format_exc()}')


if __name__ == "__main__":
    logger.addHandler("start_enqueue.py cli")
    environment = sys.argv[1]
    db_id = sys.argv[2]
    pidfile = sys.argv[3]

    with open(pidfile,'w') as f:
        f.write(str(os.getpid()))
    inst = EnqueueCore(environment,db_id,cfg)
    inst.run()

import logging
logger = logging.getLogger(__file__)

import os
import re
import sys
import ast
import time
import math
import traceback
import pytz
import datetime
import json
import asyncio
import threading
import luigi
from celery import Celery

celery_app = Celery('tasks')
import celeryconfig
celery_app.config_from_object(celeryconfig)

from insert import background_subscribe
@celery_app.task
def trigger_testing(*args,**kwargs):
    asyncio.run(background_subscribe())

if __name__ == "__main__":
    trigger_testing.apply_async()

"""



"""
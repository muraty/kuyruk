import logging
import multiprocessing
import math
import traceback

from . import loader
from .exceptions import Reject

logger = logging.getLogger(__name__)

MAX_LOAD = math.ceil(multiprocessing.cpu_count() * 4)
MAX_RUN_TIME = 60  # seconds


class Worker(object):

    RESULT_OK = 0
    RESULT_ERROR = 1
    RESULT_REJECT = 2

    def __init__(self, in_queue, out_queue):
        self.in_queue = in_queue
        self.out_queue = out_queue

    def work(self):
        tag, task_description = self.in_queue.get()
        logger.info('got message: %s', task_description)

        try:
            self.process_task(task_description)
            logger.debug('Task is successful')
            self.out_queue.put((tag, Worker.RESULT_OK))
        except Reject:
            logger.info('Task is rejected')
            self.out_queue.put((tag, Worker.RESULT_REJECT))
        except Exception:
            logger.error('Task raised an exception')
            print '*' * 80
            traceback.print_exc()
            self.out_queue.put((tag, Worker.RESULT_ERROR))

    def process_task(self, task_description):
        fname, args, kwargs = (task_description['f'],
                               task_description['args'],
                               task_description['kwargs'])
        task = loader.import_task(fname)
        logger.debug(
            'Task %r will be executed with args=%r and kwargs=%r',
            task, args, kwargs)

        result = task.f(*args, **kwargs)
        logger.debug('Result: %r', result)

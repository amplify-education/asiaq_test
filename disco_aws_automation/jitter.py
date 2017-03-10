"""
This class implements the logic to run an AWS command using Backoff with Decorrelated Jitter.
The logic is based on the following article:
https://www.awsarchitectureblog.com/2015/03/backoff.html
"""
import logging
from random import randint
import time

from boto.exception import BotoServerError

retry_logger = logging.getLogger(__name__)

BASE = 3
MAX_POLL_INTERVAL = 60  # seconds


def backoff(cycle):
    """
    This function use a cycle count,
    calculates jitter and executes sleep for the calculated time.
    The minimum value 'cycle' can take is 1
    """
    # Check if we exceed the max waiting time
    new_interval = min(MAX_POLL_INTERVAL, randint(BASE, cycle * 3))
    time.sleep(new_interval)
    return new_interval


def jitter(exceptions=Exception, max_time=None, timeout_exception=None, timeout_msg=None, tries=-1,
           error_codes=None, logger=retry_logger):
    """

    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param error_codes: A list of Exception error codes
    :param max_time: the maximum cummulated time spent waiting
    :param logger: logger.exception will be called on failed attempts.
                   default: jitter.logging_logger. if None, logging is disabled.
    :return:
    """
    # pylint: disable=unused-argument
    def jitter_decorator(f):
        """
        :param f:
        :return:
        """
        def jitter_function(funct, *fargs, **fkwargs):
            """
            :param funct:
            :param fargs:
            :param fkwargs:
            :return:
            """
            _cycle = 0
            _time_passed = 0
            _tries = tries
            while _tries:
                try:
                    return funct(*fargs, **fkwargs)
                except exceptions as err:
                    if logger.level == logging.DEBUG:
                        logger.exception("Failed to run %s.", funct)

                    _tries -= 1
                    # Did we reach the max tries or the max wait time
                    if not _tries or (max_time and _time_passed > max_time):
                        if logger.level == logging.DEBUG:
                            err_msg = \
                                "Jitter reached max tries {0} for function {1}.".format(tries, funct) \
                                if _tries else \
                                "Jitter reached timeout {0} for function {1}.".format(max_time, funct)
                            logger.exception(err_msg)
                        if not timeout_exception:
                            raise
                        else:
                            raise timeout_exception(timeout_msg.format(_time_passed))

                    if error_codes:
                        if isinstance(err, BotoServerError):
                            error_code = err.error_code
                        else:
                            error_code = err.response['Error'].get('Code', 'Unknown')
                        if error_code not in error_codes:
                            raise

                    # backoff
                    _cycle += 1
                    _time_passed += backoff(_cycle)
        return jitter_function

    return jitter_decorator

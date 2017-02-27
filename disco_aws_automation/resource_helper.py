"""
This module has a bunch of functions about waiting for an AWS resource to become available
"""
import logging
import time
from random import randint

from botocore.exceptions import ClientError
from boto.exception import EC2ResponseError, BotoServerError

from .exceptions import (
    TimeoutError,
    ExpectedTimeoutError,
    S3WritingError
)

logger = logging.getLogger(__name__)

STATE_POLL_INTERVAL = 2  # seconds
INSTANCE_SSHABLE_POLL_INTERVAL = 15  # seconds
MAX_POLL_INTERVAL = 60  # seconds


def create_filters(filter_dict):
    """
    Converts a dict to a list of boto3 filters. The keys and value of the dict represent
    the Name and Values of a filter, respectively.
    """
    filters = []
    for key in filter_dict.keys():
        filters.append({'Name': key, 'Values': filter_dict[key]})

    return filters


def tag2dict(tags):
    """ Converts a list of AWS tag dicts to a single dict with corresponding keys and values """
    return {tag.get('Key'): tag.get('Value') for tag in tags or {}}


def find_or_create(find, create):
    """Given a find and a create function, create a resource if it doesn't exist"""
    result = find()
    if result:
        return result
    else:
        return create()


def keep_trying(max_time, fun, *args, **kwargs):
    """
    Execute function fun with args and kwargs until it does
    not throw exception or max time has passed.

    After each failed attempt a delay is introduced of an
    increasing number seconds following the fibonacci series
    (up to MAX_POLL_INTERVAL seconds).

    Note: If you are only concerned about throttling use throttled_call
    instead. Any irrecoverable exception within a keep_trying will
    cause a max_time delay.
    """

    jitter = Jitter(max_time)
    while True:
        try:
            return fun(*args, **kwargs)
        except Exception as exception:
            if logging.getLogger().level == logging.DEBUG:
                logger.exception("Failed to run %s.", fun)
            try:
                jitter.backoff()
            except TimeoutError:
                raise exception


def throttled_call(fun, *args, **kwargs):
    """
    Execute function fun with args and kwargs until it does
    not throw a throttled exception or 5 minutes have passed.

    After each failed attempt a delay is introduced of an
    increasing number seconds following the fibonacci series
    (up to MAX_POLL_INTERVAL seconds).
    """
    max_time = 5 * 60
    jitter = Jitter(max_time)

    while True:
        try:
            return fun(*args, **kwargs)
        except (BotoServerError, ClientError) as err:
            if logging.getLogger().level == logging.DEBUG:
                logger.exception("Failed to run %s.", fun)

            if isinstance(err, BotoServerError):
                error_code = err.error_code
            else:
                error_code = err.response['Error'].get('Code', 'Unknown')

            if error_code not in ("Throttling", "RequestLimitExceeded"):
                raise
            try:
                jitter.backoff()
            except TimeoutError:
                raise err


def wait_for_state(resource, state, timeout=15 * 60, state_attr='state'):
    """Wait for an AWS resource to reach a specified state"""
    jitter = Jitter(timeout)
    time_passed = 0
    while True:
        try:
            resource.update()
            current_state = getattr(resource, state_attr)
            if current_state == state:
                return
            elif current_state in (u'failed', u'terminated'):
                raise ExpectedTimeoutError(
                    "{0} entered state {1} after {2}s waiting for state {3}"
                    .format(resource, current_state, time_passed, state))
        except (EC2ResponseError, BotoServerError):
            pass  # These are most likely transient, we will timeout if they are not

        try:
            time_passed = jitter.backoff()
        except TimeoutError:
            raise TimeoutError(
                "Timed out waiting for {0} to change state to {1} after {2}s."
                .format(resource, state, time_passed))


def wait_for_state_boto3(describe_func, params_dict, resources_name,
                         expected_state, state_attr='state', timeout=15 * 60):
    """Wait for an AWS resource to reach a specified state using the boto3 library"""
    jitter = Jitter(timeout)
    time_passed = 0
    while True:
        try:
            resources = describe_func(**params_dict)[resources_name]
            if not isinstance(resources, list):
                resources = [resources]

            all_good = True
            failure = False
            for resource in resources:
                if resource[state_attr] != expected_state:
                    all_good = False
                elif resource[state_attr] in (u'failed', u'terminated'):
                    failure = True

            if all_good:
                return
            elif failure:
                raise ExpectedTimeoutError(
                    "At least some resources who meet the following description entered either "
                    "'failed' or 'terminated' state after {0}s waiting for state {1}:\n{2}"
                    .format(time_passed, expected_state, params_dict))
        except (EC2ResponseError, ClientError):
            pass  # These are most likely transient, we will timeout if they are not

        try:
            time_passed = jitter.backoff()
        except TimeoutError:
            raise TimeoutError(
                "Timed out waiting for resources who meet the following description to change "
                "state to {0} after {1}s:\n{2}"
                .format(expected_state, time_passed, params_dict))


def wait_for_sshable(remotecmd, instance, timeout=15 * 60, quiet=False):
    """Returns True when host is up and sshable
    returns False on timeout
    """
    jitter = Jitter(timeout)
    time_passed = 0

    if not quiet:
        logger.info("Waiting for instance %s to be fully provisioned.", instance.id)
    wait_for_state(instance, u'running', timeout)
    if not quiet:
        logger.info("Instance %s running (booting up).", instance.id)

    while True:
        logger.debug(
            "Waiting for %s to become sshable.", instance.id)
        if remotecmd(instance, ['true'], nothrow=True)[0] == 0:
            logger.info("Instance %s now SSHable.", instance.id)
            logger.debug("Waited %s seconds for instance to boot", time_passed)
            return
        try:
            time_passed = jitter.backoff()
        except TimeoutError:
            raise TimeoutError(
                "Timed out waiting for instance {0} to become sshable after {1}s."
                .format(instance, timeout))


def check_written_s3(object_name, expected_written_length, written_length):
    """Check S3 object is written by checking the bytes_written from key.set_contents_from_* method
    Raise error if any problem happens so we can diagnose the causes
    """
    if expected_written_length != written_length:
        raise S3WritingError(
            "{0} is not written correctly to S3 bucket".format(object_name)
        )


class Jitter(object):
    """
    Implement Backoff with Decorrelated Jitter based on the article:
    https://www.awsarchitectureblog.com/2015/03/backoff.html
    """
    def __init__(self, timeout):
        self.__base = 3
        self.__cycle = 0
        self.timeout = timeout
        self.time_passed = 0

    def backoff(self):
        """This function use a cycle count,
        calculates jitter and executes sleep for the calculated time.
        The minimum value 'cycle' can take is 1
        """
        # Check if we exceed the max waiting time
        if self.timeout < self.time_passed:
            raise TimeoutError("Jitter backoff timed out", self.time_passed)
        self.__cycle += 1
        new_interval = min(MAX_POLL_INTERVAL, randint(self.__base, self.__cycle * 3))
        time.sleep(new_interval)
        self.time_passed += new_interval
        return self.time_passed

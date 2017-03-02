"""
This module has a bunch of functions about waiting for an AWS resource to become available
"""
import logging
import time
from random import randint

from botocore.exceptions import ClientError
from boto.exception import BotoServerError, EC2ResponseError

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

    After each failed attempt a delay is introduced using Jitter.backoff() function.

    Note: If you are only concerned about throttling use throttled_call
    instead. Any irrecoverable exception within a keep_trying will
    cause a max_time delay.
    """

    jitter = Jitter(max_time)
    return jitter.run_with_backoff(fun, None, None, *args, **kwargs)


def throttled_call(fun, *args, **kwargs):
    """
    Execute function fun with args and kwargs until it does
    not throw a throttled exception or 5 minutes have passed.

    After each failed attempt a delay is introduced using Jitter.backoff() function.
    """
    max_time = 5 * 60
    jitter = JitterOnError(max_time)
    return jitter.run_with_backoff(fun, None, ("Throttling", "RequestLimitExceeded"), *args, **kwargs)


def wait_for_state(resource, state, timeout=15 * 60, state_attr='state'):
    """Wait for an AWS resource to reach a specified state"""

    def check_resource_state(resource, state, state_attr, start_time):
        """Check resource state using boto2"""
        resource.update()
        current_state = getattr(resource, state_attr)
        if current_state == state:
            return
        elif current_state in (u'failed', u'terminated'):
            raise ExpectedTimeoutError(
                "{0} entered state {1} after {2}s waiting for state {3}"
                .format(resource, current_state, time.time() - start_time, state))
        raise ClientError({"Error": {"Code": "internal_error"}}, "update")

    jitter = JitterOnException(timeout)
    try:
        return jitter.run_with_backoff(check_resource_state, None, None,
                                       resource=resource, state=state, state_attr=state_attr,
                                       start_time=time.time())
    except (EC2ResponseError, ClientError):
        raise TimeoutError(
            "Timed out waiting for {0} to change state to {1} after {2}s."
            .format(resource, state, jitter.time_passed))
    except Exception:
        raise


def wait_for_state_boto3(describe_func, params_dict, resources_name,
                         expected_state, state_attr='state', timeout=15 * 60):
    """Wait for an AWS resource to reach a specified state using the boto3 library"""

    def check_resource_state(describe_func, params_dict, resources_name, expected_state, state_attr,
                             start_time):
        """Check resource state using boto3"""
        resources = describe_func(**params_dict)[resources_name]
        if not isinstance(resources, list):
            resources = [resources]

        all_good = True
        failure = False
        for resource in resources:
            if resource[state_attr] in (u'failed', u'terminated'):
                failure = True
                all_good = False
            elif resource[state_attr] != expected_state:
                all_good = False

        if all_good:
            return
        elif failure:
            raise ExpectedTimeoutError(
                "At least some resources who meet the following description entered either "
                "'failed' or 'terminated' state after {0}s waiting for state {1}:\n{2}"
                .format(time.time() - start_time, expected_state, params_dict))

        raise ClientError({"Error": {"Code": "internal_error"}}, "update")

    jitter = JitterOnException(timeout)
    try:
        return jitter.run_with_backoff(check_resource_state, None, None,
                                       describe_func=describe_func, params_dict=params_dict,
                                       resources_name=resources_name, expected_state=expected_state,
                                       state_attr=state_attr, start_time=time.time())
    except (EC2ResponseError, ClientError):
        raise TimeoutError(
            "Timed out waiting for resources who meet the following description to change "
            "state to {0} after {1}s:\n{2}"
            .format(expected_state, jitter.time_passed, params_dict))
    except Exception:
        raise


def wait_for_sshable(remotecmd, instance, timeout=15 * 60, quiet=False):
    """
    Returns True when host is up and sshable
    returns False on timeout
    """
    def run_remotecmd(remotecmd, instance, start_time):
        """Run remote command"""
        if remotecmd(instance, ['true'], nothrow=True)[0] == 0:
            logger.info("Instance %s now SSHable.", instance.id)
            logger.debug("Waited %s seconds for instance to boot", time.time() - start_time)
            return
        raise ClientError({"Error": {"Code": "internal_error"}}, "remotecmd")

    if not quiet:
        logger.info("Waiting for instance %s to be fully provisioned.", instance.id)
    wait_for_state(instance, u'running', timeout)
    if not quiet:
        logger.info("Instance %s running (booting up).", instance.id)

    jitter = Jitter(timeout)
    try:
        return jitter.run_with_backoff(run_remotecmd, None, None, remotecmd=remotecmd,
                                       instance=instance, start_time=time.time())
    except Exception:
        raise TimeoutError(
            "Timed out waiting for instance {0} to become sshable after {1}s."
            .format(instance, timeout))


def check_written_s3(object_name, expected_written_length, written_length):
    """
    Check S3 object is written by checking the bytes_written from key.set_contents_from_* method
    Raise error if any problem happens so we can diagnose the causes
    """
    if expected_written_length != written_length:
        raise S3WritingError(
            "{0} is not written correctly to S3 bucket".format(object_name)
        )


class Jitter(object):
    """
    This class implements the logic to run an AWS command using Backoff with Decorrelated Jitter.
    The logic is based on the following article:
    https://www.awsarchitectureblog.com/2015/03/backoff.html
    """
    def __init__(self, timeout):
        self.time_passed = 0
        self._base = 3
        self._cycle = 0
        self._timeout = timeout

    def backoff(self):
        """
        This function use a cycle count,
        calculates jitter and executes sleep for the calculated time.
        The minimum value 'cycle' can take is 1
        """
        # Check if we exceed the max waiting time
        if self._timeout < self.time_passed:
            return self.time_passed
        self._cycle += 1
        new_interval = min(MAX_POLL_INTERVAL, randint(self._base, self._cycle * 3))
        time.sleep(new_interval)
        self.time_passed += new_interval
        return self.time_passed

    def do_backoff(self, exception, error_codes):
        """default logic deciding if backoff should executed based on the raised exception"""
        # pylint: disable=unused-argument
        return True

    def run_with_backoff(self, func, wait_msg, error_codes, *args, **kwargs):
        """
        Execute a given function with backoff
        """
        time_passed = 0
        while True:
            if wait_msg:
                logger.debug(wait_msg)
            try:
                return func(*args, **kwargs)
            except ExpectedTimeoutError:
                raise
            except Exception as err:
                if logging.getLogger().level == logging.DEBUG:
                    logger.exception("Failed to run %s.", func)

                # We need to filter on error code
                if not self.do_backoff(err, error_codes):
                    raise

                if time_passed > self._timeout:
                    raise err
                else:
                    time_passed = self.backoff()


class JitterOnError(Jitter):
    """
    This class extend the logic of the Jitter class.
    The logic of do_backoff is based on the value of the exception error code
    """
    def do_backoff(self, exception, error_codes):
        """Logic deciding if backoff should executed based on the error code of the raised exception"""

        # Check if we should backoff based on the exception error code
        if error_codes and (isinstance(exception, BotoServerError) or isinstance(exception, ClientError)):
            # or isinstance(err, EC2ResponseError):
            if isinstance(exception, BotoServerError):
                error_code = exception.error_code
            else:
                error_code = exception.response['Error'].get('Code', 'Unknown')

            if error_code not in error_codes:
                return False

        return True


class JitterOnException(Jitter):
    """
    This class extend the logic of the Jitter class.
    The logic of do_backoff is based on the type of the raised exception
    """
    def do_backoff(self, exception, error_codes):
        """Logic deciding if backoff should executed based on the type of the raised exceptions"""
        return isinstance(exception, ClientError) or isinstance(exception, EC2ResponseError)

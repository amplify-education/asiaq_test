"""
Tests for resource Helper
"""
from unittest import TestCase

from mock import patch

from disco_aws_automation.exceptions import TimeoutError

from disco_aws_automation.resource_helper import Jitter


# time.sleep is being patched but not referenced.
# pylint: disable=W0613
class ResourceHelperTests(TestCase):
    """Test Resource Helper"""
    @patch('time.sleep', return_value=None)
    def test_jitter(self, mock_sleep):
        """Test backoff """
        jitter = Jitter(60)
        is_timeout = False
        cycle = 1
        previous_time_passed = jitter.backoff()
        while not is_timeout:
            try:
                cycle += 1
                time_passed = jitter.backoff()
                wait_time = time_passed - previous_time_passed
                self.assertTrue(wait_time >= 3)
                self.assertTrue(wait_time <= cycle * 3)
                previous_time_passed = time_passed
            except TimeoutError:
                is_timeout = True

    @patch('time.sleep', return_value=None)
    def test_jitter_timeout(self, mock_sleep):
        """Test backoff timeout"""
        jitter = Jitter(60)
        time_passed = jitter.backoff()
        while time_passed < 60:
            time_passed = jitter.backoff()

        self.assertRaises(TimeoutError, jitter.backoff)

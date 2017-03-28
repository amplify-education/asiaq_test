"""
Tests Socify Helper
"""
from unittest import TestCase
from disco_aws_automation.socify_helper import SocifyHelper
from test.helpers.patch_disco_aws import get_mock_config


class SocifyHelperTest(TestCase):
    """Test Socify Helper"""
    def setUp(self):
        self._soc_helper = SocifyHelper(get_mock_config({
            'socify': {'socify_baseurl': 'https://socify.aws.wgen.net/soc'}}))

    def test_build_event_url(self):
        """Test socify build event url"""
        url = self._soc_helper._build_event_url()
        self.assertEqual(url, "https://socify.aws.wgen.net/soc/event")

    def test_build_event_json(self):
        """Test socify build event json data"""
        data = self._soc_helper._build_event_json("AL-test", "myhostclass", "test", "test was successfull")
        res_data = {"Id": "AL-test",
                    "cmd": "test",
                    "hostclass": "myhostclass",
                    "data": {"status": "test was successfull"}}
        self.assertEqual(data, res_data)

    def test_send_event(self):
        """Test send event with no error"""
        res = self._soc_helper.send_event("AL-1102", "myhostclass", "ExampleEvent", "test was successfull")
        self.assertEqual(res, '{"message": "SOCIFY has successfully processed the event: ExampleEvent"}')

    def test_send_event_invalid_ticket(self):
        """Test send event using an invalid Ticket ID"""
        with self.assertRaisesRegexp(RuntimeError, '{"errorMessage": "SOCIFY failed executing the event: '):
            self._soc_helper.send_event("AL-110266", "myhostclass", "ExampleEvent", "test was successfull")

    def test_send_event_invalid_url(self):
        """Test send event using an invalid URL"""
        self._soc_helper._socify_url = "https://socify.aws.wgen.com/soc/events"
        with self.assertRaisesRegexp(RuntimeError, 'nodename nor servname provided, or not known'):
            self._soc_helper.send_event("AL-110266", "myhostclass", "ExampleEvent", "test was successfull")

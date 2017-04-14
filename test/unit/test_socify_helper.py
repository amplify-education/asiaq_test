"""
Tests Socify Helper
"""
from unittest import TestCase
import requests
import requests_mock

from disco_aws_automation.socify_helper import SocifyHelper
from test.helpers.patch_disco_aws import get_mock_config

SOCIFY_API = 'https://socify-ci.aws.wgen.net/soc/event'


class SocifyHelperTest(TestCase):
    """Test Socify Helper"""
    def setUp(self):
        self._soc_helper = SocifyHelper("AL-1102", False, "ExampleEvent", config=get_mock_config({
            'socify': {'socify_baseurl': 'https://socify-ci.aws.wgen.net/soc'}}))

    def test_build_event_url(self):
        """Test socify build event url"""
        url = self._soc_helper._build_event_url()
        self.assertEqual(url, "https://socify-ci.aws.wgen.net/soc/event")

    def test_build_event_url_no_socify_config(self):
        """Test socify build event url when the socify section is missing from the config"""
        self._soc_helper = SocifyHelper("AL-1102", False, "ExampleEvent", config=get_mock_config({}))
        url = self._soc_helper._build_event_url()
        self.assertIsNone(url)

    def test_build_event_url_no_socify_url(self):
        """Test socify build event url when the socify section is missing from the config"""
        self._soc_helper = SocifyHelper("AL-1102", False, "ExampleEvent", config=get_mock_config({
            'socify': {'baseurl': 'https://socify-ci.aws.wgen.net/soc'}}))
        url = self._soc_helper._build_event_url()
        self.assertIsNone(url)

    def test_build_event_json(self):
        """Test socify build event json data"""
        data = self._soc_helper._build_event_json(SocifyHelper.SOC_EVENT_OK, hostclass="myhostclass",
                                                  msg="test was successfull")
        res_data = {"ticketId": "AL-1102",
                    "cmd": "ExampleEvent",
                    "data": {"status": SocifyHelper.SOC_EVENT_OK,
                             "hostclass": "myhostclass",
                             "msg": "test was successfull"}}
        self.assertEqual(data, res_data)

    def test_build_event_json_with_sub_command(self):
        """Test socify build event json data"""
        self._soc_helper._sub_command = 'mySubCommand'
        data = self._soc_helper._build_event_json(SocifyHelper.SOC_EVENT_OK, hostclass="myhostclass",
                                                  msg="test was successfull")
        res_data = {"ticketId": "AL-1102",
                    "cmd": "ExampleEvent",
                    "data": {"status": SocifyHelper.SOC_EVENT_OK,
                             "sub_cmd": "mySubCommand",
                             "hostclass": "myhostclass",
                             "msg": "test was successfull"}}
        self.assertEqual(data, res_data)

    @requests_mock.Mocker()
    def test_send_event(self, mock_requests):
        """Test send event with no error"""
        mock_response = {
            'message': 'SOCIFY has successfully processed the event: ExampleEvent'
        }
        mock_requests.post(SOCIFY_API, json=mock_response)
        self.assertEqual(self._soc_helper.send_event(SocifyHelper.SOC_EVENT_OK, msg="test was successfull"),
                         "SOCIFY has successfully processed the event: ExampleEvent")

    @requests_mock.Mocker()
    def test_send_event_timeout(self, mock_requests):
        """Test send event with no error"""
        mock_requests.post(SOCIFY_API, exc=requests.exceptions.ConnectTimeout)
        self.assertEqual(self._soc_helper.send_event(SocifyHelper.SOC_EVENT_OK, msg="test was successfull"),
                         "Failed sending the Socify event")

    @requests_mock.Mocker()
    def test_send_event_httperror(self, mock_requests):
        """Test send event with no error"""
        mock_response = {
            'errorMessage': 'SOCIFY failed executing the event request'
        }
        mock_requests.post(SOCIFY_API, json=mock_response, status_code=400)
        self.assertEqual(self._soc_helper.send_event(SocifyHelper.SOC_EVENT_OK, msg="test was successfull"),
                         "SOCIFY failed executing the event request")

    def test_send_event_no_url(self):
        """Test send event with no error"""
        self._soc_helper = SocifyHelper("AL-1102", False, "ExampleEvent", config=get_mock_config({}))
        res = self._soc_helper.send_event(SocifyHelper.SOC_EVENT_OK, msg="test was successfull")
        self.assertIsNone(res)

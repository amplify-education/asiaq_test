"""
Tests Socify Helper
"""

from unittest import TestCase
from disco_aws_automation.socify_helper import SocifyHelper
from test.helpers.patch_disco_aws import get_mock_config


class SocifyHelperTest(TestCase):
    """Test Socify Helper"""
    def setUp(self):
        self._soc_helper = SocifyHelper("AL-1102", "ExampleEvent", config=get_mock_config({
            'socify': {'socify_baseurl': 'https://socify.aws.wgen.net/soc'}}))

    def test_build_event_url(self):
        """Test socify build event url"""
        url = self._soc_helper._build_event_url()
        self.assertEqual(url, "https://socify.aws.wgen.net/soc/event")

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

    def test_send_event(self):
        """Test send event with no error"""
        res = self._soc_helper.send_event(SocifyHelper.SOC_EVENT_OK, msg="test was successfull")
        self.assertEqual(res, {"message": "SOCIFY has successfully processed the event: ExampleEvent"})

    def test_send_event_soc_bad_data(self):
        """Test send event with SOC BAD DATA"""
        res = self._soc_helper.send_event(SocifyHelper.SOC_EVENT_BAD_DATA, err_msg="No AMI")
        self.assertEqual(res, {"message": "SOCIFY has successfully processed the event: ExampleEvent"})

    def test_send_event_soc_error(self):
        """Test send event with SOC ERROR"""
        res = self._soc_helper.send_event(SocifyHelper.SOC_EVENT_OK, hostclass="myhostclass",
                                          err_msg="some Error occured")
        self.assertEqual(res, {"message": "SOCIFY has successfully processed the event: ExampleEvent"})

    def test_send_event_invalid_ticket(self):
        """Test send event using an invalid Ticket ID"""
        self._soc_helper._ticket_id = "AL-110266"
        with self.assertRaisesRegexp(RuntimeError, '{"errorMessage": "SOCIFY failed executing the event: '):
            self._soc_helper.send_event(SocifyHelper.SOC_EVENT_OK, hostclass="myhostclass",
                                        msg="test was successfull")

    def test_send_event_invalid_url(self):
        """Test send event using an invalid URL"""
        self._soc_helper._socify_url = "https://socify.aws.wgen.com/soc/events"
        with self.assertRaisesRegexp(RuntimeError, 'Failure sending event to Socify:'):
            self._soc_helper.send_event(SocifyHelper.SOC_EVENT_OK, hostclass="myhostclass")

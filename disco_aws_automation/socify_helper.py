"""
This module has utility functions for working with the socify lambda
"""
import logging
from ConfigParser import NoOptionError, NoSectionError

import requests
from .disco_config import read_config

logger = logging.getLogger(__name__)


SocifyConfig = {
    "EVENT": {
        "basePath": "/event",
        "use_data": True
    },
    "VALIDATE": {
        "basePath": "/validate",
        "use_data": False
    }
}


class SocifyHelper(object):
    """Socify helper provides the function to invoke the Socify lambda functions"""
    SOC_EVENT_OK = 100
    SOC_EVENT_BAD_DATA = 200
    SOC_EVENT_ERROR = 300

    def __init__(self, ticket_id, dry_run, command, sub_command=None, ami_id=None, config=None):
        self._ticket_id = ticket_id
        self.dry_run = dry_run
        self._command = command
        self._sub_command = sub_command
        self._ami_id = ami_id

        if config:
            self._config = config
        else:
            self._config = read_config()
        # Init the socify base url
        self._set_socify_base_url()

    def _set_socify_base_url(self):
        try:
            self._socify_url = self._config.get("socify", "socify_baseurl")
        except (NoOptionError, NoSectionError):
            logger.exception("The property socify_baseurl is not set in your disco_aws.ini file. The "
                             "deploy action won't be logged in your ticket. Please make sure to add the "
                             "definition for socify_baseurl in the [socify] section.")
            raise RuntimeError("Socify_Helper: The property socify_baseurl is not set")

    def _build_url(self, function_name):
        """
        Build the socify url for the specified function name
        :param function_name: The Socify function name which will be invoked
        :return: The socify URL associated to the Function
        """
        return self._socify_url + SocifyConfig[function_name]["basePath"]

    def _build_json_data(self, status, **kwargs):
        """
        generate the Socify json data object
        :param status: The status of the executed command that we are going to log
        :param kwargs:  additional named arguments used to populate the data section of the json
        :return: a dictionary containing the socify event data
        """
        event_info = {'status': status}
        if self._sub_command:
            event_info['sub_cmd'] = self._sub_command
        event_info.update(kwargs)
        return event_info

    def _build_json(self, function_name, status=None, **kwargs):
        """
        Build the event JSON for the Socify Event associated to the executed command
        :param function_name: The Socify function name which will be invoked
        :param status: The status of the executed command that we are going to log
        :param kwargs:  additional named arguments used to populate the data section of the json
        :return: The Event JSON for the associated Event
        """
        event_json = {"ticketId": self._ticket_id,
                      "cmd": self._command,
                      "ami_id": self._ami_id}

        # Add data section if required
        if SocifyConfig[function_name]["use_data"]:
            event_json["data"] = self._build_json_data(status, **kwargs)

        return event_json

    def _can_invoke_socify(self):
        """
        Verify if we should invoke socify and that all the required parameters are available
        :return: True if all parameter are valid otherwise False
        """
        return self._ticket_id and self._socify_url and not self.dry_run

    def _call_function(self, function_name, status=None, **kwargs):
        """
        helper function used to run the socify function
        :param function_name: The Socify function name which will be invoked
        :param status: The status of the executed command that we are going to log
        :param kwargs:  additional named arguments used to populate the data section of the json
        """
        if not self._can_invoke_socify():
            return

        url = self._build_url(function_name)

        data = self._build_json(function_name, status, **kwargs)
        result = None
        try:
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url=url, headers=headers, json=data)
            response.raise_for_status()
            status = response.status_code
            rsp_msg = response.json()['message']
            result = response.json().get("result")
            if result:
                logger.info("received response status %s result: %s data: %s", status, result, rsp_msg)
            else:
                logger.info("received response status %s data: %s", status, rsp_msg)
        except requests.HTTPError:
            rsp_msg = response.json()['errorMessage']
            logger.error("Socify event failed with the following error: %s", rsp_msg)
        except Exception:
            logger.exception("Failed to send event to Socify")
            rsp_msg = 'Failed sending the Socify event'

        return result if result else rsp_msg

    def send_event(self, status, **kwargs):
        """
        helper function used to send a socify event
        :param status: The status of the executed command that we are going to log
        :param kwargs:  additional named arguments used to populate the data section of the json
        (example: hostclass, message, etc)
        """
        return self._call_function("EVENT", status, **kwargs)

    def validate(self):
        """
        Helper function used to verify if the asiaq command can be executed based on the status of the
        associated ticket
        :return: True if the validation was successful, False otherwise
        """
        return self._call_function("VALIDATE")

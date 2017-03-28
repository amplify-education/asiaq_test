"""
This module has utility functions for working with the socify lambda
"""
import json
import urllib2

import logging

from .disco_config import read_config

logger = logging.getLogger(__name__)


class SocifyHelper(object):
    """Socify helper provides the function to invoke the Socify lambda functions"""
    def __init__(self, config=None):
        if config:
            self._config = config
        else:
            self._config = read_config()
        self._socify_url = None

    def _build_event_url(self):
        """
        :return:
        """
        if not self._socify_url:
            self._socify_url = self._config.get("socify", "socify_baseurl")
        return self._socify_url + "/event"

    def _build_event_json(self, ticket_id, hostclass, command, message):
        """
        :param ticket_id:
        :param hostclass:
        :param command:
        :param message:
        :return:
        """
        data = {"Id": ticket_id,
                "cmd": command,
                "hostclass": hostclass,
                "data": {"status": message}}
        return data

    def send_event(self, ticket_id, hostclass, command, message):
        """
        :param ticket_id:
        :param hostclass:
        :param command:
        :param message:
        :return:
        """
        url = self._build_event_url()
        data = self._build_event_json(ticket_id, hostclass, command, message)
        try:
            req = urllib2.Request(url=url)
            req.add_header('Content-Type', 'application/json')
            response = urllib2.urlopen(req, data=json.dumps(data))
            data = response.read()
            logger.info("received response: %s", data)
            return data
        except urllib2.HTTPError as err:
            msg = err.read()
            logger.error("Failed to send event to Socify: %s", err)
            raise RuntimeError(msg)
        except Exception as err:
            logger.error("Failed to send event to Socify: %s", err)
            raise RuntimeError(err.reason.strerror)

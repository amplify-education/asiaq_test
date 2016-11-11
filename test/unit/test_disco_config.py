"""Tests for disco_config utilities."""

from unittest import TestCase
from mock import patch, Mock
import os

from disco_aws_automation import disco_config, exceptions


@patch("disco_aws_automation.disco_config.ASIAQ_CONFIG", "FAKE_CONFIG_DIR")
class TestStuff(TestCase):
    ""

    @patch('os.path.exists')
    def test__normalized_path__no_such_path__exception(self, path_exists):
        path_exists.return_value = False
        self.assertRaises(exceptions.AsiaqConfigError, disco_config.normalize_path, "yabba", "dabba")
        path_exists.assert_called_once_with("FAKE_CONFIG_DIR/yabba/dabba")

    @patch('os.path.exists')
    def test__normalized_path__path_exists__path_returned(self, path_exists):
        path_exists.return_value = True
        found = disco_config.normalize_path("yabba", "dabba")
        self.assertEquals(found, "FAKE_CONFIG_DIR/yabba/dabba")
        path_exists.assert_called_once_with(found)

    @patch('os.path.exists')
    def test__normalized_path__list_arg__correct_path_returned(self, path_exists):
        path_exists.return_value = True
        found = disco_config.normalize_path(["yabba", "dabba"])
        self.assertEquals(found, "FAKE_CONFIG_DIR/yabba/dabba")
        path_exists.assert_called_once_with(found)

    @patch('os.path.exists', Mock(return_value=True))
    @patch('disco_aws_automation.disco_config.ConfigParser')
    def test__read_config__named_arg__expected_behavior(self, configparser_constructor):
        parser = Mock()
        configparser_constructor.return_value = parser
        parsed = disco_config.read_config(config_file="Foobar")
        self.assertIs(parsed, parser)
        parser.read.assert_called_once_with("FAKE_CONFIG_DIR/Foobar")

    @patch('os.path.exists', Mock(return_value=True))
    @patch('disco_aws_automation.disco_config.ConfigParser')
    def test__read_config__arglist__expected_behavior(self, configparser_constructor):
        parser = Mock()
        configparser_constructor.return_value = parser
        parsed = disco_config.read_config("foo", "bar")
        self.assertIs(parsed, parser)
        parser.read.assert_called_once_with("FAKE_CONFIG_DIR/foo/bar")

    @patch('os.path.exists', Mock(return_value=True))
    @patch('disco_aws_automation.disco_config.ConfigParser')
    def test__read_config__arg_combo__named_arg_last(self, configparser_constructor):
        parser = Mock()
        configparser_constructor.return_value = parser
        parsed = disco_config.read_config("foo", "bar", config_file="baz.ini")
        self.assertIs(parsed, parser)
        parser.read.assert_called_once_with("FAKE_CONFIG_DIR/foo/bar/baz.ini")

    @patch('os.path.exists', Mock(return_value=True))
    @patch('disco_aws_automation.disco_config.open')
    def test__open_normalized__passthrough_successful(self, open_mock):
        expected = Mock()
        open_mock.return_value = expected
        found = disco_config.open_normalized("path", "to", "file", mode="moody")
        self.assertIs(expected, found)
        open_mock.assert_called_once_with("FAKE_CONFIG_DIR/path/to/file", mode="moody")

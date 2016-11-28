"""Tests for disco_config utilities."""

from unittest import TestCase
from mock import patch, Mock

from disco_aws_automation import disco_config, exceptions


@patch("disco_aws_automation.disco_config.ASIAQ_CONFIG", "FAKE_CONFIG_DIR")
class TestNormalizePath(TestCase):
    """Tests for the normalize_path utility function."""

    @patch('os.path.exists')
    def test__no_such_path__exception(self, path_exists):
        "Normalize path finds no such path - exception."
        path_exists.return_value = False
        self.assertRaises(exceptions.AsiaqConfigError, disco_config.normalize_path, "yabba", "dabba")
        path_exists.assert_called_once_with("FAKE_CONFIG_DIR/yabba/dabba")

    @patch('os.path.exists')
    def test__path_exists__path_returned(self, path_exists):
        "Normalize path thinks the path exists - path is returned"
        path_exists.return_value = True
        found = disco_config.normalize_path("yabba", "dabba")
        self.assertEquals(found, "FAKE_CONFIG_DIR/yabba/dabba")
        path_exists.assert_called_once_with(found)

    @patch('os.path.exists')
    def test__list_arg__correct_path_returned(self, path_exists):
        "Normalize path works with a single list-typed argument"
        path_exists.return_value = True
        found = disco_config.normalize_path(["yabba", "dabba"])
        self.assertEquals(found, "FAKE_CONFIG_DIR/yabba/dabba")
        path_exists.assert_called_once_with(found)

    @patch('os.path.exists')
    def test__tuple_arg__correct_path_returned(self, path_exists):
        "Normalize path with a single tuple-typed argument"
        path_exists.return_value = True
        found = disco_config.normalize_path(("yabba", "dabba"))
        self.assertEquals(found, "FAKE_CONFIG_DIR/yabba/dabba")
        path_exists.assert_called_once_with(found)


@patch("disco_aws_automation.disco_config.ASIAQ_CONFIG", "FAKE_CONFIG_DIR")
@patch('disco_aws_automation.disco_config.AsiaqConfig')
class TestReadConfig(TestCase):
    """Tests for the read_config utility function."""

    @patch('os.path.exists', Mock(return_value=True))
    def test__no_arg__default_behavior(self, configparser_constructor):
        "Default argument for read_config works"
        parser = Mock()
        configparser_constructor.return_value = parser
        parsed = disco_config.read_config()
        self.assertIs(parsed, parser)
        parser.read.assert_called_once_with("FAKE_CONFIG_DIR/disco_aws.ini")

    @patch('os.path.exists', Mock(return_value=True))
    def test__named_arg__expected_behavior(self, configparser_constructor):
        "Keyword argument for read_config works"
        parser = Mock()
        configparser_constructor.return_value = parser
        parsed = disco_config.read_config(config_file="Foobar")
        self.assertIs(parsed, parser)
        parser.read.assert_called_once_with("FAKE_CONFIG_DIR/Foobar")

    @patch('os.path.exists', Mock(return_value=True))
    def test__arglist__expected_behavior(self, configparser_constructor):
        "Unnamed argument list for read_config works"
        parser = Mock()
        configparser_constructor.return_value = parser
        parsed = disco_config.read_config("foo", "bar")
        self.assertIs(parsed, parser)
        parser.read.assert_called_once_with("FAKE_CONFIG_DIR/foo/bar")

    @patch('os.path.exists', Mock(return_value=True))
    def test__arg_combo__named_arg_last(self, configparser_constructor):
        "Combined keyword and listed args for read_config work"
        parser = Mock()
        configparser_constructor.return_value = parser
        parsed = disco_config.read_config("foo", "bar", config_file="baz.ini")
        self.assertIs(parsed, parser)
        parser.read.assert_called_once_with("FAKE_CONFIG_DIR/foo/bar/baz.ini")


@patch("disco_aws_automation.disco_config.ASIAQ_CONFIG", "FAKE_CONFIG_DIR")
class TestOpenNormalized(TestCase):
    """Tests for the open_normalized utility function."""

    @patch('os.path.exists', Mock(return_value=True))
    @patch('disco_aws_automation.disco_config.open')
    def test__path_exists__passthrough_successful(self, open_mock):
        "Valid path for open_normalized - 'open' called"
        expected = Mock()
        open_mock.return_value = expected
        found = disco_config.open_normalized("path", "to", "file", mode="moody")
        self.assertIs(expected, found)
        open_mock.assert_called_once_with("FAKE_CONFIG_DIR/path/to/file", mode="moody")

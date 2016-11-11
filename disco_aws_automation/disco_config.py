"""
Package for reading configurations out of standard locations.
"""

import os
import os.path
from ConfigParser import ConfigParser
import argparse

from .exceptions import AsiaqConfigError

ASIAQ_CONFIG = os.getenv("ASIAQ_CONFIG", ".")
DEFAULT_CONFIG_FILE = "disco_aws.ini"


def read_config(*path_components, **kwargs):
    """
    Normalize and read in a config file (defaulting to "disco_aws.ini").

    For call compatibility, we allow either an arglist or a named argument.  If you are sufficiently
    over-clever to use both the config_file keyword argument and an argument list, you should get
    a reasonable behavior (the keyword arg will be the file, the other parts will be the directory path),
    but seriously why?
    """
    all_components = list(path_components)  # copy the list to avoid aliasing
    if 'config_file' in kwargs:
        all_components.append(kwargs['config_file'])
    if not all_components:
        all_components
    real_config_file = normalize_path(all_components)
    config = ConfigParser()
    config.read(real_config_file)
    return config


def normalize_path(*path_components):
    """
    Prepend ASIAQ_CONFIG (or ".") to the list of path components, and join them into a path string.
    If the resulting path points to a nonexistent file, raise AsiaqConfigError, otherwise return the path.

    Takes either an arglist or a single list as an argument.

    NOTE: directories and symlinks "exist" for purposes of this function.
    """
    real_components = (
        path_components[0] if path_components and isinstance(path_components[0], (tuple, list))
        else path_components)
    normalized_path = os.path.join(ASIAQ_CONFIG, *real_components)
    if os.path.exists(normalized_path):
        return normalized_path
    else:
        raise AsiaqConfigError("Config path not found: %s" % normalized_path)


def open_normalized(*path_components, **kwargs):
    """
    Find a file in the configuration directory and open it.  Non-keyword arguments
    are treated as path segments; keyword arguments are passed through to the "open" function.
    """
    real_path = normalize_path(path_components)
    return open(real_path, **kwargs)

"""
Code for a conventional entry-point based command-line interface.
"""

import argparse
import os
from logging import getLogger

import boto3

from . import DiscoVPC, DiscoAWS, read_config
from .disco_aws_util import read_pipeline_file, graceful
from .disco_logging import configure_logging

PEERING_SECTION = 'peerings'
BLACK_LIST_S3_CONTAINER = "zookeeper-sync-black-lists"
BLACK_LIST_BUCKET = 'us-west-2.mclass.sandboxes'


class CliCommand(object):
    """
    Abstract(ish) base class for CLI subcommand drivers.  Common behaviors of all subcommands
    can be implemented here, but of course there's no real guarantee that they'll be honored.

    Required elements in subclasses:

    * a static field called DESCRIPTION, which will be used in arg parsers
    * a class or static method called "init_args", which takes an ArgumentParser as its
      input and adds to it whatever arguments and options this command needs.
    """

    DESCRIPTION = "This command has no description.  Perhaps you should add one?"

    def __init__(self):
        self.logger = getLogger(type(self).__name__)

    def run(self, args):
        raise Exception("This is an abstact method. Override it so this command does something useful!")

    @classmethod
    def init_args(cls, parser):
        """
        Set up any arguments and options for this command/subcommand.
        """
        pass


class SandboxCommand(CliCommand):
    """
    Command to manage sandboxes (currently, just creates, but should get other functions later).
    """

    DESCRIPTION = "Create and populate a sandbox for local development and testing."

    @classmethod
    def init_args(cls, parser):
        parser.add_argument("sandbox_name")

    def run(self, args):
        self.logger.debug("Updating sandbox %s", args.sandbox_name)
        sandbox_name = args.sandbox_name
        pipeline_file = os.path.join("sandboxes", sandbox_name, "pipeline.csv")

        aws_config = read_config()
        hostclass_dicts = read_pipeline_file(pipeline_file)

        self._update_blacklist(sandbox_name)

        self.logger.info("Checking if environment '%s' already exists", sandbox_name)
        vpc = DiscoVPC.fetch_environment(environment_name=sandbox_name)
        if vpc:
            self.logger.info("Sandbox %s already exists: updating it.", sandbox_name)
            vpc.update()
        else:
            vpc = DiscoVPC(environment_name=sandbox_name,
                        environment_type='sandbox',
                        defer_creation=True)
            peering_found = False
            peering_prefixes = ("*:sandbox", ("%s:sandbox" % sandbox_name))
            if vpc.config.has_section(PEERING_SECTION):
                for peering in vpc.config.options(PEERING_SECTION):
                    peers = vpc.config.get(PEERING_SECTION, peering)
                    self.logger.debug("Peering config: %s = '%s'", peering, peers)
                    if peers.startswith(peering_prefixes):
                        peering_found = True
                        break
                    elif peering.endswith("_99"):
                        raise Exception("oh this is going to be a problem")
            else:
                self.logger.warn("No peering section found")
                vpc.config.add_section(PEERING_SECTION)
            if not peering_found:
                self.logger.warn("Need to update peering config for %s", sandbox_name)
                vpc.config.set(PEERING_SECTION, "connection_99", "%s:sandbox/intranet ci/intranet" % sandbox_name)
            vpc.create()

        self.logger.debug("Hostclass definitions for spin-up: %s", hostclass_dicts)
        DiscoAWS(aws_config, sandbox_name, vpc=vpc).spinup(hostclass_dicts)


    def _update_blacklist(self, sandbox_name):
        local_blacklist = os.path.join("sandboxes", sandbox_name, "blacklist")
        remote_blacklist = os.path.join(BLACK_LIST_S3_CONTAINER, sandbox_name)
        self.logger.info("Uploading blacklist file %s to %s", local_blacklist, remote_blacklist)
        sandbox_s3_bucket = boto3.resource("s3").Bucket(name=BLACK_LIST_BUCKET)
        sandbox_s3_bucket.upload_file(local_blacklist, remote_blacklist)


def _base_arg_init(parser):
    parser.add_argument("--debug", "-d", action='store_const', const=True,
                        help='Log at DEBUG level.')

SUBCOMMANDS = {
    "sandbox": SandboxCommand
}

@graceful
def sandbox_command():
    args = _command_init(SandboxCommand.DESCRIPTION, SandboxCommand.init_args)
    SandboxCommand().run(args)


@graceful
def super_command():
    parser = argparse.ArgumentParser(description="All the Asiaq Things")
    _base_arg_init(parser)
    subcommands = parser.add_subparsers(title="subcommands", dest="command")
    for subcommand, driver in SUBCOMMANDS.items():
        sub_parser = subcommands.add_parser(subcommand,
                                            help=driver.DESCRIPTION, description=driver.DESCRIPTION)
        driver.init_args(sub_parser)
    args = parser.parse_args()
    configure_logging(debug=args.debug)

    if args.command:
        SUBCOMMANDS[args.command]().run(args)


def _command_init(description, argparse_setup_func):
    parser = argparse.ArgumentParser(description=description)
    _base_arg_init(parser)
    argparse_setup_func(parser)
    args = parser.parse_args()
    configure_logging(debug=args.debug)
    return args

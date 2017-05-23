#!/usr/bin/env python
"""
Disco Snapshot Purger.

When AMIs are created or Volumes are copied AWS creates an object called a
snapshot. It is a reference to the new volume. Deleting the corresponding
vsdolume does not delete the snapshot pointer and so they can build up.

This script cleans up snapshots that we no longer need. Including snapshots
that:
    - Created by CreateImage process but AMI is no longer available
    - have no tags

Usage:
    disco_purge_snapshots.py [options]

Options:
    -h --help                 Show this screen.
    --debug                   Log in debug level.
    --stray-ami               Purge only snapshots created by CreateImage
    --no-metadata             Purge only snapshots with no tags
    --old                     Purge only old snapshots (100 days) [DEPRECATED: use --keep-days instead]
    --keep-days DAYS          Delete snapshots older than this number of days
    --keep-num NUM            Keep at least this number of snapshots per hostclass per env
    --dry-run                 Only print what will be done
    --max-per-day NUM_PER_DAY Purge snapshots for hostclasses that take more than this number
                              of snapshots per day
"""

from __future__ import print_function
import re
from datetime import datetime
import sys
from itertools import groupby

import boto
from boto.exception import EC2ResponseError
from docopt import docopt
import iso8601
import pytz

from disco_aws_automation.disco_aws_util import run_gracefully
from disco_aws_automation.disco_logging import configure_logging

OLD_IMAGE_DAYS = 100
DEFAULT_KEEP_LAST = 5
NOW = datetime.now(pytz.UTC)


def run():
    """
    Main
    """
    args = docopt(__doc__)

    configure_logging(args["--debug"])

    # If no options are set, we assume user wants all of 'em.
    arg_options = ["--stray-ami", "--no-metadata",
                   "--keep-days", OLD_IMAGE_DAYS,
                   "--max-per-day", None,
                   "--keep-num", DEFAULT_KEEP_LAST]
    if not any([args[option] for option in arg_options if option in args]):
        args = docopt(__doc__, argv=arg_options)

    _ignore, failed_to_purge = purge_snapshots(args)
    if failed_to_purge:
        sys.exit(1)


def purge_snapshots(options):
    """
    Purge snapshots we consider no longer worth keeping
    """
    snaps_to_purge = []
    failed_to_purge = []

    ec2_conn = boto.connect_ec2()

    ami_snapshots, non_ami_snapshots = parse_snapshots(ec2_conn.get_all_snapshots(owner='self'))

    snapshot_hostclass_dict = create_hostclass_snapshot_dict(non_ami_snapshots)

    if options["--stray-ami"]:
        image_ids = [image.id for image in ec2_conn.get_all_images(owners=['self'])]
        snaps_to_purge.extend(purge_stray_ami_snapshots(ami_snapshots, image_ids))

    if options["--no-metadata"]:
        snaps_to_purge.extend(purge_no_metadata_snapshots(non_ami_snapshots))

    if options["--old"] or options["--keep-days"]:
        old_days = int(options.get('--keep-days') or OLD_IMAGE_DAYS)
        snaps_to_purge.extend(purge_old_snapshots(old_days, non_ami_snapshots))

    if options['--max-per-day']:
        max_per_day = int(options['--max-per-day'])
        snaps_to_purge.extend(purge_extra_daily_snapshots(max_per_day, snapshot_hostclass_dict))

    if options.get('--keep-num'):
        snaps_to_keep = get_kept_snapshots(int(options.get('--keep-num')), snapshot_hostclass_dict)
        # remove the snapshots we plan to keep from purge list
        snaps_to_purge = [snap for snap in snaps_to_purge if snap not in snaps_to_keep]

    if not options["--dry-run"]:
        for snap in snaps_to_purge:
            try:
                snap.delete()
            except EC2ResponseError:
                failed_to_purge.append(snap)
                print("Failed to purge snapshot: {0}".format(snap.id))

    return snaps_to_purge, failed_to_purge


def parse_snapshots(snapshots):
    """
    Group the snapshots by type. Either ami snapshots or non-ami snapshots
    :param list[Snapshot] snapshots:
    :return list[Snapshot], list[Snapshot]:
    """
    snap_pattern = re.compile(
        r"Created by CreateImage\(i-[a-f0-9]+\) for ami-[a-f0-9]+"
    )

    ami_snapshots = []
    non_ami_snapshots = []

    for snap in snapshots:
        if snap_pattern.search(snap.description):
            ami_snapshots.append(snap)
        else:
            non_ami_snapshots.append(snap)

    return ami_snapshots, non_ami_snapshots


def create_hostclass_snapshot_dict(snapshots):
    """
    Create a dictionary of hostclass name to a list of snapshots for that hostclass
    :param list[Snapshot] snapshots:
    :return dict[str, list[Snapshot]]:
    """
    snapshot_hostclass_dict = {}
    for snap in snapshots:
        # build a dict of hostclass+environment to a list of snapshots
        # use this dict for the --keep-num option to know how many snapshots are there for each hostclass
        if snap.tags and snap.tags.get('hostclass') and snap.tags.get('env'):
            key_name = snap.tags.get('hostclass') + '_' + snap.tags.get('env')
            hostclass_snapshots = snapshot_hostclass_dict.setdefault(key_name, [])
            hostclass_snapshots.append(snap)

    return snapshot_hostclass_dict


def purge_stray_ami_snapshots(ami_snapshots, image_ids):
    """
    Return a list of snapshots to purge that are leftover from deleted AMIs
    :param list[Snapshot] ami_snapshots:
    :param list[str] image_ids:
    :return:
    """
    ami_pattern = re.compile(r"ami-[a-f0-9]+")
    snaps_to_purge = []
    for snap in ami_snapshots:
        # snapshots for existing AMIs can't be deleted
        # get the AMI id from the description if there is one
        image_id = ami_pattern.search(snap.description).group(0)

        # skip snapshots that are in use by AMIs
        if image_id not in image_ids:
            print("Deleting stray ami snapshot: {0}".format(snap.id))
            snaps_to_purge.append(snap)

    return snaps_to_purge


def purge_no_metadata_snapshots(snapshots):
    """
    Return a list of snapshots to purge that are missing tags
    :param list[Snapshot] snapshots:
    :return list[Snapshot]:
    """
    snaps_to_purge = []
    for snap in snapshots:
        if not snap.description and not snap.tags:
            snaps_to_purge.append(snap)

    return snaps_to_purge


def purge_old_snapshots(old_days, snapshots):
    """
    Return a list of snapshots to purge that are older than the given number of days
    :param int old_days:
    :param list[Snapshot] snapshots:
    :return list[Snapshot]:
    """
    snaps_to_purge = []
    for snap in snapshots:
        snap_date = iso8601.parse_date(snap.start_time)
        snap_days_old = (NOW - snap_date).days

        if snap_days_old > old_days:
            print("Deleting old ({1} > {2} days) snapshot: {0}".format(
                snap.id, snap_days_old, old_days))
            snaps_to_purge.append(snap)

    return snaps_to_purge


def purge_extra_daily_snapshots(max_per_day, snapshot_hostclass_dict):
    """
    Return a list of snapshots to purge in order to keep the most recent "max_per_day" number
    of snapshots per day
    :param int max_per_day:
    :param dict[str, list[Snapshot]] snapshot_hostclass_dict:
    :return list[Snapshot]:
    """
    snaps_to_purge = []
    for hostclass, snapshots in snapshot_hostclass_dict.iteritems():
        snapshots = sorted(snapshots, key=lambda snap: snap.start_time)
        for key, group in groupby(snapshots, key=lambda snap: iso8601.parse_date(snap.start_time).date()):
            if (NOW.date() - key).days > 1:  # don't purge snapshots that are less than a day old
                group = sorted(group, key=lambda snap: snap.start_time, reverse=True)
                extra_snaps = group[max_per_day:]
                if extra_snaps:
                    print(
                        "Deleting {0} snapshots {1} for hostclass/env {2} "
                        "to keep no more than {3} of {4} snapshots on {5}"
                        .format(len(extra_snaps), extra_snaps, hostclass, max_per_day, len(group), key)
                    )
                    snaps_to_purge.extend(extra_snaps)
    return snaps_to_purge


def get_kept_snapshots(keep_count, snapshot_hostclass_dict):
    """
    Return a new list of snapshots to purge after making sure at least "keep_count"
    snapshots are kept for each hostclass in each environment
    :param int keep_count:
    :param dict[str, list[Snapshot] snapshot_hostclass_dict:
    :return list[Snapshot]:
    """
    if keep_count < 1:
        raise ValueError("The number of snapshots to keep must be greater than 1 for --keep-num")
    snaps_to_keep = []
    for hostclass_snapshots in snapshot_hostclass_dict.values():
        keep_for_hostclass = sorted(hostclass_snapshots,
                                    key=lambda snap: iso8601.parse_date(snap.start_time))[-keep_count:]
        snaps_to_keep.extend(keep_for_hostclass)

        snap_ids = ', '.join([snap.id for snap in keep_for_hostclass])
        hostclass = keep_for_hostclass[0].tags['hostclass']
        env = keep_for_hostclass[0].tags['env']

        print(
            "Keeping last %s snapshots (%s) for hostclass %s in environment %s" %
            (keep_count, snap_ids, hostclass, env)
        )

    return snaps_to_keep

if __name__ == "__main__":
    run_gracefully(run)

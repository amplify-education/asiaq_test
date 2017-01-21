"""Contains DiscoElastigroup class that orchestrates AWS Spotinst Elastigruops"""
import logging
import time
import json
import requests

from os.path import expanduser
from base64 import b64encode

logger = logging.getLogger(__name__)

SPOTINST_API = 'https://api.spotinst.io/aws/ec2/group/'

class DiscoElastigroup(object):
    """Class orchestrating elastigroups"""

    def __init__(self, environment_name, token=None, session=None):
        self.environment_name = environment_name
        self._token = token or None
        self._session = session or None
        self.account_id = '646102706174'

    @property
    def token(self):
        """
        Returns spotinst auth token from JSON file in ~/.aws/spotinst_api_token

        File format example:

        {
          "name": "user_ampli
          "token": "f7e6c5abb51bb04fcaa411b7b70cce414c821bf719f7db0679b296e588630515"
        }
        """
        token_file = json.load(open(expanduser('~')+'/.aws/spotinst_api_token'))
        if token_file:
            self._token = token_file['token']
        return self._token

    @property
    def session(self):
        """Lazily create session object"""
        if not self._session:
            self._session = requests.Session()

        # Insert auth token in header
        self._session.headers.update(
            {
              "Content-Type" : "application/json",
              "Authorization": "Bearer {}".format(self.token)
            }
        )
        return self._session

    def get_new_groupname(self, hostclass):
        """Returns a new elastigroup name when given a hostclass"""
        return self.environment_name + '_' + hostclass + "_" + str(int(time.time()))

    def _filter_by_environment(self, groups):
        """Filters elastigroups by environment"""
        return [
            group for group in groups
            if group['name'].startswith("{0}_".format(self.environment_name))
        ]

    def get_hostclass(self, groupname):
        """Returns the hostclass when given an elastigroup name"""
        return groupname.split('_')[1]

    def get_existing_groups(self, hostclass):
        """
        Returns all elastigroups for a given hostclass, sorted by most recent creation. If no
        elastigroup can be found, returns an empty list.
        """
        try:
            groups = self.session.get(SPOTINST_API).json()['response']['items']
        except KeyError:
            return []
        filtered_groups = [group for group in groups if hostclass in group["name"]]
        filtered_groups.sort(key=lambda group: group["updatedAt"], reverse=True)
        return filtered_groups

    # def get_existing_group(self, hostclass=None, group_name=None, throw_on_two_groups=True):
    #     """
    #     Returns the elastigroup object for the given hostclass or group name, or None if no
    #     elastigroup exists.
    #
    #     If two or more autoscaling groups exist for a hostclass, then this method will throw an exception,
    #     unless 'throw_on_two_groups' is False. Then if there are two groups the most recently created
    #     autoscaling group will be returned. If there are more than two autoscaling groups, this method will
    #     always throw an exception.
    #     """
    #     groups = self.get_existing_groups(hostclass=hostclass, group_name=group_name)
    #     if not groups:
    #         return None
    #     elif len(groups) == 1 or (len(groups) == 2 and not throw_on_two_groups):
    #         return groups[0]
    #     else:
    #         raise TooManyAutoscalingGroups("There are too many autoscaling groups for {}.".format(hostclass))

    def get_group_ids(self,hostclass):
        """Returns list of elastigroup ids pertaining to a hostclass"""
        groups = self.get_existing_groups(hostclass)
        group_ids = [ group["id"] for group in groups
                      if hostclass in group["name"] ]
        return group_ids

    def get_group_instances(self,group_id):
        """Returns list of instance ids in a group"""
        instances = self.session.get(SPOTINST_API + group_id + '/status').json()['response']['items']
        return [ instance['instanceId'] for instance in instances ]

    def create_elastigroup_config(self, hostclass, availability_vs_cost, desired_size, min_size, max_size,
                                  spot_instances, zones, security_groups, instance_monitoring, ebs_optimized, image_id,
                                  key_name, user_data, instance_profile_name, tags):
        """Create new elastigroup configuration"""
        group_name = self.get_new_groupname(hostclass)
        strategy = {
            'risk': 100,
            'availabilityVsCost': availability_vs_cost,
            'fallbackToOd': True
    }

        capacity = {
            "target": desired_size,
            "minimum": min_size,
            "maximum": max_size,
            "unit": "instance"
        }

        compute = {}

        compute["instanceTypes"] = {
            "ondemand": "t2.micro",
            "spot": spot_instances.split(',')
        }

        compute["availabilityZones"] = [ {'name': zone, 'subnetIds': [subnet_id]}
            for zone, subnet_id in zones.iteritems() ]

        compute["product"] = "Linux/UNIX"

        launch_specification = {
            "securityGroupIds": security_groups,
            "monitoring": instance_monitoring,
            "ebsOptimized": ebs_optimized,
            "imageId": image_id,
            "keyPair": key_name,
            "userData": b64encode(str(user_data)),
            "iamRole": {
                "arn": "arn:aws:iam::{}:instance-profile/{}"
                .format(self.account_id, instance_profile_name)
            },
            "tags": self._create_elastigroup_tags(tags)
        }

        compute["launchSpecification"] = launch_specification

        group = {
            "name": group_name,
            "description": "Spotinst elastigroup: {}".format(group_name),
            "strategy": strategy,
            "capacity": capacity,
            "compute": compute
        }

        logger.info(
            "Creating elastigroup config for elastigroup '%s'", group_name)

        elastigroup_config = { "group": group }
        return json.dumps(elastigroup_config)

    def _create_elastigroup_tags(self, tags):
        """Given a python dictionary, return list of elastigroups tags"""
        return [ {'tagKey': key, 'tagValue': str(value)}
                for key, value in tags.iteritems() ] if tags else None

    def _create_az_subnets_dict(self, subnets):
        zones = {}
        for subnet in subnets:
            zones[subnet['AvailabilityZone']] = subnet['SubnetId']
        return zones

    def create_group(self, hostclass, availability_vs_cost="balanced", desired_size=None,  min_size=None, max_size=None,
                     spot_instances=None, subnets=None, security_groups=None, instance_monitoring=None,
                     ebs_optimized=None, image_id=None, key_name=None, user_data=None, instance_profile_name=None,
                     tags=None):
        """Create an elastigroup for a given hostclass"""
        group_config = self.create_elastigroup_config(
            hostclass=hostclass,
            availability_vs_cost=availability_vs_cost,
            desired_size=desired_size,
            min_size=min_size,
            max_size=max_size,
            spot_instances=spot_instances,
            zones=self._create_az_subnets_dict(subnets),
            security_groups=security_groups,
            instance_monitoring=instance_monitoring,
            ebs_optimized=ebs_optimized,
            image_id=image_id,
            key_name=key_name,
            user_data=user_data,
            instance_profile_name=instance_profile_name,
            tags=tags
        )
        self.session.post(SPOTINST_API, data=group_config)

    def delete_group(self, group_id):
        """Delete an elastigroup by group id"""
        self.session.delete(SPOTINST_API + group_id)

    def delete_groups(self, hostclass):
        """Delete all elastigroups pertaining to a hostclass"""
        group_ids = self.get_group_ids(hostclass)
        for group_id in group_ids:
            self.delete_group(group_id)

    # def terminate(self, instance_id, decrement_capacity=True):
    #     """
    #     Terminate instances using the spotinst API.
    #
    #     Detaching instances from elastigroup will delete them.
    #
    #     When decrement_capacity is True this allows us to avoid
    #     autoscaling immediately replacing a terminated instance.
    #     """
    #     pass

'''Contains DiscoSpotinst class that orchestrates AWS Spotinst Elastigruops'''
import logging
import time
import json
import requests

from os.path import expanduser
from base64 import b64encode

from .resource_helper import throttled_call
from .exceptions import TooManyAutoscalingGroups

logger = logging.getLogger(__name__)

SPOTINST_API = 'https://api.spotinst.io/aws/ec2/group/'

class DiscoSpotinst(object):
    '''Class orchestrating elastigroups'''

    def __init__(self, environment_name, token=None, session=None):
        self.environment_name = environment_name
        self._token = token or None
        self._session = session or None

    @property
    def token(self):
        '''
        Returns spotinst auth token from JSON file in ~/.aws/spotinst_api_token

        File format example:

        {
          "name": "user_amplify",
          "token": "f7e6c5abb51bb04fcaa411b7b70cce414c821bf719f7db0679b296e588630515"
        }
        '''
        token_file = json.load(open(expanduser('~')+'/.aws/spotinst_api_token'))
        if token_file:
            self._token = token_file['token']
        return self._token

    @property
    def session(self):
        '''Lazily create session object'''
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
        '''Returns a new elastigroup name when given a hostclass'''
        return self.environment_name + '_' + hostclass + "_" + str(int(time.time()))

    def _filter_by_environment(self, groups):
        '''Filters elastigroups by environment'''
        return [
            group for group in groups
            if group['name'].startswith("{0}_".format(self.environment_name))
        ]

    def get_hostclass(self, groupname):
        '''Returns the hostclass when given an elastigroup name'''
        return groupname.split('_')[1]

    def _get_group_generator(self):
        '''Yields elastigroups in current environment'''
        groups = self.session.get(SPOTINST_API).json()['response']['items']
        for group in self._filter_by_environment(groups):
            yield group

    def _get_instance_generator(self, instance_ids=None, hostclass=None, group_name=None):
        '''Yields elastigroup instances in current environment'''
        pass

    def get_instances(self, instance_ids=None, hostclass=None, group_name=None):
        '''Returns elastigroup instances in the current environment'''
        return list(self._get_instance_generator(instance_ids=instance_ids, hostclass=hostclass,
                                                 group_name=group_name))

    def delete_groups(self, hostclass=None):
        '''Delete elastigroups, filtering on hostclass.'''
        groups = self.get_existing_groups(hostclass=hostclass)
        group_ids = [ group["id"] for group in groups
                      if hostclass in group["name"] ]
        for group_id in group_ids:
            self.session.delete(SPOTINST_API + group)

    @staticmethod
    def create_elastigroup_tags(tags):
        '''Given a python dictionary return list of elastigroups tags'''
        return [{tagKey: key, tagValue: value} for key, value in tags.iteritems()] if tags else None

    def create_group(self, hostclass, group_config, vpc_zone_id, image_id,
                     min_size=None, max_size=None, desired_size=None, tags=None):
        '''
        Create an elastigroup.'''
        group_config = self.create_elastigroup_config(
            group_name = self.get_new_groupname(hostclass),
            desired_size = desired_size,
            min_size = min_size,
            max_size = max_size,
            image_id = image_id,
            tags=self.create_elastigroup_tags(tags)
        )

        self.session.post(API, json=group_config)

    def get_existing_groups(self, hostclass):
        '''
        Returns all elastigroups for a given hostclass, sorted by most recent creation. If no
        elastigroup can be found, returns an empty list.
        '''
        groups = list(self._get_group_generator())
        filtered_groups = [group for group in groups if hostclass in group["name"]]
        filtered_groups.sort(key=lambda group: group["updatedAt"], reverse=True)
        return filtered_groups

    def get_existing_group(self, hostclass=None, group_name=None, throw_on_two_groups=True):
        '''
        Returns the elastigroup object for the given hostclass or group name, or None if no
        elastigroup exists.

        If two or more autoscaling groups exist for a hostclass, then this method will throw an exception,
        unless 'throw_on_two_groups' is False. Then if there are two groups the most recently created
        autoscaling group will be returned. If there are more than two autoscaling groups, this method will
        always throw an exception.
        '''
        groups = self.get_existing_groups(hostclass=hostclass, group_name=group_name)
        if not groups:
            return None
        elif len(groups) == 1 or (len(groups) == 2 and not throw_on_two_groups):
            return groups[0]
        else:
            raise TooManyAutoscalingGroups("There are too many autoscaling groups for {}.".format(hostclass))

    def terminate(self, instance_id, decrement_capacity=True):
        '''
        Terminate instances using the spotinst API.

        Detaching instances from elastigroup will delete them.

        When decrement_capacity is True this allows us to avoid
        autoscaling immediately replacing a terminated instance.
        '''
        pass

    def create_elastigroup_config(
            self,
            group_name,
            image_id,
            user_data,
            account_id,
            instance_profile_name,
            tags,
            availabilityVsCost="balanced",
            desired_size=1,
            min_size=1,
            max_size=1,
            spot="m3.medium",
            key_pair="bake"
    ):
        '''
        Creates a new elastigroup configuration. Handles the logic of constructing the correct autoscaling policy request,
        because not all parameters are required.
        '''

        elastigroup_config = { "group": group }
        group = {
            "name": group_name,
            "description": "Spotinst elastigroup: {}".format(group_name),
            "capacity": capacity,
            "compute": compute
        }

        capacity = {
            "target": desired_size,
            "minimum": min_size,
            "maximum": max_size,
            "unit": "instance"
        }

        compute["instanceTypes"] = {
            "ondemand": "t2.micro",
            "spot": [
               "m3.medium"
            ]
        }

        for zone in zones:
            compute["availabilityZones"] = [
                {"name": zone, "subnetIds": [subnet_id]}
        ]

        compute["product"] = "Linux/UNIX"
        compute["launchSpecification"] = launchSpecification

        launchSpecification = {
            "securityGroupIds": [ metanetwork_sg ],
            "monitoring": false,
            "ebsOptimized": false,
            "imageId": image_id,
            "keyPair": key_pair,
            "userData": b64encode(user_data),
            "iamRole": {
                "arn": "arn:aws:iam::{}:instance-profile/{}"
                .format(account_id, instance_profile_name)
            },
            "tags": tags
        }


        logger.info(
            "Creating elastigroup config for elastigroup '%s'",group_name)

        return json.dumps(elastigroup_config)

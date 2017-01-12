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

    def __init__(self, environment_name, token, session=None):
        self.environment_name = environment_name
        self._token = token or None
        self._session = session

        # Insert auth token in header
        self._session.headers.update(
            {
              "Authorization": "Bearer {}".format(self._token)
            }
        )

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
        return token_file['token']

    @property
    def session(self):
        '''Lazily create session object'''
        if not self._session:
            self._session = requests.Session()
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

    def _get_group_generator(self, group_names=None):
        '''Yields elastigroups in current environment'''
        groups = self._session.get(SPOTINST_API).json()['response']['items']
        for group in self._filter_by_environment(groups):
            yield group

    def get_instances(self, instance_ids=None, hostclass=None, group_name=None):
        '''Returns autoscaled instances in the current environment'''
        return list(self._get_instance_generator(instance_ids=instance_ids, hostclass=hostclass,
                                                 group_name=group_name))


    def get_config(self, *args, **kwargs):
        '''Returns a new launch configuration'''
        config = boto.ec2.autoscale.launchconfig.LaunchConfiguration(
            connection=self.connection, *args, **kwargs
        )
        throttled_call(self.connection.create_launch_configuration, config)
        return config

    def delete_config(self, config_name):
        '''Delete a specific Launch Configuration'''
        throttled_call(self.connection.delete_launch_configuration, config_name)
        logger.info("Deleting launch configuration %s", config_name)

    def clean_configs(self):
        '''Delete unused Launch Configurations in current environment'''
        logger.info("Cleaning up unused launch configurations in %s", self.environment_name)
        for config in self._get_config_generator():
            try:
                self.delete_config(config.name)
            except BotoServerError:
                pass

    def delete_groups(self, hostclass=None, group_name=None, force=False):
        '''Delete elastigroups, filtering on either hostclass or the group_name.'''
        groups = self.get_existing_groups(hostclass=hostclass, group_name=group_name)
        for group in groups:
            try:
                throttled_call(group.delete, force_delete=force)
                logger.info("Deleting group %s", group.name)
                self.delete_config(group.launch_config_name)
            except BotoServerError as exc:
                logger.info("Unable to delete group %s due to: %s. Force delete is set to %s",
                            group.name, exc.message, force)

    def clean_groups(self, force=False):
        '''Delete all elastigroups in the current environment'''
        self.delete_groups()


    @staticmethod
    def create_elastiscale_tags(group_name, tags):
        '''Given a python dictionary return list of elastigroups tags'''
        return [{tagKey: key, tagValue: value} for key, value in tags.iteritems()] if tags else None

    def update_group(self, group, launch_config, vpc_zone_id=None,
                     min_size=None, max_size=None, desired_size=None,
                     termination_policies=None, tags=None,
                     load_balancers=None):
        '''Update an existing autoscaling group'''
        group.launch_config_name = launch_config
        if vpc_zone_id:
            group.vpc_zone_identifier = vpc_zone_id
        if min_size is not None:
            group.min_size = min_size
        if max_size is not None:
            group.max_size = max_size
        if desired_size is not None:
            group.desired_capacity = desired_size
        if termination_policies:
            group.termination_policies = termination_policies
        throttled_call(group.update)
        if tags:
            throttled_call(self.connection.create_or_update_tags,
                           DiscoAutoscale.create_autoscale_tags(group.name, tags))
        if load_balancers:
            self.update_elb(elb_names=load_balancers, group_name=group.name)
        return group

    def create_group(self, hostclass, launch_config, vpc_zone_id,
                     min_size=None, max_size=None, desired_size=None,
                     termination_policies=None, tags=None,
                     load_balancers=None):
        '''
        Create an autoscaling group.

        The group must not already exist. Use get_group() instead if you want to update a group if it
        exits or create it if it does not.
        '''
        _min_size = min_size or 0
        _max_size = max([min_size, max_size, desired_size, 0])
        _desired_capacity = desired_size or max_size
        #termination_policies = termination_policies or DEFAULT_TERMINATION_POLICIES
        group_name = self.get_new_groupname(hostclass)
        group = boto.ec2.autoscale.group.AutoScalingGroup(
            connection=self.connection,
            name=group_name,
            launch_config=launch_config,
            #load_balancers=load_balancers,
            default_cooldown=None,
            health_check_type=None,
            health_check_period=None,
            placement_group=None,
            vpc_zone_identifier=vpc_zone_id,
            desired_capacity=_desired_capacity,
            min_size=_min_size,
            max_size=_max_size,
            tags=DiscoAutoscale.create_autoscale_tags(group_name, tags),
            #termination_policies=termination_policies,
            instance_id=None)
        throttled_call(self.connection.create_auto_scaling_group, group)
        return group

    # pylint: disable=too-many-arguments
    def get_group(self, hostclass, launch_config, vpc_zone_id=None,
                  min_size=None, max_size=None, desired_size=None,
                  termination_policies=None, tags=None,
                  load_balancers=None, create_if_exists=False,
                  group_name=None):
        '''
        Returns autoscaling group.
        This updates an existing autoscaling group if it exists,
        otherwise this creates a new autoscaling group.

        NOTE: Deleting tags is not currently supported.
        '''
        # Check if an autoscaling group already exists.
        existing_group = self.get_existing_group(hostclass=hostclass, group_name=group_name)
        if create_if_exists or not existing_group:
            group = self.create_group(
                hostclass=hostclass, launch_config=launch_config, vpc_zone_id=vpc_zone_id,
                min_size=min_size, max_size=max_size, desired_size=desired_size,
                termination_policies=termination_policies, tags=tags, load_balancers=load_balancers)
        else:
            group = self.update_group(
                group=existing_group, launch_config=launch_config,
                vpc_zone_id=vpc_zone_id, min_size=min_size, max_size=max_size, desired_size=desired_size,
                termination_policies=termination_policies, tags=tags, load_balancers=load_balancers)

        # Create default scaling policies
        self.create_policy(
            group_name=group.name,
            policy_name='up',
            policy_type='SimpleScaling',
            adjustment_type='PercentChangeInCapacity',
            scaling_adjustment='10',
            min_adjustment_magnitude='1'
        )
        self.create_policy(
            group_name=group.name,
            policy_name='down',
            policy_type='SimpleScaling',
            adjustment_type='PercentChangeInCapacity',
            scaling_adjustment='-10',
            min_adjustment_magnitude='1'
        )

        return group

    def get_existing_groups(self, hostclass=None, group_name=None):
        '''
        Returns all elastigroups for a given hostclass, sorted by most recent creation. If no
        autoscaling groups can be found, returns an empty list.
        '''
        groups = list(self._get_group_generator(group_names=[group_name]))
        filtered_groups = [group for group in groups
                           if not hostclass or self.get_hostclass(group.name) == hostclass]
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
        """
        Terminates an instance using the autoscaling API.

        When decrement_capacity is True this allows us to avoid
        autoscaling immediately replacing a terminated instance.
        """
        throttled_call(self.connection.terminate_instance,
                       instance_id, decrement_capacity=decrement_capacity)

    def create_elastigroup_config(
            self,
            group_name,
            availabilityVsCost="balanced",
            desired_size=1,
            min_size=1,
            max_size=1,
            spot="m3.medium",
            keyPair="bake"
    ):
        '''
        Creates a new elastigroup configuration. Handles the logic of constructing the correct autoscaling policy request,
        because not all parameters are required.
        '''

        elastigroup_config = { "group": group }
        group = {
            "name": group_name,
            "description": "Spotinst elastigroup: {}".format(group_name)
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


        logger.info(
            "Creating elastigroup config for elastigroup '%s'",group_name)

        return json.dumps(elastigroup_config)

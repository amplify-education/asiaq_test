"""Contains DiscoElastigroup class that orchestrates AWS Spotinst Elastigruops"""
import logging
import time
import json
from os.path import expanduser
from base64 import b64encode
import requests

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
        token_file = json.load(open(expanduser('~') + '/.aws/spotinst_api_token'))
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
                "Content-Type": "application/json",
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

    def get_hostclass(self, group_name):
        """Returns the hostclass when given an elastigroup name"""
        return group_name.split('_')[1]

    def get_existing_groups(self, hostclass=None, group_name=None):
        """
        Returns all elastigroups for a given hostclass, sorted by most recent creation. If no
        elastigroup can be found, returns an empty list.
        """
        try:
            groups = self.session.get(SPOTINST_API).json()['response']['items']
        except KeyError:
            return []
        filtered_groups = [group for group in groups
                           if not hostclass or self.get_hostclass(group_name) == hostclass]
        filtered_groups.sort(key=lambda group: group["name"], reverse=True)
        return filtered_groups

    def get_group_ids(self, hostclass=None, group_name=None):
        """Returns list of elastigroup ids filtered by hostclass or group_name"""
        groups = self.get_existing_groups(hostclass, group_name)
        group_ids = [group["id"] for group in groups]
        return group_ids

    def get_group_instances(self, group_id):
        """Returns list of instance ids in a group"""
        instances = self.session.get(SPOTINST_API + group_id + '/status').json()['response']['items']
        return [instance['instanceId'] for instance in instances]

    def create_elastigroup_config(self, hostclass, availability_vs_cost, desired_size, min_size, max_size,
                                  instance_type, zones, load_balancers, security_groups, instance_monitoring,
                                  ebs_optimized, image_id, key_name, associate_public_ip_address, user_data,
                                  tags, instance_profile_name, block_device_mappings):
        # Pylint thinks this function has too many arguments and too many local variables
        # pylint: disable=R0913, R0914
        """Create new elastigroup configuration"""
        group_name = self.get_new_groupname(hostclass)
        strategy = dict(risk=100, availabilityVsCost=availability_vs_cost, fallbackToOd=True)
        capacity = dict(target=desired_size, minimum=min_size, maximum=max_size, unit="instance")

        compute = {"instanceTypes": {
            "ondemand": "t2.small",
            "spot": instance_type.split(',')
        }, "availabilityZones": [{'name': zone, 'subnetIds': [subnet_id]}
                                 for zone, subnet_id in zones.iteritems()], "product": "Linux/UNIX"}

        bdms = []
        for name, ebs in block_device_mappings[0].iteritems():
            if any([ebs.size, ebs.iops, ebs.snapshot_id]):
                bdm = {'deviceName': name, 'ebs': {'deleteOnTermination': ebs.delete_on_termination}}
                if ebs.size:
                    bdm['ebs']['volumeSize'] = ebs.size
                if ebs.iops:
                    bdm['ebs']['iops'] = ebs.iops
                if ebs.volume_type:
                    bdm['ebs']['volumeType'] = ebs.volume_type
                if ebs.snapshot_id:
                    bdm['ebs']['snapshotId'] = ebs.snapshot_id
                bdms.append(bdm)
            else:
                bdms = None

        network_interfaces = [
            {"deleteOnTermination": True,
             "deviceIndex": 0,
             "associatePublicIpAddress": associate_public_ip_address}
        ] if associate_public_ip_address else None

        if load_balancers:
            elbs = [{"name": elb, "type": "CLASSIC"} for elb in load_balancers]
        else:
            elbs = None

        launch_specification = {
            "loadBalancersConfig": {
                "loadBalancers": elbs
            },
            "securityGroupIds": security_groups,
            "monitoring": instance_monitoring,
            "ebsOptimized": ebs_optimized,
            "imageId": image_id,
            "keyPair": key_name,
            "blockDeviceMappings": bdms,
            "networkInterfaces": network_interfaces,
            "userData": b64encode(str(user_data)),
            "tags": self._create_elastigroup_tags(tags),
            "iamRole": {
                "arn": "arn:aws:iam::{}:instance-profile/{}".format(self.account_id, instance_profile_name)
            }
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

        elastigroup_config = {"group": group}
        return json.dumps(elastigroup_config)

    def _create_elastigroup_tags(self, tags):
        """Given a python dictionary, it returns a list of elastigroup tags"""
        return [{'tagKey': key, 'tagValue': str(value)}
                for key, value in tags.iteritems()] if tags else None

    def _create_az_subnets_dict(self, subnets):
        zones = {}
        for subnet in subnets:
            zones[subnet['AvailabilityZone']] = subnet['SubnetId']
        return zones

    def update_group(self, hostclass, availability_vs_cost="balanced", desired_size=None, min_size=None,
                     max_size=None, instance_type=None, subnets=None, load_balancers=None,
                     security_groups=None, instance_monitoring=None, ebs_optimized=None, image_id=None,
                     key_name=None, associate_public_ip_address=None, user_data=None, tags=None,
                     instance_profile_name=None, block_device_mappings=None):
        # Pylint thinks this function has too many arguments and too many local variables
        # pylint: disable=R0913, R0914
        """Updates an existing elastigroup if it exists,
        otherwise this creates a new elastigroup."""
        kwargs = dict(
            hostclass=hostclass,
            availability_vs_cost=availability_vs_cost,
            desired_size=desired_size,
            min_size=min_size,
            max_size=max_size,
            instance_type=instance_type,
            load_balancers=load_balancers,
            zones=self._create_az_subnets_dict(subnets),
            security_groups=security_groups,
            instance_monitoring=instance_monitoring,
            ebs_optimized=ebs_optimized,
            image_id=image_id,
            key_name=key_name,
            associate_public_ip_address=associate_public_ip_address,
            user_data=user_data,
            tags=tags,
            instance_profile_name=instance_profile_name,
            block_device_mappings=block_device_mappings
        )
        group_config = self.create_elastigroup_config(**kwargs)
        if self.get_existing_groups(hostclass):
            group_id = self.get_group_ids(hostclass)[0]
            self.session.put(SPOTINST_API + group_id, data=group_config)
        else:
            self.session.post(SPOTINST_API, data=group_config)

    def delete_group(self, group_id, force=None):
        """Delete an elastigroup by group id"""
        self.session.delete(SPOTINST_API + group_id, force)

    def delete_groups(self, hostclass=None, group_name=None, force=False):
        """Delete all elastigroups based on hostclass or group name"""
        group_ids = self.get_group_ids(hostclass, group_name)
        for group_id in group_ids:
            self.delete_group(group_id, force)

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

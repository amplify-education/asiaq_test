"""
Tests of disco_elastigroup
"""
import random

from unittest import TestCase

from parameterized import parameterized
from mock import MagicMock, ANY, patch
from disco_aws_automation import DiscoElastigroup

ENVIRONMENT_NAME = "moon"


class DiscoElastigroupTests(TestCase):
    """Test DiscoElastigroup class"""

    def mock_elastigroup(self, hostclass, ami_id=None, min_size=2, max_size=2, desired_size=2):
        """Convenience function for creating an elastigroup object"""
        grp_id = 'sig-' + ''.join(random.choice("1234567890") for _ in range(10))
        name = "{0}_{1}_{2}".format(
            ENVIRONMENT_NAME,
            hostclass,
            ''.join(random.choice("1234567890") for _ in range(10))
        )
        ami_id = ami_id or 'ami-' + ''.join(random.choice("1234567890") for _ in range(12))
        mock_elastigroup = {
            "id": grp_id,
            "name": name,
            "capacity": {
                "minimum": min_size,
                "maximum": max_size,
                "target": desired_size,
                "unit": "instance"
            },
            "compute": {
                "product": "Linux/UNIX",
                "launchSpecification": {
                    "imageId": ami_id,
                    "loadBalancersConfig": {
                        "loadBalancers": [{
                            "name": "elb-1234",
                            "type": "CLASSIC"
                        }]
                    },
                    "blockDeviceMappings": [{
                        "deviceName": "/dev/xvda",
                        "ebs": {
                            "deleteOnTermination": "true",
                            "volumeSize": "80",
                            "volumeType": "gp2",
                            "snapshotId": "snapshot-abcd1234"
                        }
                    }]
                },
                "availabilityZones": [{
                    "name": 'us-moon-1a',
                    "subnetId": "subnet-abcd1234"
                }]
            },
            "scheduling": {
                "tasks": [{
                    'taskType': 'scale',
                    'cronExpression': '12 0 * * *',
                    'scaleMinCapcity': 5
                }]
            }
        }

        return mock_elastigroup

    def setUp(self):
        """Pre-test setup"""
        self.elastigroup = DiscoElastigroup(
            ENVIRONMENT_NAME
        )
        self.elastigroup.spotinst_client = MagicMock()

    def test_delete_groups_bad_hostclass(self):
        """Verifies elastigroup not deleted for bad hostclass"""
        self.elastigroup._delete_group = MagicMock()
        self.elastigroup._spotinst_call = MagicMock()

        self.elastigroup.delete_groups(hostclass="mhcfoo")

        self.assertFalse(self.elastigroup._delete_group.called)

    def test_delete_groups_bad_groupname(self):
        """Verifies elastigroup not deleted for bad group name"""
        self.elastigroup._delete_group = MagicMock()
        self.elastigroup._spotinst_call = MagicMock()

        self.elastigroup.delete_groups(group_name='moon_mhcfoo_12345678')

        self.assertFalse(self.elastigroup._delete_group.called)

    def test_delete_groups_good_hostclass(self):
        """Verifies elastigroup is deleted for only given hostclass"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')

        self.elastigroup._delete_group = MagicMock()
        self.elastigroup.get_existing_groups = MagicMock(return_value=[mock_group])

        self.elastigroup.delete_groups(hostclass='mhcfoo')

        self.elastigroup._delete_group.assert_called_once_with(group_id=mock_group['id'])

    def test_delete_groups_good_groupname(self):
        """Verifies elastigroup is deleted for only given group name"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')

        self.elastigroup._delete_group = MagicMock()
        self.elastigroup.get_existing_groups = MagicMock(return_value=[mock_group])

        self.elastigroup.delete_groups(group_name=mock_group['name'])

        self.elastigroup._delete_group.assert_called_once_with(group_id=mock_group['id'])

    def test_list_groups_with_groups(self):
        """Verifies that listgroups correctly formats elastigroups"""
        mock_group1 = self.mock_elastigroup(hostclass="mhcfoo")
        mock_group2 = self.mock_elastigroup(hostclass="mhcbar")
        self.elastigroup.spotinst_client.get_groups.return_value = [mock_group1, mock_group2]
        self.elastigroup.spotinst_client.get_group_status.return_value = [{
            "instanceId": "instance1"
        }, {
            "instanceId": "instance1"
        }]

        mock_listings = [
            {
                'name': mock_group1['name'],
                'image_id': mock_group1['compute']['launchSpecification']['imageId'],
                'group_cnt': 2,
                'min_size': mock_group1['capacity']['minimum'],
                'desired_capacity': mock_group1['capacity']['target'],
                'max_size': mock_group1['capacity']['maximum'],
                'type': 'spot'
            },
            {
                'name': mock_group2['name'],
                'image_id': mock_group2['compute']['launchSpecification']['imageId'],
                'group_cnt': 2,
                'min_size': mock_group2['capacity']['minimum'],
                'desired_capacity': mock_group2['capacity']['target'],
                'max_size': mock_group2['capacity']['maximum'],
                'type': 'spot'
            }
        ]

        self.assertEqual(self.elastigroup.list_groups(), mock_listings)

    def test_create_new_group(self):
        """Verifies new elastigroup is created"""
        self.elastigroup.spotinst_client.create_group.return_value = {
            'name': 'mhcfoo'
        }

        group = self.elastigroup.create_or_update_group(
            hostclass="mhcfoo",
            subnets=[{
                'SubnetId': 'sub-1234',
                'AvailabilityZone': 'us-moon-1'
            }],
            spotinst=True,
            instance_type='t2.small:m3.medium',
            min_size=1,
            desired_size=1,
            max_size=1
        )

        self.elastigroup.spotinst_client.create_group.assert_called_with({
            'group': {
                'compute': {
                    'product': 'Linux/UNIX',
                    'availabilityZones': [{'subnetIds': ['sub-1234'], 'name': 'us-moon-1'}],
                    'instanceTypes': {
                        'spot': ['t2.small', 'm3.medium'],
                        'ondemand': 't2.small'
                    },
                    'launchSpecification': {
                        "iamRole": None,
                        'userData': None,
                        'tags': [{'tagKey': 'group_name', 'tagValue': ANY},
                                 {'tagKey': 'spotinst', 'tagValue': 'True'}],
                        'blockDeviceMappings': None,
                        'imageId': None,
                        'networkInterfaces': None,
                        'monitoring': None,
                        'loadBalancersConfig': None,
                        'securityGroupIds': None,
                        'keyPair': None,
                        'ebsOptimized': None
                    }
                },
                'strategy': {
                    'onDemandCount': None,
                    'availabilityVsCost': 'availabilityOriented',
                    'fallbackToOd': True,
                    'risk': 100,
                    'utilizeReservedInstances': True
                },
                'capacity': {
                    'minimum': 1,
                    'target': 1,
                    'maximum': 1,
                    'unit': 'instance'
                },
                'name': ANY,
                'description': ANY
            }
        })
        self.assertEqual(group['name'], 'mhcfoo')

    def test_update_image_id(self):
        """Verifies updating AMI of an existing group"""
        group = self.mock_elastigroup(hostclass='mhcfoo')
        self.elastigroup.spotinst_client.get_groups.return_value = [group]

        self.elastigroup.create_or_update_group(
            hostclass="mhcfoo",
            spotinst=True,
            image_id="ami-123456"
        )

        expected_request = {
            'group': {
                'name': ANY,
                'capacity': ANY,
                'compute': {
                    'availabilityZones': ANY,
                    'launchSpecification': {
                        'blockDeviceMappings': ANY,
                        'loadBalancersConfig': ANY,
                        'imageId': 'ami-123456'
                    }
                },
                'scheduling': ANY
            }
        }

        self.elastigroup.spotinst_client.update_group.assert_called_once_with(group['id'], expected_request)

    def test_update_size(self):
        """Verifies resizing an existing group"""
        group = self.mock_elastigroup(hostclass='mhcfoo')
        self.elastigroup.spotinst_client.get_groups.return_value = [group]

        self.elastigroup.create_or_update_group(
            hostclass="mhcfoo",
            spotinst=True,
            min_size=5,
            max_size=10,
            desired_size=5
        )

        expected_request = {
            'group': {
                'name': ANY,
                'capacity': {
                    'minimum': 5,
                    'maximum': 10,
                    'target': 5
                },
                'compute': ANY,
                'scheduling': ANY
            }
        }

        self.elastigroup.spotinst_client.update_group.assert_called_once_with(group['id'], expected_request)

    @parameterized.expand([
        ("53%", 47, None),
        ("20", None, 20)
    ])
    def test_update_spotinst_reserve(self, spotinst_reserve, risk, on_demand_count):
        """Verifies updating risk of an existing group"""
        group = self.mock_elastigroup(hostclass='mhcfoo')
        self.elastigroup.spotinst_client.get_groups.return_value = [group]

        self.elastigroup.create_or_update_group(
            hostclass="mhcfoo",
            spotinst=True,
            spotinst_reserve=spotinst_reserve
        )

        expected_request = {
            'group': {
                'name': ANY,
                'capacity': ANY,
                'compute': ANY,
                'scheduling': ANY,
                'strategy': {
                    "risk": risk,
                    "onDemandCount": on_demand_count
                }
            }
        }

        self.elastigroup.spotinst_client.update_group.assert_called_once_with(group['id'], expected_request)

    def test_update_snapshot(self):
        """Verifies that snapshots for a Elastigroup are updated"""
        group = self.mock_elastigroup(hostclass='mhcfoo')
        self.elastigroup.spotinst_client.get_groups.return_value = [group]

        self.elastigroup.update_snapshot('snapshot-newsnapshotid', 100, hostclass='mhcfoo')

        expected_request = {
            'group': {
                'compute': {
                    'launchSpecification': {
                        'blockDeviceMappings': [{
                            "deviceName": "/dev/xvda",
                            "ebs": {
                                "deleteOnTermination": "true",
                                "volumeSize": 100,
                                "volumeType": "gp2",
                                "snapshotId": "snapshot-newsnapshotid"
                            }
                        }]
                    }
                }
            }
        }

        self.elastigroup.spotinst_client.update_group.assert_called_once_with(group['id'], expected_request)

    def test_update_elb(self):
        """Verifies ELBs for a Elastigroup are updated"""
        group = self.mock_elastigroup(hostclass='mhcfoo')
        self.elastigroup.spotinst_client.get_groups.return_value = [group]

        self.elastigroup.update_elb(['elb-newelb'], hostclass='mhcfoo')

        expected_request = {
            'group': {
                'compute': {
                    'launchSpecification': {
                        'loadBalancersConfig': {
                            'loadBalancers': [{
                                'name': 'elb-newelb',
                                'type': 'CLASSIC'
                            }]
                        }
                    }
                }
            }
        }

        self.elastigroup.spotinst_client.update_group.assert_called_once_with(group['id'], expected_request)

    def test_create_recurring_group_action(self):
        """Verifies recurring actions are created for Elastigroups"""
        group = self.mock_elastigroup(hostclass='mhcfoo')
        self.elastigroup.spotinst_client.get_groups.return_value = [group]

        self.elastigroup.create_recurring_group_action('0 0 * * *', min_size=1, hostclass='mhcfoo')

        expected_request = {
            'group': {
                'scheduling': {
                    'tasks': [{
                        'taskType': 'scale',
                        'cronExpression': '12 0 * * *',
                        'scaleMinCapcity': 5
                    }, {
                        'taskType': 'scale',
                        'cronExpression': '0 0 * * *',
                        'scaleMinCapcity': 1
                    }]
                }
            }
        }

        self.elastigroup.spotinst_client.update_group.assert_called_once_with(group['id'], expected_request)

    def test_delete_all_recurring_group_actions(self):
        """Verifies recurring actions are deleted for Elastigroups"""
        group = self.mock_elastigroup(hostclass='mhcfoo')
        self.elastigroup.spotinst_client.get_groups.return_value = [group]

        self.elastigroup.delete_all_recurring_group_actions(hostclass='mhcfoo')

        expected_request = {
            'group': {
                'scheduling': {
                    'tasks': []
                }
            }
        }

        self.elastigroup.spotinst_client.update_group.assert_called_once_with(group['id'], expected_request)

    def test_scaledown(self):
        """Verifies Elastigroups are scaled down"""
        group = self.mock_elastigroup(hostclass='mhcfoo')
        self.elastigroup.spotinst_client.get_groups.return_value = [group]

        self.elastigroup.scaledown_groups(hostclass='mhcfoo')

        expected_request = {
            "group": {
                "capacity": {
                    "target": 0,
                    "minimum": 0,
                    "maximum": 0
                }
            }
        }

        self.elastigroup.spotinst_client.update_group.assert_called_once_with(group['id'], expected_request)

    @parameterized.expand([
        ("53%", 47, None),
        ("20", None, 20),
        (None, 100, None)
    ])
    def test_spotinst_reserve(self, spotinst_reserve, risk, on_demand_count):
        """"Verifies spotinst_reserve handled correctly"""
        self.elastigroup.create_or_update_group(
            hostclass="mhcfoo",
            instance_type='m3.medium',
            spotinst_reserve=spotinst_reserve,
            spotinst=True
        )
        expected_request = {
            "group": {
                "compute": ANY,
                "capacity": ANY,
                'name': ANY,
                'description': ANY,
                "strategy": {
                    "utilizeReservedInstances": ANY,
                    "availabilityVsCost": ANY,
                    "risk": risk,
                    "onDemandCount": on_demand_count,
                    "fallbackToOd": ANY
                }
            }
        }

        self.elastigroup.spotinst_client.create_group.assert_called_once_with(expected_request)

    @patch('os.environ.get', MagicMock(return_value=None))
    def test_is_spotinst_not_enabled(self):
        """Verify that if no spotinst token is set, spotinst is not enabled"""
        self.elastigroup = DiscoElastigroup(ENVIRONMENT_NAME)

        self.assertFalse(self.elastigroup.is_spotinst_enabled())

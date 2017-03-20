"""
Tests of disco_elastigroup
"""
import random

from unittest import TestCase

import requests_mock
from mock import MagicMock
from disco_aws_automation import DiscoElastigroup

ENVIRONMENT_NAME = "moon"
ACCOUNT_ID = ''.join(random.choice("1234567890") for _ in range(10))
SPOTINST_API = 'https://api.spotinst.io/aws/ec2/group/'


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
                    "blockDeviceMappings": []
                },
                "availabilityZones": [{
                    "name": 'us-moon-1a',
                    "subnetId": "subnet-abcd1234"
                }]
            }
        }

        return mock_elastigroup

    def assert_request_made(self, requests, url, method):
        """Assert that a request was made to the given url and method"""
        filtered_items = [item for item in requests.request_history
                          if item.url == url and item.method == method]

        history_contains = len(filtered_items) > 0

        self.assertEqual(history_contains, True, "%s request was not made to %s" % (url, method))

    def setUp(self):
        """Pre-test setup"""
        self.elastigroup = DiscoElastigroup(
            ENVIRONMENT_NAME,
            account_id=ACCOUNT_ID
        )

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

    @requests_mock.Mocker()
    def test_list_groups_with_groups(self, requests):
        """Verifies that listgroups correctly formats elastigroups"""
        mock_group1 = self.mock_elastigroup(hostclass="mhcfoo")
        mock_group2 = self.mock_elastigroup(hostclass="mhcbar")

        requests.get(SPOTINST_API, json={
            'response': {
                'items': [mock_group1, mock_group2]
            }
        })

        self.elastigroup._get_group_instances = MagicMock(return_value=['instance1', 'instance2'])

        actual_listings = self.elastigroup.list_groups()
        mock_listings = [
            {
                'name': mock_group1['name'],
                'image_id': mock_group1['compute']['launchSpecification']['imageId'],
                'group_cnt': len(self.elastigroup._get_group_instances()),
                'min_size': mock_group1['capacity']['minimum'],
                'desired_capacity': mock_group1['capacity']['target'],
                'max_size': mock_group1['capacity']['maximum'],
                'type': 'spot'
            },
            {
                'name': mock_group2['name'],
                'image_id': mock_group2['compute']['launchSpecification']['imageId'],
                'group_cnt': len(self.elastigroup._get_group_instances()),
                'min_size': mock_group2['capacity']['minimum'],
                'desired_capacity': mock_group2['capacity']['target'],
                'max_size': mock_group2['capacity']['maximum'],
                'type': 'spot'
            }
        ]

        self.assertEqual(actual_listings, mock_listings)

    @requests_mock.Mocker()
    def test_create_new_group(self, requests):
        """Verifies new elastigroup is created"""
        self.elastigroup._create_az_subnets_dict = MagicMock()
        self.elastigroup._create_elastigroup_config = MagicMock(return_value=dict())
        self.elastigroup.get_existing_group = MagicMock(return_value=None)

        mock_response = {
            'response': {
                'items': [{
                    'name': 'mhcfoo'
                }]
            }
        }

        requests.post(SPOTINST_API, json=mock_response)
        requests.get(SPOTINST_API, json=mock_response)

        group = self.elastigroup.update_group(hostclass="mhcfoo", spotinst=True)

        self.assert_request_made(requests, SPOTINST_API, 'POST')
        self.assertEqual(group['name'], 'mhcfoo')

    def test_update_existing_group(self):
        """Verifies existing elastigroup is updated"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')
        mock_group_config = {
            "group": {
                "capacity": {
                    "unit": "instance"
                },
                "compute": {
                    "product": "Linux/UNIX"
                }
            }
        }
        self.elastigroup._create_az_subnets_dict = MagicMock()
        self.elastigroup._create_elastigroup_config = MagicMock(return_value=mock_group_config)
        self.elastigroup.get_existing_group = MagicMock(return_value=mock_group)
        self.elastigroup._spotinst_call = MagicMock()

        self.elastigroup.update_group(hostclass="mhcfoo", spotinst=True)

        self.elastigroup._spotinst_call.assert_called_once_with(path='/' + mock_group['id'],
                                                                data=mock_group_config, method='put')

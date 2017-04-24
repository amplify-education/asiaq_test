"""
Tests of disco_elastigroup
"""
import random
from collections import namedtuple

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

    def mock_spotinst_response(self, elastigroups):
        """Mock a response for spotinst that would be returned from /group requests"""
        return {
            'response': {
                'items': elastigroups
            }
        }

    def assert_request_made(self, requests, url, method, json=None):
        """Assert that a request was made to the given url and method"""
        filtered_items = [item for item in requests.request_history
                          if (item.url == url and item.method == method) and
                          (not json or item.json() == json)]

        history_contains = len(filtered_items) > 0

        self.assertTrue(history_contains, "%s %s request was not made with data %s" % (method, url, json))

    def is_similar_obj(self, dict1, dict2):
        """"Deep compare 2 dictionaries and return True if all keys and values in dict1 also exist in dict2"""
        if not isinstance(dict1, dict):
            return dict1 == dict2

        for key in dict1:
            if key not in dict2:
                return False

            if not isinstance(dict1[key], type(dict2[key])):
                return False

            if not self.is_similar_obj(dict1[key], dict2[key]):
                return False

        return True

    def assert_similar_request_made(self, requests, url, method, json):
        """Assert that a request was made to the given url/method with 'similar' data"""
        filtered_items = [item for item in requests.request_history
                          if (item.url == url and item.method == method) and
                          (not json or self.is_similar_obj(json, item.json()))]

        history_contains = len(filtered_items) > 0

        self.assertTrue(history_contains, "%s %s request not made with data like %s" % (method, url, json))

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

    @requests_mock.Mocker()
    def test_update_existing_group(self, requests):
        """Verifies existing elastigroup is updated"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')

        requests.get(SPOTINST_API, json={
            "response": {
                "items": [mock_group]
            }
        })
        requests.put(SPOTINST_API + mock_group['id'])
        requests.get(SPOTINST_API + mock_group['id'] + '/status', json={
            'response': {
                'items': []
            }
        })
        requests.put(SPOTINST_API + mock_group['id'] + '/roll', json={})

        self.elastigroup.update_group(
            hostclass="mhcfoo",
            spotinst=True,
            subnets=[{
                'SubnetId': 'sub-1234',
                'AvailabilityZone': 'us-moon-1'
            }],
            instance_type="m3.medium"
        )

        self.assert_request_made(requests, SPOTINST_API + mock_group['id'], 'PUT')

    @requests_mock.Mocker()
    def test_update_snapshot(self, requests):
        """Verifies that snapshots for a Elastigroup are updated"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')

        requests.get(SPOTINST_API, json={
            "response": {
                "items": [mock_group]
            }
        })

        requests.put(SPOTINST_API + mock_group['id'], json={})

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

        self.assert_request_made(requests, SPOTINST_API + mock_group['id'], 'PUT', json=expected_request)

    @requests_mock.Mocker()
    def test_update_elb(self, requests):
        """Verifies ELBs for a Elastigroup are updated"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')

        requests.get(SPOTINST_API, json={
            "response": {
                "items": [mock_group]
            }
        })
        requests.put(SPOTINST_API + mock_group['id'], json={})
        requests.get(SPOTINST_API + mock_group['id'] + '/status', json={
            'response': {
                'items': []
            }
        })
        requests.put(SPOTINST_API + mock_group['id'] + '/roll', json={})

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

        self.assert_request_made(requests, SPOTINST_API + mock_group['id'], 'PUT', json=expected_request)

    @requests_mock.Mocker()
    def test_create_recurring_group_action(self, requests):
        """Verifies recurring actions are created for Elastigroups"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')

        requests.get(SPOTINST_API, json={
            "response": {
                "items": [mock_group]
            }
        })

        requests.put(SPOTINST_API + mock_group['id'], json={})

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

        self.assert_request_made(requests, SPOTINST_API + mock_group['id'], 'PUT', json=expected_request)

    @requests_mock.Mocker()
    def test_delete_all_recurring_group_actions(self, requests):
        """Verifies recurring actions are deleted for Elastigroups"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')

        requests.get(SPOTINST_API, json={
            "response": {
                "items": [mock_group]
            }
        })

        requests.put(SPOTINST_API + mock_group['id'], json={})

        self.elastigroup.delete_all_recurring_group_actions(hostclass='mhcfoo')

        expected_request = {
            'group': {
                'scheduling': {
                    'tasks': []
                }
            }
        }

        self.assert_request_made(requests, SPOTINST_API + mock_group['id'], 'PUT', json=expected_request)

    @requests_mock.Mocker()
    def test_scaledown(self, requests):
        """Verifies Elastigroups are scaled down"""
        mock_group = self.mock_elastigroup(hostclass='mhcfoo')

        requests.get(SPOTINST_API, json={
            "response": {
                "items": [mock_group]
            }
        })

        requests.put(SPOTINST_API + mock_group['id'], json={})

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

        self.assert_request_made(requests, SPOTINST_API + mock_group['id'], 'PUT', json=expected_request)

    @requests_mock.Mocker()
    def test_persist_ebs(self, requests):
        """"Verifies EBS persistence option is enabled if EBS volumes exist"""

        self.elastigroup._create_az_subnets_dict = MagicMock()
        mock_response = {
            'response': {
                'items': [{
                    'name': 'mhcfoo'
                }]
            }
        }

        requests.post(SPOTINST_API, json=mock_response)
        requests.get(SPOTINST_API, json=mock_response)

        # silly pylint, namedtutples should use class naming convention
        # pylint: disable=C0103
        Ebs = namedtuple('EBS', ['size', 'iops', 'snapshot_id', 'delete_on_termination', 'volume_type'])

        self.elastigroup.update_group(
            hostclass="mhcfoo",
            block_device_mappings=[{
                "/dev/xvdb": Ebs(
                    size=100, iops=None, snapshot_id=None,
                    delete_on_termination=False, volume_type='io1'
                )
            }],
            instance_type='m3.medium',
            spotinst=True
        )

        expected_request = {
            "group": {
                "strategy": {
                    "persistence": {
                        "shouldPersistBlockDevices": True
                    }
                }
            }
        }

        self.assert_similar_request_made(requests, SPOTINST_API, 'POST', json=expected_request)

    @requests_mock.Mocker()
    def test_risk_as_percentage(self, requests):
        """"Verifies spot instance percentage is handled correctly"""
        self.elastigroup._create_az_subnets_dict = MagicMock()

        requests.get(SPOTINST_API, json=self.mock_spotinst_response([]))
        requests.post(SPOTINST_API, json=self.mock_spotinst_response([self.mock_elastigroup('mhcfoo')]))

        self.elastigroup.update_group(
            hostclass="mhcfoo",
            instance_type='m3.medium',
            spotinst_reserve="53%",
            spotinst=True
        )

        expected_request = {
            "group": {
                "strategy": {
                    "risk": 47
                }
            }
        }

        self.assert_similar_request_made(requests, SPOTINST_API, 'POST', json=expected_request)

    @requests_mock.Mocker()
    def test_risk_as_count(self, requests):
        """"Verifies that ondemand count is handled correctly"""
        self.elastigroup._create_az_subnets_dict = MagicMock()

        requests.get(SPOTINST_API, json=self.mock_spotinst_response([]))
        requests.post(SPOTINST_API, json=self.mock_spotinst_response([self.mock_elastigroup('mhcfoo')]))

        self.elastigroup.update_group(
            hostclass="mhcfoo",
            instance_type='m3.medium',
            spotinst_reserve="20",
            spotinst=True
        )

        expected_request = {
            "group": {
                "strategy": {
                    "onDemandCount": 20
                }
            }
        }

        self.assert_similar_request_made(requests, SPOTINST_API, 'POST', json=expected_request)

    @requests_mock.Mocker()
    def test_risk_default(self, requests):
        """"Verifies that elastigroups default to 100% spot instances"""
        self.elastigroup._create_az_subnets_dict = MagicMock()

        requests.get(SPOTINST_API, json=self.mock_spotinst_response([]))
        requests.post(SPOTINST_API, json=self.mock_spotinst_response([self.mock_elastigroup('mhcfoo')]))

        self.elastigroup.update_group(
            hostclass="mhcfoo",
            instance_type='m3.medium',
            spotinst=True
        )

        expected_request = {
            "group": {
                "strategy": {
                    "risk": 100
                }
            }
        }

        self.assert_similar_request_made(requests, SPOTINST_API, 'POST', json=expected_request)

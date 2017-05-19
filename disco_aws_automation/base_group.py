"""Base Group (abstract)"""
import logging

from abc import ABCMeta, abstractmethod

from botocore.exceptions import WaiterError
import boto3

from .resource_helper import throttled_call

logger = logging.getLogger(__name__)


class BaseGroup(object):
    """Abstract class definition for AWS groups"""
    __metaclass__ = ABCMeta

    def __init__(self):
        self._boto3_ec = None

    @abstractmethod
    def get_existing_group(self, hostclass, group_name, throw_on_two_groups):
        """Get list of group objects for a hostclass"""
        return

    @abstractmethod
    def get_existing_groups(self, hostclass, group_name):
        """Get list of all group objects for a hostclass"""
        return

    @abstractmethod
    def list_groups(self):
        """Returns list of objects for display purposes for all groups"""
        return

    @abstractmethod
    def get_instances(self, hostclass=None, group_name=None):
        """Get list of instances in groups"""
        return

    @abstractmethod
    def delete_groups(self, hostclass, group_name, force):
        """Delete groups of a hostclass"""
        return

    @abstractmethod
    def scaledown_groups(self, hostclass, group_name, wait, noerror):
        """Scale down number of instances in a group"""
        return

    @abstractmethod
    def terminate(self, instance_id, decrement_capacity=True):
        """
        Terminates an instance using the autoscaling API.

        When decrement_capacity is True this allows us to avoid
        autoscaling immediately replacing a terminated instance.
        """
        pass

    @abstractmethod
    def delete_all_recurring_group_actions(self, hostclass=None, group_name=None):
        """Deletes all recurring scheduled actions for a hostclass"""
        pass

    @abstractmethod
    def create_recurring_group_action(self, recurrance, min_size=None, desired_capacity=None, max_size=None,
                                      hostclass=None, group_name=None):
        """Creates a recurring scheduled action for a hostclass"""
        pass

    @abstractmethod
    def update_elb(self, elb_names, hostclass=None, group_name=None):
        """Updates an existing autoscaling group to use a different set of load balancers"""
        pass

    @abstractmethod
    def get_launch_config(self, hostclass=None, group_name=None):
        """Create new launchconfig group name"""
        pass

    # pylint: disable=R0913, R0914
    @abstractmethod
    def create_or_update_group(self, hostclass, desired_size=None, min_size=None, max_size=None,
                               instance_type=None, load_balancers=None, subnets=None, security_groups=None,
                               instance_monitoring=None, ebs_optimized=None, image_id=None, key_name=None,
                               associate_public_ip_address=None, user_data=None, tags=None,
                               instance_profile_name=None, block_device_mappings=None, group_name=None,
                               create_if_exists=False, termination_policies=None, spotinst=False,
                               spotinst_reserve=None, roll_if_needed=False):
        """
        Create a new autoscaling group or update an existing one
        """
        pass

    @abstractmethod
    def clean_configs(self):
        """Delete unused Launch Configurations in current environment"""
        pass

    @abstractmethod
    def get_configs(self, names=None):
        """Returns Launch Configurations in current environment"""
        pass

    @abstractmethod
    def delete_config(self, config_name):
        """Delete a specific Launch Configuration"""
        pass

    @abstractmethod
    def list_policies(self, group_name=None, policy_types=None, policy_names=None):
        """Returns all autoscaling policies"""
        pass

    @abstractmethod
    def create_policy(self, group_name, policy_name, policy_type="SimpleScaling", adjustment_type=None,
                      min_adjustment_magnitude=None, scaling_adjustment=None, cooldown=600,
                      metric_aggregation_type=None, step_adjustments=None, estimated_instance_warmup=None):
        """
        Creates a new autoscaling policy, or updates an existing one if the autoscaling group name and
        policy name already exist. Handles the logic of constructing the correct autoscaling policy request,
        because not all parameters are required.
        """
        pass

    @abstractmethod
    def delete_policy(self, policy_name, group_name):
        """Deletes an autoscaling policy"""
        pass

    @abstractmethod
    def update_snapshot(self, snapshot_id, snapshot_size, hostclass=None, group_name=None):
        """Updates all of a hostclasses existing autoscaling groups to use a different snapshot"""
        pass

    @property
    def boto3_ec(self):
        """Lazily create boto3 ec2 connection"""
        if not self._boto3_ec:
            self._boto3_ec = boto3.client('ec2')
        return self._boto3_ec

    def wait_instance_termination(self, group_name=None, group=None, noerror=False):
        """Wait for instance to be terminated during scaledown"""
        instance_ids = [inst['instance_id'] for inst in self.get_instances(group_name=group_name)]

        # don't wait if there are no instances to wait for
        if not instance_ids:
            return True

        try:
            logger.info("Waiting for scaledown of group %s, instances %s", group['name'], instance_ids)
            throttled_call(self.boto3_ec.get_waiter('instance_terminated').wait, InstanceIds=instance_ids)
        except WaiterError:
            if noerror:
                logger.exception("Unable to wait for scaling down of %s", group_name)
                return False
            else:
                raise

        return True

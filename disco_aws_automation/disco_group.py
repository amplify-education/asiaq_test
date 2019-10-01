"""Contains DiscoGroup class that is above all other group classes used"""
import logging

from .base_group import BaseGroup
from .disco_autoscale import DiscoAutoscale
from .disco_elastigroup import DiscoElastigroup

logger = logging.getLogger(__name__)


class DiscoGroup(BaseGroup):
    """Implementation of DiscoGroup regardless of the type of group"""

    def __init__(self, environment_name):
        """Implementation of BaseGroup in AWS"""
        self.environment_name = environment_name
        self.autoscale = DiscoAutoscale(environment_name=self.environment_name)
        self.elastigroup = DiscoElastigroup(environment_name=self.environment_name)
        super(DiscoGroup, self).__init__()

    def get_existing_group(self, hostclass=None, group_name=None, throw_on_two_groups=True):
        asg_group = self.autoscale.get_existing_group(
            hostclass=hostclass,
            group_name=group_name,
            throw_on_two_groups=throw_on_two_groups
        )
        spot_group = self._safe_elastigroup_call(
            self.elastigroup.get_existing_group,
            default=[],
            hostclass=hostclass,
            group_name=group_name,
            throw_on_two_groups=throw_on_two_groups
        )

        if asg_group and spot_group:
            return sorted([asg_group, spot_group], key=lambda grp: grp['name'], reverse=True)[0]
        elif asg_group:
            return asg_group
        elif spot_group:
            return spot_group
        else:
            logger.info('No group found for hostclass %s and group name %s ', hostclass, group_name)

    def _safe_elastigroup_call(self, fun, default=None, *args, **kwargs):
        """
        Call a function in DiscoElastiGroup and handle cases when SpotInst usage is disabled
        Args:
            fun (function): The function to call
            default:  Default value to return in case SpotInst usage is disabled
        """
        if not self.elastigroup.is_spotinst_enabled():
            return default

        return fun(*args, **kwargs)

    def get_existing_groups(self, hostclass=None, group_name=None):
        asg_groups = self.autoscale.get_existing_groups()
        spot_groups = self._safe_elastigroup_call(self.elastigroup.get_existing_groups, default=[])

        return asg_groups + spot_groups

    def list_groups(self):
        """Returns list of objects for display purposes for all groups"""
        asg_groups = self.autoscale.list_groups()
        spot_groups = self._safe_elastigroup_call(self.elastigroup.list_groups, default=[])

        groups = asg_groups + spot_groups
        groups.sort(key=lambda grp: grp['name'])
        return groups

    def get_instances(self, hostclass=None, group_name=None):
        asg_instances = self.autoscale.get_instances(hostclass=hostclass, group_name=group_name)
        spot_instances = self._safe_elastigroup_call(
            self.elastigroup.get_instances,
            default=[],
            hostclass=hostclass,
            group_name=group_name
        )

        return asg_instances + spot_instances

    def delete_groups(self, hostclass=None, group_name=None, force=False):
        self.autoscale.delete_groups(hostclass=hostclass, group_name=group_name, force=force)
        self._safe_elastigroup_call(
            self.elastigroup.delete_groups,
            hostclass=hostclass,
            group_name=group_name,
            force=force
        )

    def scaledown_groups(self, hostclass=None, group_name=None, wait=False, noerror=False):
        self.autoscale.scaledown_groups(
            hostclass=hostclass,
            group_name=group_name,
            wait=wait,
            noerror=noerror
        )

        self._safe_elastigroup_call(
            self.elastigroup.scaledown_groups,
            hostclass=hostclass,
            group_name=group_name,
            wait=wait,
            noerror=noerror
        )

    def terminate(self, instance_id, decrement_capacity=True):
        """
        Terminates an instance using the autoscaling API.

        When decrement_capacity is True this allows us to avoid
        autoscaling immediately replacing a terminated instance.
        """
        # todo check if instance belongs to spotinst or ASG and decrement the correct group
        self.autoscale.terminate(instance_id, decrement_capacity)

    def delete_all_recurring_group_actions(self, hostclass=None, group_name=None):
        """Deletes all recurring scheduled actions for a hostclass"""
        self.autoscale.delete_all_recurring_group_actions(hostclass, group_name)
        self._safe_elastigroup_call(
            self.elastigroup.delete_all_recurring_group_actions,
            hostclass,
            group_name
        )

    def create_recurring_group_action(self, recurrance, min_size=None, desired_capacity=None, max_size=None,
                                      hostclass=None, group_name=None):
        """Creates a recurring scheduled action for a hostclass"""
        self._service_call_for_group(
            'create_recurring_group_action',
            _hostclass=hostclass,
            _group_name=group_name,
            recurrance=recurrance,
            min_size=min_size,
            desired_capacity=desired_capacity,
            max_size=max_size,
            hostclass=hostclass,
            group_name=group_name
        )

    def update_elb(self, elb_names, hostclass=None, group_name=None):
        """Updates an existing autoscaling group to use a different set of load balancers"""
        self._service_call_for_group(
            'update_elb',
            _hostclass=hostclass,
            _group_name=group_name,
            elb_names=elb_names,
            hostclass=hostclass,
            group_name=group_name
        )

    def get_launch_config(self, hostclass=None, group_name=None):
        """Return launch config info for a hostclass, None otherwise"""
        return self._service_call_for_group(
            'get_launch_config',
            _hostclass=hostclass,
            _group_name=group_name,
            hostclass=hostclass,
            group_name=group_name
        )

    # pylint: disable=R0913, R0914
    def create_or_update_group(self, hostclass, desired_size=None, min_size=None, max_size=None,
                               instance_type=None, load_balancers=None, target_groups=None, subnets=None,
                               security_groups=None, instance_monitoring=None, ebs_optimized=None,
                               image_id=None, key_name=None, associate_public_ip_address=None, user_data=None,
                               tags=None, instance_profile_name=None, block_device_mappings=None,
                               group_name=None, create_if_exists=False, termination_policies=None,
                               spotinst=False, spotinst_reserve=None):
        """
        Create a new autoscaling group or update an existing one
        """
        existing_group = self.get_existing_group(hostclass, group_name)

        # if there is an existing group and create_if_exists is false then we will be updating that group
        # in that case, use the group type of the existing group instead of the passed in type
        if existing_group and not create_if_exists:
            existing_spot = existing_group['type'] == 'spot'
            if existing_spot != spotinst:
                logger.info(
                    'Running update_group using %s group type because existing group type is %s',
                    existing_group['type'], existing_group['type']
                )
                spotinst = existing_spot

        return self._service_call(
            spotinst, 'create_or_update_group',
            hostclass=hostclass,
            desired_size=desired_size,
            min_size=min_size,
            max_size=max_size,
            instance_type=instance_type,
            load_balancers=load_balancers,
            target_groups=target_groups,
            subnets=subnets,
            security_groups=security_groups,
            instance_monitoring=instance_monitoring,
            ebs_optimized=ebs_optimized,
            image_id=image_id,
            key_name=key_name,
            associate_public_ip_address=associate_public_ip_address,
            user_data=user_data,
            tags=tags,
            instance_profile_name=instance_profile_name,
            block_device_mappings=block_device_mappings,
            group_name=group_name,
            create_if_exists=create_if_exists,
            termination_policies=termination_policies,
            spotinst=spotinst,
            spotinst_reserve=spotinst_reserve
        )

    def clean_configs(self):
        """Delete unused Launch Configurations in current environment"""
        self.autoscale.clean_configs()

    def get_configs(self, names=None):
        """Returns Launch Configurations in current environment"""
        return self.autoscale.get_configs(names)

    def delete_config(self, config_name):
        """Delete a specific Launch Configuration"""
        self.autoscale.delete_config(config_name)

    def list_policies(self, group_name=None, policy_types=None, policy_names=None):
        """Returns all autoscaling policies"""
        return self.autoscale.list_policies(group_name, policy_types, policy_names)

    def create_policy(self, group_name, policy_name, policy_type="SimpleScaling", adjustment_type=None,
                      min_adjustment_magnitude=None, scaling_adjustment=None, cooldown=600,
                      metric_aggregation_type=None, step_adjustments=None, estimated_instance_warmup=None):
        """
        Creates a new autoscaling policy, or updates an existing one if the autoscaling group name and
        policy name already exist. Handles the logic of constructing the correct autoscaling policy request,
        because not all parameters are required.
        """
        self._service_call_for_group(
            'create_policy',
            _group_name=group_name,
            group_name=group_name,
            policy_name=policy_name,
            policy_type=policy_type,
            adjustment_type=adjustment_type,
            min_adjustment_magnitude=min_adjustment_magnitude,
            scaling_adjustment=scaling_adjustment,
            cooldown=cooldown,
            metric_aggregation_type=metric_aggregation_type,
            step_adjustments=step_adjustments,
            estimated_instance_warmup=estimated_instance_warmup
        )

    def delete_policy(self, policy_name, group_name):
        """Deletes an autoscaling policy"""
        self._service_call_for_group(
            'delete_policy',
            _group_name=group_name,
            policy_name=policy_name,
            group_name=group_name
        )

    def update_snapshot(self, snapshot_id, snapshot_size, hostclass=None, group_name=None):
        """Updates all of a hostclasses existing autoscaling groups to use a different snapshot"""
        self._service_call_for_group(
            'update_snapshot',
            _hostclass=hostclass,
            _group_name=group_name,
            hostclass=hostclass,
            group_name=group_name,
            snapshot_id=snapshot_id,
            snapshot_size=snapshot_size
        )

    def _service_call(self, use_spotinst, fun_name, *args, **kwargs):
        """
        Make a call to either DiscoAutoscale or DiscoElastigroup
        Args:
            use_spotinst (bool): Use DiscoElastiGroup if True
            fun_name (str): Function name to call for the selected service
            default: Default value to use when calling DiscoElastigroup in case SpotInst is disabled
        """
        fun = getattr(self.elastigroup, fun_name) if use_spotinst else getattr(self.autoscale, fun_name)
        return fun(*args, **kwargs)

    def _service_call_for_group(self, fun_name, _hostclass=None, _group_name=None, *args,
                                **kwargs):
        """
        Make a call to either DiscoAutoscale or DiscoElastigroup based on the type of group affected
        Defaults to using DiscoAutoscale if the group is not found
        Args:
            fun_name (str): Function to call on the selected service
            _hostclass (str): Hostclass name to find group by
            _group_name (str): ASG or Elastigroup name to find group by
        """
        existing_group = self.get_existing_group(_hostclass, _group_name)
        use_spotinst = existing_group and existing_group['type'] == 'spot'

        return self._service_call(use_spotinst, fun_name, *args, **kwargs)

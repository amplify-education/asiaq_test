"""
Amazon VPC (Virtual Private Cloud) orchestration code.  We use VPC's to provide isolation between
environments, and between an environment and the internet.  In particular non-VPC instances
(EC2-Classic) have internet routable addresses which is not what we want.
"""

import logging
import random

from ConfigParser import ConfigParser

from boto.exception import EC2ResponseError
import boto3

from netaddr import IPNetwork, IPSet

from disco_aws_automation.network_helper import calc_subnet_offset
from . import normalize_path

from .resource_helper import tag2dict
from .disco_log_metrics import DiscoLogMetrics
from .disco_alarm import DiscoAlarm
from .disco_alarm_config import DiscoAlarmsConfig
from .disco_autoscale import DiscoAutoscale
from .disco_constants import CREDENTIAL_BUCKET_TEMPLATE, NETWORKS
from .disco_metanetwork import DiscoMetaNetwork
from .disco_vpc_sg_rules import DiscoVPCSecurityGroupRules
from .disco_vpc_gateways import DiscoVPCGateways
from .disco_vpc_peerings import DiscoVPCPeerings
from .disco_elasticache import DiscoElastiCache
from .disco_sns import DiscoSNS
from .disco_rds import DiscoRDS
from .disco_elb import DiscoELB
from .exceptions import (
    MultipleVPCsForVPCNameError, VPCConfigError, VPCEnvironmentError,
    VPCNameNotFound)


CONFIG_FILE = "disco_vpc.ini"


# FIXME: pylint thinks the file has too many instance arguments
# pylint: disable=R0902
class DiscoVPC(object):
    """
    This class contains all our VPC orchestration code
    """

    def __init__(self, environment_name, environment_type, vpc=None, config_file=None, boto3_ec2=None):
        self.config_file = config_file or CONFIG_FILE

        self.environment_name = environment_name
        self.environment_type = environment_type
        self._config = None  # lazily initialized
        self._region = None  # lazily initialized
        self._networks = None  # lazily initialized
        self._alarms_config = None  # lazily initialized

        if boto3_ec2:
            self.boto3_ec2 = boto3_ec2
        else:
            self.boto3_ec2 = boto3.client('ec2')

        self.rds = DiscoRDS(vpc=self)
        self.elb = DiscoELB(vpc=self)
        self.disco_vpc_sg_rules = DiscoVPCSecurityGroupRules(vpc=self, boto3_ec2=self.boto3_ec2)
        self.disco_vpc_gateways = DiscoVPCGateways(vpc=self, boto3_ec2=self.boto3_ec2)
        self.disco_vpc_peerings = DiscoVPCPeerings(vpc=self, boto3_ec2=self.boto3_ec2)
        self.elasticache = DiscoElastiCache(vpc=self)
        self.log_metrics = DiscoLogMetrics(environment=environment_name)

        if "_" in environment_name:  # Underscores break our alarm name parsing.
            raise VPCConfigError(
                "VPC name {0} must not contain an underscore".format(environment_name))

        if vpc:
            self.vpc = vpc
        else:
            self._create_environment()

    @property
    def config(self):
        """lazy load config"""
        if not self._config:
            try:
                config = ConfigParser()
                config.read(normalize_path(self.config_file))
                self._config = config
            except Exception:
                return None
        return self._config

    def get_config(self, option, default=None):
        '''Returns appropriate configuration for the current environment'''
        env_section = "env:{0}".format(self.environment_name)
        envtype_section = "envtype:{0}".format(self.environment_type)
        peering_section = "peerings"
        if self.config.has_option(env_section, option):
            return self.config.get(env_section, option)
        elif self.config.has_option(envtype_section, option):
            return self.config.get(envtype_section, option)
        elif self.config.has_option(peering_section, option):
            return self.config.get(peering_section, option)
        else:
            return default

    def get_vpc_id(self):
        ''' Returns the vpc id '''
        return self.vpc['VpcId'] if self.vpc else None

    def ami_stage(self):
        '''Returns default AMI stage to deploy in a development environment'''
        return self.get_config("ami_stage")

    @staticmethod
    def get_credential_buckets_from_env_name(aws_config, environment_name):
        """Return the credentials S3 bucket names for this environment"""

        env_name = environment_name or aws_config.get("disco_aws", "default_environment")
        if not env_name:
            raise VPCEnvironmentError(
                "Can not determine credentials bucket name, need to know environment name"
            )

        project_name = aws_config.get("disco_aws", "project_name")
        if not env_name:
            raise VPCEnvironmentError(
                "Can not determine credentials bucket name, need to know project name"
            )

        vpc = DiscoVPC.fetch_environment(environment_name=env_name)
        if not vpc:
            raise VPCEnvironmentError(
                "Can not determine credentials from environment name unless vpc exists"
            )

        return vpc.get_credential_buckets(project_name)

    @property
    def region(self):
        """Region we're operating in"""
        if not self._region:
            self._region = self.boto3_ec2.describe_availability_zones()['AvailabilityZones'][0]['RegionName']
        return self._region

    @property
    def alarms_config(self):
        """The configuration for metrics and alarms"""
        if not self._alarms_config:
            self._alarms_config = DiscoAlarmsConfig(self.environment_name)
        return self._alarms_config

    def get_credential_buckets(self, project_name):
        """Returns list of buckets to locate credentials in"""
        return [CREDENTIAL_BUCKET_TEMPLATE.format(region=self.region, project=project_name, postfix=postfix)
                for postfix in self.get_config("credential_buckets", "").split()]

    @classmethod
    def fetch_environment(cls, vpc_id=None, environment_name=None):
        """
        Returns an instance of this class for the specified VPC, or None if it does not exist
        """
        client = boto3.client('ec2')
        try:
            if vpc_id:
                vpc = client.describe_vpcs(
                    Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}])['Vpcs'][0]
            elif environment_name:
                vpc = client.describe_vpcs(
                    Filters=[{'Name': 'tag:Name', 'Values': [environment_name]}])['Vpcs'][0]
            else:
                raise VPCEnvironmentError("Expect vpc_id or environment_name")
        except IndexError:
            return None

        tags = tag2dict(vpc['Tags'] if 'Tags' in vpc else None)
        return cls(tags.get("Name", '-'), tags.get("type", '-'), vpc)

    @property
    def networks(self):
        """A dictionary containing each metanetwork name with its DiscoMetaNetwork class"""
        if self._networks:
            return self._networks
        self._networks = {
            network: DiscoMetaNetwork(network, self)
            for network in NETWORKS.keys()
            if self.get_config("{0}_cidr".format(network))  # don't create networks we haven't defined
        }
        return self._networks

    def _create_new_meta_networks(self):
        """Read the VPC config and create the DiscoMetaNetwork objects that should exist in a new VPC"""

        # don't create networks we haven't defined
        # a map of network names to the configured cidr value or "auto"
        networks = {network: self.get_config("{0}_cidr".format(network))
                    for network in NETWORKS.keys()
                    if self.get_config("{0}_cidr".format(network))}

        if len(networks) < 1:
            raise VPCConfigError('No Metanetworks configured for VPC %s' % self.environment_name)

        # calculate the extra cidr bits needed to represent the networks
        # for example breaking a /20 VPC into 4 meta networks will create /22 sized networks
        cidr_offset = calc_subnet_offset(len(networks))
        vpc_size = IPNetwork(self.vpc['CidrBlock']).prefixlen
        meta_network_size = vpc_size + cidr_offset

        # /32 is the smallest possible network
        if meta_network_size > 32:
            raise VPCConfigError('Unable to create %s metanetworks in /%s size VPC'
                                 % (len(networks), vpc_size))

        # keep a list of the cidrs used by the meta networks in case we need to pick a random one
        used_cidrs = [cidr for cidr in networks.values() if cidr != 'auto']

        metanetworks = {}
        for network_name, cidr in networks.iteritems():
            # pick a random ip range if there isn't one configured for the network in the config
            if cidr == 'auto':
                cidr = DiscoVPC.get_random_free_subnet(self.vpc['CidrBlock'], meta_network_size, used_cidrs)

                if not cidr:
                    raise VPCConfigError("Can't create metanetwork %s. No subnets available", network_name)

            metanetworks[network_name] = DiscoMetaNetwork(network_name, self, cidr)
            used_cidrs.append(cidr)

        return metanetworks

    def find_instance_route_table(self, instance):
        """ Return route tables corresponding to instance """
        rt_filter = []
        rt_filter.append(self.vpc_filter())
        rt_filter.append({"Name": "route.instance-id", "Values": [instance.id]})
        return self.boto3_ec2.describe_route_tables(Filters=rt_filter)['RouteTables']

    def delete_instance_routes(self, instance):
        """ Delete all routes associated with instance """
        route_tables = self.find_instance_route_table(instance)
        for route_table in route_tables:
            for route in route_table.routes:
                if route.instance_id == instance.id:
                    self.boto3_ec2.delete_route(
                        RouteTableId=route_table.id,
                        DestinationCidrBlock=route.destination_cidr_block)

    def _configure_dhcp(self):
        internal_dns = self.get_config("internal_dns")
        external_dns = self.get_config("external_dns")
        domain_name = self.get_config("domain_name")

        ntp_server = self.get_config("ntp_server")
        if not ntp_server:
            ntp_server_metanetwork = self.get_config("ntp_server_metanetwork")
            ntp_server_offset = self.get_config("ntp_server_offset")
            ntp_server = self.networks[ntp_server_metanetwork].ip_by_offset(ntp_server_offset)

        # internal_dns server should be default, and for this reason it comes last.
        dhcp_configs = []
        dhcp_configs.append({"Key": "domain-name", "Values": [domain_name]})
        dhcp_configs.append({"Key": "domain-name-servers", "Values": [internal_dns, external_dns]})
        dhcp_configs.append({"Key": "ntp-servers", "Values": [ntp_server]})

        response = self.boto3_ec2.create_dhcp_options(DhcpConfigurations=dhcp_configs)
        ec2 = boto3.resource('ec2')
        dhcp_options = ec2.DhcpOptions(response['DhcpOptions']['DhcpOptionsId'])
        dhcp_options.create_tags(Tags=[{'Key': 'Name', 'Value': self.environment_name}])
        return self.boto3_ec2.describe_dhcp_options(
            DhcpOptionsIds=[response['DhcpOptions']['DhcpOptionsId']]
        )['DhcpOptions']

    def _create_environment(self):

        """Create a new disco style environment VPC"""
        vpc_cidr = self.get_config("vpc_cidr")

        # if a vpc_cidr is not configured then allocate one dynamically
        if not vpc_cidr:
            ip_space = self.get_config("ip_space")
            vpc_size = self.get_config("vpc_cidr_size")

            if not ip_space and vpc_size:
                raise VPCConfigError('Cannot create VPC %s. ip_space or vpc_cidr_size missing'
                                     % self.environment_name)

            # get the cidr for all other VPCs so we can avoid overlapping with other VPCs
            occupied_network_cidrs = [vpc['cidr_block'] for vpc in self.list_vpcs()]

            vpc_cidr = DiscoVPC.get_random_free_subnet(ip_space, int(vpc_size), occupied_network_cidrs)

            if vpc_cidr is None:
                raise VPCConfigError('Cannot create VPC %s. No subnets available' % self.environment_name)

        # Create VPC
        self.vpc = self.boto3_ec2.create_vpc(CidrBlock=str(vpc_cidr))['Vpc']
        waiter = self.boto3_ec2.get_waiter('vpc_available')
        waiter.wait(VpcIds=[self.vpc['VpcId']])
        ec2 = boto3.resource('ec2')
        vpc = ec2.Vpc(self.vpc['VpcId'])
        tags = vpc.create_tags(Tags=[{'Key': 'Name', 'Value': self.environment_name},
                                     {'Key': 'type', 'Value': self.environment_type}])
        logging.debug("vpc: %s", self.vpc)
        logging.debug("vpc tags: %s", tags)

        dhcp_options = self._configure_dhcp()[0]
        self.boto3_ec2.associate_dhcp_options(DhcpOptionsId=dhcp_options['DhcpOptionsId'],
                                              VpcId=self.vpc['VpcId'])

        # Enable DNS
        self.boto3_ec2.modify_vpc_attribute(
            VpcId=self.vpc['VpcId'], EnableDnsSupport={'Value': True})
        self.boto3_ec2.modify_vpc_attribute(
            VpcId=self.vpc['VpcId'], EnableDnsHostnames={'Value': True})

        # Create metanetworks (subnets, route_tables and security groups)
        self._networks = self._create_new_meta_networks()
        for network in self.networks.values():
            network.create()

        # Configure security group rules for all meta networks
        self.disco_vpc_sg_rules.add_meta_network_sg_rules()

        # Setup internet gateway and VPN gateway
        self.disco_vpc_gateways.set_up_gateways()

        # Setup NAT gateways
        self.disco_vpc_gateways.set_up_nat_gateways()

        self.configure_notifications()

        DiscoVPCPeerings.create_peering_connections(
            self.disco_vpc_peerings.parse_peerings_config(self.get_vpc_id()))

        self.rds.update_all_clusters_in_vpc()

    def configure_notifications(self, dry_run=False):
        """
        Configure SNS topics for CloudWatch alarms.
        Note that topics are not deleted with the VPC, since that would require re-subscribing the members.
        """
        notifications = self.alarms_config.get_notifications()
        logging.info("Desired alarms config: %s", notifications)
        if not dry_run:
            DiscoSNS().update_sns_with_notifications(notifications, self.environment_name)

    def assign_eip(self, instance, eip_address, allow_reassociation=False):
        """
        Assign EIP to an instance
        """
        eip = self.boto3_ec2.describe_addresses(PublicIps=[eip_address])['Addresses'][0]
        try:
            self.boto3_ec2.associate_address(
                InstanceId=instance.id,
                AllocationId=eip['AllocationId'],
                AllowReassociation=allow_reassociation
            )
        except EC2ResponseError:
            logging.exception("Skipping failed EIP association. Perhaps reassociation of EIP is not allowed?")

    def vpc_filter(self):
        """Filter used to get only the current VPC when filtering an AWS reply by 'vpc-id'"""
        return {"Name": "vpc-id", "Values": [self.vpc['VpcId']]}

    def update(self, dry_run=False):
        """ Update the existing VPC """
        # Ignoring changes in CIDR for now at least

        logging.info("Updating security group rules...")
        self.disco_vpc_sg_rules.update_meta_network_sg_rules(dry_run)
        logging.info("Updating gateway routes...")
        self.disco_vpc_gateways.update_gateway_routes(dry_run)
        logging.info("Updating NAT gateways and routes...")
        self.disco_vpc_gateways.update_nat_gateways_and_routes(dry_run)
        logging.info("Updating VPC peering connections...")
        self.disco_vpc_peerings.update_peering_connections(dry_run)
        logging.info("Updating alarms...")
        self.configure_notifications(dry_run)

    def destroy(self):
        """ Delete all VPC resources in the right order and then delete the vpc itself """
        DiscoAlarm().delete_environment_alarms(self.environment_name)
        self.log_metrics.delete_all_metrics()
        self.log_metrics.delete_all_log_groups()
        self._destroy_instances()
        self.elb.destroy_all_elbs()
        self._destroy_rds()
        self.elasticache.delete_all_cache_clusters(wait=True)
        self.elasticache.delete_all_subnet_groups()
        self._destroy_interfaces()

        self.disco_vpc_sg_rules.destroy()
        self.disco_vpc_gateways.destroy_all()
        DiscoVPCPeerings.delete_peerings(self.get_vpc_id())
        self._destroy_subnets()
        self._destroy_routes()
        return self._destroy_vpc()

    def _destroy_instances(self):
        """ Find all instances in vpc and terminate them """
        autoscale = DiscoAutoscale(environment_name=self.environment_name)
        autoscale.clean_groups(force=True)
        instances = [i['InstanceId']
                     for r in self.boto3_ec2.describe_instances(Filters=[self.vpc_filter()])['Reservations']
                     for i in r['Instances']]

        if not instances:
            logging.debug("No running instances")
            return
        logging.debug("terminating %s instance(s) %s", len(instances), instances)

        # for instance in instances:
        #    instance.terminate()
        # for instance in instances:
        #    wait_for_state(instance, u'terminated')

        self.boto3_ec2.terminate_instances(InstanceIds=instances)
        waiter = self.boto3_ec2.get_waiter('instance_terminated')
        waiter.wait(InstanceIds=instances,
                    Filters=[{'Name': 'instance-state-name', 'Values': ['terminated']}])
        autoscale.clean_configs()

        logging.debug("waiting for instance shutdown scripts")

    def _destroy_rds(self, wait=True):
        """ Delete all RDS instances/clusters. Final snapshots are automatically taken. """
        self.rds.delete_all_db_instances(wait=wait)

    def _destroy_interfaces(self):
        """ Deleting interfaces explicitly lets go of subnets faster """
        for interface in self.boto3_ec2.describe_network_interfaces(
                Filters=[self.vpc_filter()])["NetworkInterfaces"]:
            try:
                self.boto3_ec2.delete_network_interface(NetworkInterfaceId=interface['NetworkInterfaceId'])
            except EC2ResponseError:
                # Occasionally we get InvalidNetworkInterfaceID.NotFound, not sure why.
                logging.exception("Skipping error deleting network.")

    def _destroy_subnets(self):
        """ Find all subnets belonging to a vpc and destroy them"""
        for subnet in self.boto3_ec2.describe_subnets(Filters=[self.vpc_filter()])['Subnets']:
            self.boto3_ec2.delete_subnet(SubnetId=subnet['SubnetId'])

    def _destroy_routes(self):
        """ Find all route_tables belonging to vpc and destroy them"""
        for route_table in self.boto3_ec2.describe_route_tables(Filters=[self.vpc_filter()])['RouteTables']:
            if len(route_table['Tags']) < 1:
                logging.info("Skipping untagged (default) route table %s", route_table['RouteTableId'])
                continue
            try:
                self.boto3_ec2.delete_route_table(RouteTableId=route_table['RouteTableId'])
            except EC2ResponseError:
                logging.error("Error deleting route_table %s:.", route_table['RouteTableId'])
                raise

    def _destroy_vpc(self):
        """Delete VPC and then delete the dhcp_options that were associated with it. """

        # save function and parameters so we can delete dhcp_options after vpc. We do this becase botos
        # get_all_dhcp_options does not support filter. Because we cannot easily find the default dhcp
        # options, re-assigning default dhcp option is not trivial.
        # delete_dhcp_options = self.vpc.connection.delete_dhcp_options
        dhcp_options_id = self.vpc['DhcpOptionsId']

        self.boto3_ec2.delete_vpc(VpcId=self.get_vpc_id())
        # delete_status = keep_trying(30, self.vpc.delete)
        self.vpc = None

        self.boto3_ec2.delete_dhcp_options(DhcpOptionsId=dhcp_options_id)
        # if not delete_dhcp_options(dhcp_options_id):
        #    logging.warning("failed to delete dhcp options (%s)", dhcp_options_id)

        # return delete_status

    @staticmethod
    def find_vpc_id_by_name(vpc_name):
        """Find VPC by name"""
        client = boto3.client('ec2')
        vpcs = client.describe_vpcs(Filters=[{'Name': 'tag:Name', 'Values': [vpc_name]}])['Vpcs']
        if len(vpcs) == 1:
            return vpcs[0]['VpcId']
        elif len(vpcs) == 0:
            raise VPCNameNotFound("No VPC is named as {}".format(vpc_name))
        else:
            raise MultipleVPCsForVPCNameError("More than 1 VPC is named as {}".format(vpc_name))

    @staticmethod
    def list_vpcs():
        """Returns list of boto.vpc.vpc.VPC classes, one for each existing VPC"""
        client = boto3.client('ec2')
        vpcs = client.describe_vpcs()
        return [{'id': vpc['VpcId'],
                 'tags': tag2dict(vpc['Tags'] if 'Tags' in vpc else None),
                 'cidr_block': vpc['CidrBlock']}
                for vpc in vpcs['Vpcs']]

    @staticmethod
    def get_random_free_subnet(network_cidr, network_size, occupied_network_cidrs):
        """
        Pick a random available subnet from a bigger network
        Args:
            network_cidr (str): CIDR string describing a network
            network_size (int): The number of bits for the CIDR of the subnet
            occupied_network_cidrs (List[str]): List of CIDR strings describing existing networks
                                                to avoid overlapping with

        Returns str: The CIDR of a randomly chosen subnet that doesn't intersect with
                     the ip ranges of any of the given other networks
        """
        possible_subnets = IPNetwork(network_cidr).subnet(int(network_size))
        occupied_networks = [IPSet(IPNetwork(cidr)) for cidr in occupied_network_cidrs]

        # find the subnets that don't overlap with any other networks
        available_subnets = [subnet for subnet in possible_subnets
                             if all([IPSet(subnet).isdisjoint(occupied_network)
                                     for occupied_network in occupied_networks])]

        return random.choice(available_subnets) if available_subnets else None

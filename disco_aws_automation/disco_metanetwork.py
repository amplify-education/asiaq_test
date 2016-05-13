"""
Network abstraction
"""

import logging
from random import choice

from netaddr import IPNetwork, IPAddress
from boto.ec2.networkinterface import (
    NetworkInterfaceSpecification,
    NetworkInterfaceCollection
)
from boto.exception import EC2ResponseError

from disco_aws_automation.network_helper import calc_subnet_offset
from .disco_subnet import DiscoSubnet
from .resource_helper import (
    keep_trying,
    find_or_create
)
from .disco_constants import NETWORKS
from .exceptions import (
    IPRangeError,
    EIPConfigError,
    RouteCreationError
)


class DiscoMetaNetwork(object):
    """
    Representation of a disco meta-network. Contains a subnet for each availability zone,
    along with a route table which is applied all the subnets.
    """
    def __init__(self, name, vpc, network_cidr=None):
        self.vpc = vpc
        self.name = name
        if network_cidr:
            self._network_cidr = IPNetwork(network_cidr)
        else:
            self._network_cidr = None
        self._centralized_route_table_loaded = False
        self._centralized_route_table = None  # lazily initialized
        self._security_group = None  # lazily initialized
        self._disco_subnets = None  # lazily initialized

    @property
    def network_cidr(self):
        """Get the network_cidr for the meta network"""
        if not self._network_cidr:
            # if we don't have a network_cidr yet (if it wasn't passed in the constructor)
            # then calculate it from the subnets
            subnets = self._instantiate_subnets(try_creating_aws_subnets=False)

            # calculate how big the meta network must have been if we divided it into the existing subnets
            subnet_cidr_offset = calc_subnet_offset(len(subnets.values()))

            # pick one of the subnets to do our math from
            subnet_network = IPNetwork(subnets.values()[0].subnet_dict['CidrBlock'])

            # the meta network cidr is the cidr of one of the subnets but with a smaller prefix
            subnet_network.prefixlen = subnet_network.prefixlen - subnet_cidr_offset
            self._network_cidr = subnet_network.cidr

        return self._network_cidr

    def _resource_name(self, suffix=None):
        suffix = "_{0}".format(suffix) if suffix else ""
        return "{0}_{1}{2}".format(self.vpc.environment_name, self.name, suffix)

    def create(self):
        """
        Metanetwork is initialized lazily. This forces creation of all
        components.
        """
        self._centralized_route_table = self.centralized_route_table
        self._security_group = self.security_group
        self._disco_subnets = self.disco_subnets

    @property
    def _resource_filter(self):
        resource_filter = self.vpc.vpc_filter()
        resource_filter["tag:meta_network"] = self.name
        return resource_filter

    def _tag_resource(self, resource, suffix=None):
        keep_trying(300, resource.add_tag, "Name", self._resource_name(suffix))
        keep_trying(300, resource.add_tag, "meta_network", self.name)

    @property
    def centralized_route_table(self):
        '''Returns the centralized route table for our metanetwork,
        which could be None'''
        if not self._centralized_route_table_loaded:
            self._centralized_route_table = self._find_centralized_route_table()
            self._centralized_route_table_loaded = True
        return self._centralized_route_table

    def _find_centralized_route_table(self):
        route_tables = self.vpc.vpc.connection.get_all_route_tables(
            filters=self._resource_filter
        )
        if len(route_tables) != 1:
            # If the number of route tables is more than one, it means there is
            # one route table per disco_subnet, therefore don't return anything.
            return None

        return route_tables[0]

    @property
    def security_group(self):
        '''Finds or creates the security group for our metanetwork'''
        if not self._security_group:
            self._security_group = find_or_create(
                self._find_security_group, self._create_security_group
            )
        return self._security_group

    def _find_security_group(self):
        try:
            return self.vpc.vpc.connection.get_all_security_groups(
                filters=self._resource_filter
            )[0]
        except IndexError:
            return None

    @property
    def sg_description(self):
        """Returns a description of the metanetwork's purpose"""
        return NETWORKS[self.name]

    def _create_security_group(self):
        security_group = self.vpc.vpc.connection.create_security_group(
            self._resource_name(),
            self.sg_description,
            self.vpc.vpc.id
        )
        self._tag_resource(security_group)
        logging.debug("%s security_group: %s", self.name, security_group)
        return security_group

    @property
    def disco_subnets(self):
        '''Creates the subnets for our metanetwork'''
        if not self._disco_subnets:
            self._disco_subnets = self._instantiate_subnets()
        return self._disco_subnets

    @property
    def subnet_ip_networks(self):
        """
        Return IPNetwork of all subnet CIDRs
        """
        return [
            IPNetwork(subnet['CidrBlock'])
            for subnet in
            self.disco_subnets.values()
        ]

    def add_nat_gateways(self, allocation_ids):
        """
        Creates a NAT gateway in each of the metanetwork's subnet
        :param allocation_ids: Allocation ids of the Elastic IPs that will be
                               associated with the NAT gateways
        """
        if len(self.disco_subnets.values()) != len(allocation_ids):
            raise EIPConfigError("The number of subnets does not match with the "
                                 "number of NAT gateway EIPs provided for {0}: "
                                 "{1} != {2}"
                                 .format(self._resource_name(),
                                         len(self.disco_subnets.values()),
                                         len(allocation_ids)))

        if self.centralized_route_table:
            for disco_subnet in self.disco_subnets.values():
                disco_subnet.recreate_route_table()

            self.vpc.vpc.connection.delete_route_table(self.centralized_route_table.id)
            self._centralized_route_table = None

        for disco_subnet, allocation_id in zip(self.disco_subnets.values(), allocation_ids):
            disco_subnet.create_nat_gateway(allocation_id)

    def delete_nat_gateways(self):
        """ Deletes all subnets' NAT gateways if any """
        for disco_subnet in self.disco_subnets.values():
            disco_subnet.delete_nat_gateway()

    def _instantiate_subnets(self, try_creating_aws_subnets=True):
        # FIXME needs to talk about and simplify this
        logging.debug("instantiating subnets")
        zones = self.vpc.vpc.connection.get_all_zones()
        logging.debug("zones: %s", zones)
        # We'll need to split each subnet into smaller ones, one per zone
        # offset is how much we need to add to cidr divisor to create at least
        # that len(zone) subnets
        zone_cidr_offset = calc_subnet_offset(len(zones))
        logging.debug("zone_offset: %s", zone_cidr_offset)

        if try_creating_aws_subnets:
            zone_cidrs = self.network_cidr.subnet(
                int(self.network_cidr.prefixlen + zone_cidr_offset)
            )
        else:
            zone_cidrs = ['' for _ in zones]

        subnets = {}
        for zone, cidr in zip(zones, zone_cidrs):
            logging.debug("%s %s", zone, cidr)
            disco_subnet = DiscoSubnet(str(zone.name), self, str(cidr),
                                       self.centralized_route_table.id
                                       if self.centralized_route_table else None)
            subnets[zone.name] = disco_subnet
            logging.debug("%s disco_subnet: %s", self.name, disco_subnet)

        return subnets

    def subnet_by_ip(self, ip_address):
        """ Return the subnet to which the ip address belongs to """
        ip_address = IPAddress(ip_address)
        for disco_subnet in self.disco_subnets.values():
            cidr = IPNetwork(disco_subnet.subnet_dict['CidrBlock'])
            if ip_address >= cidr[0] and ip_address <= cidr[-1]:
                return disco_subnet.subnet_dict
        raise IPRangeError("IP {0} is not in Metanetwork ({1}) range.".format(ip_address, self.name))

    def create_interfaces_specification(self, subnet_ids=None, public_ip=False):
        """
        Create a network interface specification for an instance -- to be used
        with run_instance()
        """
        random_subnet_id = choice(subnet_ids if subnet_ids else
                                  [disco_subnet.subnet_dict['SubnetId']
                                   for disco_subnet in self.disco_subnets.values()])
        interface = NetworkInterfaceSpecification(
            subnet_id=random_subnet_id,
            groups=[self.security_group.id],
            associate_public_ip_address=public_ip)
        interfaces = NetworkInterfaceCollection(interface)
        return interfaces

    def get_interface(self, private_ip):
        """
        Allocate a 'floating' network inteface with static ip --
        if it does not already exist.
        """
        interface_filter = self.vpc.vpc_filter()
        interface_filter["private-ip-address"] = private_ip
        interfaces = self.vpc.vpc.connection.get_all_network_interfaces(
            filters=interface_filter
        )
        if interfaces:
            return interfaces[0]

        logging.debug("Creating floating ENI %s", private_ip)
        aws_subnet = self.subnet_by_ip(private_ip)
        return self.vpc.vpc.connection.create_network_interface(
            subnet_id=aws_subnet['SubnetId'],
            private_ip_address=private_ip,
            description="floating interface",
            groups=[self.security_group.id],
        )

    def add_sg_rule(self, protocol, ports, sg_source=None, cidr_source=None):
        """ Add a security rule to the network """
        sg_args = {
            "group_id": self.security_group.id,
            "ip_protocol": protocol
        }
        if sg_source:
            sg_args["src_security_group_group_id"] = sg_source
        if cidr_source:
            sg_args["cidr_ip"] = cidr_source

        sg_args["from_port"] = ports[0]
        sg_args["to_port"] = ports[1]
        logging.debug("Adding sg_rule: %s", sg_args)
        self.vpc.vpc.connection.authorize_security_group(**sg_args)

    def ip_by_offset(self, offset):
        """
        Pass in +10 and get 10th ip of subnet range
        Pass in -2 and get 2nd to last ip of subnet

        Returns IpAddress object, usually you'll want
        to cast this to str.
        """

        try:
            offset = int(offset)
        except ValueError:
            raise IPRangeError(
                "Cannot find IP in metanetwork {0} by offset {1}."
                .format(self.name, offset)
            )

        subnets = sorted(self.subnet_ip_networks)
        base_address = subnets[0].first if offset >= 0 else subnets[-1].last
        desired_address = IPAddress(base_address + offset)
        # Lazy check to ensure IP address is in metanetwork range
        self.subnet_by_ip(desired_address)

        return desired_address

    def add_route(self, destination_cidr_block, gateway_id):
        """ Add a gateway route to the centralized route table or to all the
        subnets' route tables"""

        if self.centralized_route_table:
            try:
                return self.vpc.vpc.connection.create_route(
                    route_table_id=self.centralized_route_table.id,
                    destination_cidr_block=destination_cidr_block,
                    gateway_id=gateway_id
                )
            except EC2ResponseError:
                logging.exception("Failed to create route due to conflict. Deleting old route and re-trying.")
                self.vpc.vpc.connection.delete_route(self.centralized_route_table.id, destination_cidr_block)
                new_route = self.vpc.vpc.connection.create_route(
                    route_table_id=self.centralized_route_table.id,
                    destination_cidr_block=destination_cidr_block,
                    gateway_id=gateway_id
                )
                logging.error("Route re-created")
                return new_route
        else:
            # No centralized route table here, so add a route to each disco_subnet
            for disco_subnet in self.disco_subnets.values():
                if not disco_subnet.add_route_to_gateway(destination_cidr_block, gateway_id):
                    raise RouteCreationError("Failed to create a route for metanetwork-subnet {0}-{1}:"
                                             "{2} -> {3}".format(self.name,
                                                                 disco_subnet.name,
                                                                 destination_cidr_block,
                                                                 gateway_id))

    def add_nat_gateway_route(self, dest_metanetwork):
        """ Add a default route in each of the subnet's route table to the corresponding NAT gateway
        of the same AZ in the destination metanetwork """
        for zone in self.disco_subnets.keys():
            self.disco_subnets[zone].add_route_to_nat_gateway(
                '0.0.0.0/0',
                dest_metanetwork.disco_subnets[zone].nat_gateway['NatGatewayId']
            )

    def create_peering_route(self, peering_conn, cidr):
        """ create/update a route between the peering connection and all the subnets.
        If a centralized route table is used, add the route there. If not, add the route
        to all the subnets. """
        if self.centralized_route_table:
            peering_routes_for_peering = [
                _ for _ in self.centralized_route_table.routes
                if _.vpc_peering_connection_id == peering_conn
            ]
            if not peering_routes_for_peering:
                peering_routes_for_cidr = [
                    _ for _ in self.centralized_route_table.routes
                    if _.destination_cidr_block == cidr
                ]
                if not peering_routes_for_cidr:
                    logging.info(
                        'create routes for (route_table: %s, dest_cidr: %s, connection: %s)',
                        self.centralized_route_table.id, cidr, peering_conn.id)
                    self.vpc.vpc.connection.create_route(route_table_id=self.centralized_route_table.id,
                                                         destination_cidr_block=cidr,
                                                         vpc_peering_connection_id=peering_conn.id)
                else:
                    logging.info(
                        'update routes for (route_table: %s, dest_cidr: %s, connection: %s)',
                        self.centralized_route_table.id, cidr, peering_conn.id)
                    self.vpc.vpc.connection.replace_route(route_table_id=self.centralized_route_table.id,
                                                          destination_cidr_block=cidr,
                                                          vpc_peering_connection_id=peering_conn.id)
        else:
            # No centralized route table here, so add a route to each subnet
            for disco_subnet in self.disco_subnets.values():
                disco_subnet.create_peering_routes(peering_conn.id, cidr)

"""
Some code to manage elastic IP's.  Elastic IP's are fixed internet routable addresses
that we can assign to our AWS instances.  We use them for certain hostclasses, such as Jenkins.
"""
import boto3

from boto.vpc import VPCConnection
from disco_aws_automation.resource_helper import throttled_call


class DiscoEIP(object):
    """
    A simple class to manage EIP's
    """

    def __init__(self):
        self.vpc_conn = VPCConnection()
        self.ec2_conn = boto3.client('ec2')

    def list(self):
        """Returns all of our currently allocated EIPs"""
        return self.vpc_conn.get_all_addresses()

    def allocate(self):
        """Allocates a new VPC EIP"""
        return self.vpc_conn.allocate_address(domain='vpc')

    def tag_dynamic(self, eip_allocation_id):
        """
        Tag an EIP as dynamic
        "Tags": [
            {
                "Key": "dynamic",
                "Value": "true"
            }
        ]
        """
        return throttled_call(self.ec2_conn.create_tags, Resources=[eip_allocation_id],
                              Tags=[{'Key': 'dynamic', 'Value': 'true'}])

    def release(self, eip_address, force=False):
        """
        Releases an EIP.

        If it is currently associated with a machine we do not release it unless
        the force param is set.
        """
        eip = self.vpc_conn.get_all_addresses([eip_address])[0]

        if eip.association_id:
            if force:
                eip.disassociate()
            else:
                return False

        return eip.release()

    def find_eip_address(self, eip):
        """ Finds the EIP Address for the public eip specified. """
        address_filter = {'public-ip': eip}
        try:
            return self.vpc_conn.get_all_addresses(filters=address_filter)[0]
        except IndexError:
            return None

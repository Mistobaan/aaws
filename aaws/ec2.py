#
# Copyright 2011 Snitch Incorporated
#
# This file is part of AAWS.
#
# AAWS is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# AAWS is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with AAWS.  If not, see <http://www.gnu.org/licenses/>.
#
#
#		ec2.py,
#
#			This module provides an interface to Amazon Elastic Compute Cloud (EC2).
#
#
#

from aws import AWSService, AWSError, getBotoCredentials
import request
from xml.etree import ElementTree as ET
from urlparse import urlparse


class EC2(AWSService):
	endpoints = {
		'us-east-1': 'ec2.us-east-1.amazonaws.com',
		'us-west-1': 'ec2.us-west-1.amazonaws.com',
		'eu-west-1': 'ec2.eu-west-1.amazonaws.com',
		'ap-southeast-1': 'ec2.ap-southeast-1.amazonaws.com',
		'ap-northeast-1': 'ec2.ap-northeast-1.amazonaws.com',
	}
	version = '2011-05-15'
	xmlns = 'http://ec2.amazonaws.com/doc/2011-05-15/'

	def __init__(self, region, key, secret, version=None):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret


	def DescribeInstances(self, InstanceIds=None, Filters=None):
		"""Returns information about instances that you own. If you specify one or more instance IDs, Amazon EC2 returns information
			for those instances. If you do not specify instance IDs, Amazon EC2 returns information for all relevant instances. If
			you specify an invalid instance ID, an error is returned. If you specify an instance that you do not own, it will not be
			included in the returned results.
			Recently terminated instances might appear in the returned results.This interval is usually less than one hour.
			You can filter the results to return information only about instances that match criteria you specify. For example, you
			could get information about only instances launched with a certain key pair. You can specify multiple values for a filter
			(e.g., the instance was launched with either key pair A or key pair B). An instance must match at least one of the
			specified values for it to be included in the results.
			You can specify multiple filters (e.g., the instance was launched with a certain key pair and uses an Amazon EBS volume
			as the root device). An instance must match all the filters for it to be included in the results. If there's no match, no
			special message is returned; the response is simply empty.
			You can use wildcards with the filter values: * matches zero or more characters, and ? matches exactly one character. You
			can escape special characters using a backslash before the character. For example, a value of \\*amazon\\?\\\\ searches for
			the literal string *amazon?\\.
			"""

		def findadd(m, node, attr):
			for part in attr.split('.'):
				if node is None:
					break
				node = node.find('{%s}%s' % (self.xmlns, part))
			if node is not None:
				m[attr] = node.text

		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				instances = []
				for node in root.findall('.//{%s}instancesSet' % self.xmlns):
					for item in node.findall('{%s}item' % self.xmlns):
						i = {}
						for attr in ('instanceId', 'imageId', 'privateDnsName', 'dnsName', 'keyName', 'amiLaunchIndex', 'instanceType', 'launchTime',
								'kernelId', 'privateIpAddress', 'ipAddress', 'architecture', 'rootDeviceType', 'rootDeviceName', 'virtualizationType',
								'instanceState.code', 'instanceState.name', 'placement.availabilityZone', 'placement.tenancy', 'monitoring.state', 'hypervisor'):
							findadd(i, item, attr)
						# XXX: groupSet, blockDeviceMapping
						tags = {}
						tagSet = item.find('{%s}tagSet' % self.xmlns)
						for tagitem in tagSet.findall('{%s}item' % self.xmlns):
							tags[tagitem.find('{%s}key' % self.xmlns).text] = tagitem.find('{%s}value' % self.xmlns).text
						i['tags'] = tags
						instances.append(i)
				return instances
			raise AWSError(status, reason, data)

		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'DescribeInstances', {
				'Version': self.version,
			}, response)
		if InstanceIds is not None:
			for idx, iid in enumerate(InstanceIds):
				r.addParm('InstanceId.%d' % idx, iid)
		# XXX: Filters
		return r



if __name__ == '__main__':
	key, secret = getBotoCredentials()
	ec2 = EC2('us-west-1', key, secret)
	print ec2.DescribeInstances().execute()



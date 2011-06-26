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

from aws import AWSService, AWSError, getBotoCredentials
import request
from xml.etree import ElementTree as ET


class SimpleDB(AWSService):
	endpoints = {
		'us-east-1': 'sdb.amazonaws.com',
		'us-west-1': 'sdb.us-west-1.amazonaws.com',
		'eu-west-1': 'sdb.eu-west-1.amazonaws.com',
		'ap-southeast-1': 'sdb.ap-southeast-1.amazonaws.com',
		'ap-northeast-1': 'sdb.ap-northeast-1.amazonaws.com',
	}
	version = '2009-04-15'
	xmlns = 'http://sdb.amazonaws.com/doc/2009-04-15/'

	def __init__(self, region, key, secret):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret

	def BatchDeleteAttributes(self, DomainName, Items):
		pass

	def BatchPutAttributes(self, DomainName, Items):
		pass

	def CreateDomain(self, DomainName):
		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'CreateDomain', {
			'DomainName': DomainName,
			'Version': self.version,
		}, response)


	def DeleteAttributes(self, DomainName, ItemName, Attributes=None, Expected=None):
		"""Deletes one or more attributes associated with the item. If all attributes of an item are deleted, the item is deleted.

			NOTE: If you specify DeleteAttributes without attributes, all the attributes for the item are deleted.
			Unless you specify conditions, the DeleteAttributes is an idempotent operation; running it multiple times on the same item
			or attribute does not result in an error response.
			Conditional deletes are useful for only deleting items and attributes if specific conditions are met. If the conditions are
			met, Amazon SimpleDB performs the delete. Otherwise, the data is not deleted.
			When using eventually consistent reads, a getAttributes or select request (read) immediately after a DeleteAttributes or
			Put Attributes request (write) might not return the updated data. A consistent read always reflects all writes that received
			a successful response prior to the read. For more information, see Consistency.
			You can perform the expected conditional check on one attribute per operation.

			domain -- The name of the domain in which to perform the operation.
				Type: String
				Required: Yes
			itemName -- The name of the item.
				Type: String
				Required: Yes
			attributes -- Either a dict or iterable of tuples/strings specifying the attributes to be deleted. Each 'key' specifies the
				attribute name to delete; if a value is specified (not None) then only the specified key/value is deleted (for multi-value
				attributes).
				Type: dict or iterable of tuples/strings
				Required: No
			"""
		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'DeleteAttributes', {
			'DomainName': DomainName,
			'ItemName': ItemName,
			'Version': self.version,
		}, response)
		if Attributes is not None:
			if hasattr(Attributes, 'items'):
				Attributes = Attributes.items()
			for idx, (name, value) in enumerate(Attributes):
				r.addParm('Attribute.%d.Name' % idx, name)
				r.addParm('Attribute.%d.Value' % idx, value)
		if Expected is not None:
			if hasattr(Expected, 'items'):
				Expected = Expected.items()
			for idx, (name, value) in enumerate(Expected):
				r.addParm('Expected.%d.Name' % idx, name)
				if value is None:
					r.addParm('Expected.%d.Exists' % idx, True)
				else:
					r.addParm('Expected.%d.Value' % idx, value)
					r.addParm('Expected.%d.Exists' % idx, False)
		return r


	def DeleteDomain(self, DomainName):
		"""The DeleteDomain operation deletes a domain. Any items (and their attributes) in the domain are deleted as well.
			The DeleteDomain operation might take 10 or more seconds to complete.

			Note: Running DeleteDomain on a domain that does not exist or running the function multiple times using the same domain name
			will not result in an error response.

			domain -- The name of the domain to delete.
				Type: string
				Required: Yes
			"""
		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'DeleteDomain', {
			'DomainName': DomainName,
			'Version': self.version,
		}, response)


	def DomainMetadata(self, DomainName):
		"""Returns information about the domain, including when the domain was created, the number of items and attributes, and the size
			of attribute names and values.

			domain -- The name of the domain for which to display metadata.
				Type: String
				Required: Yes
			"""

		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				metadata = {}
				result = root.find('.//{%s}DomainMetadataResult' % self.xmlns)
				for node in result:
					metadata[node.tag[len(self.xmlns) + 2:]] = node.text
				return metadata
			raise AWSError(status, reason, data)
		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'DomainMetadata', {
			'DomainName': DomainName,
			'Version': self.version,
		}, response)


	def GetAttributes(self, DomainName, ItemName, AttributeNames=None, ConsistentRead=None):
		"""Returns all of the attributes associated with the item. Optionally, the attributes returned can be limited to one or more specified
			attribute name parameters.

			Amazon SimpleDB keeps multiple copies of each domain. When data is written or updated, all copies of the data are updated. However,
			it takes time for the update to propagate to all storage locations. The data will eventually be consistent, but an immediate read
			might not show the change. If eventually consistent reads are not acceptable for your application, use ConsistentRead. Although this
			operation might take longer than a standard read, it always returns the last updated value.

			NOTE: If the item does not exist on the replica that was accessed for this operation, an empty set is returned.
			If you specify GetAttributes without any attribute names, all the attributes for the item are returned.

			domain -- The name of the domain in which to perform the operation.
			itemName -- The name of the item.
			attributeNames -- The name of the attributes to retrieve. If specified it should be an iterable of strings.
			consistentRead -- Boolean specifying whether consistent read should be performed. Default False.
			"""

		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				result = root.find('.//{%s}GetAttributesResult' % self.xmlns)
				attribs = []
				for node in result.findall('{%s}Attribute' % self.xmlns):
					attribs.append((node.find('{%s}Name' % self.xmlns).text, node.find('{%s}Value' % self.xmlns).text))
				return attribs
			raise AWSError(status, reason, data)
		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'GetAttributes', {
			'DomainName': DomainName,
			'ItemName': ItemName,
			'ConsistentRead': ConsistentRead,
			'Version': self.version,
		}, response)
		if AttributeNames is not None:
			for idx, name in enumerate(AttributeNames):
				r.addParm('AttributeName.%d' % idx, name)
		return r


	def ListDomains(self, MaxDomains=100, NextToken=None):
		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				result = root.find('.//{%s}ListDomainsResult' % self.xmlns)
				domains = []
				for node in result.findall('{%s}DomainName' % self.xmlns):
					domains.append(node.text)
				token = None
				node = root.find('.//{%s}NextToken' % self.xmlns)
				if node is not None:
					token = node.text
				return domains, token
			raise AWSError(status, reason, data)
		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ListDomains', {
			'MaxDomains': MaxDomains,
			'NextToken': NextToken,
			'Version': self.version,
		}, response)

	def PutAttributes(self, DomainName, ItemName, Attributes, Expected=None, replace=True):
		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'PutAttributes', {
			'DomainName': DomainName,
			'ItemName': ItemName,
			'Version': self.version,
		}, response)
		if hasattr(Attributes, 'items'):
			Attributes = [(name, value, replace) for name, value in Attributes.items()]
		for idx, (name, value, replace) in enumerate(Attributes):
			r.addParm('Attribute.%d.Name' % idx, name)
			r.addParm('Attribute.%d.Value' % idx, value)
			r.addParm('Attribute.%d.Replace' % idx, replace)
		if Expected is not None:
			if hasattr(Expected, 'items'):
				Expected = Expected.items()
			for idx, (name, value) in enumerate(Expected):
				r.addParm('Expected.%d.Name' % idx, name)
				if value is None:
					r.addParm('Expected.%d.Exists' % idx, True)
				else:
					r.addParm('Expected.%d.Value' % idx, value)
					r.addParm('Expected.%d.Exists' % idx, False)
		return r

	def Select(self, SelectExpression, NextToken=None, ConsistentRead=None):
		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				items = []
				for node in root.findall('.//{%s}Item' % self.xmlns):
					name = node.find('{%s}Name' % self.xmlns).text
					attribs = []
					for attr in node.findall('{%s}Attribute' % self.xmlns):
						attribs.append((attr.find('{%s}Name' % self.xmlns).text, attr.find('{%s}Value' % self.xmlns).text))
					items.append((name, attribs))
				return items
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'Select', {
			'SelectExpression': SelectExpression,
			'NextToken': NextToken,
			'ConsistentRead': ConsistentRead,
			'Version': self.version,
		}, response)



if __name__ == '__main__':
	key, secret = getBotoCredentials()
	sdb = SimpleDB('us-west-1', key, secret)
	print sdb.ListDomains().GET()
	print sdb.CreateDomain('test').GET()
	print sdb.PutAttributes('test', 'item1', {'name': 'mary', 'age': '123', 'gender': 'female'}).GET()
	print sdb.GetAttributes('test', 'item1').GET()
	print sdb.GetAttributes('test', 'item1', ('name', 'age')).GET()
	print sdb.Select("select * from test").GET()
	print sdb.DomainMetadata('test').GET()
	print sdb.DeleteAttributes('test', 'item1', {'age': None}).GET()
	print sdb.GetAttributes('test', 'item1', ConsistentRead=True).GET()
#	print sdb.DeleteDomain('test').GET()


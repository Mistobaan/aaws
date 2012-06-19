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


	def __init__(self, region, key, secret, version=None):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret


	def BatchDeleteAttributes(self, DomainName, Items):
		"""Performs multiple DeleteAttributes operations in a single call, which reduces round trips and
			latencies. This enables Amazon SimpleDB to optimize requests, which generally yields better throughput.

			NOTE: If you specify BatchDeleteAttributes without attributes or values, all the attributes for the
			item are deleted.
			BatchDeleteAttributes is an idempotent operation; running it multiple times on the same item or attribute
			doesn't result in an error.
			The BatchDeleteAttributes operation succeeds or fails in its entirety. There are no partial deletes.
			You can execute multiple BatchDeleteAttributes operations and other operations in parallel. However,
			large numbers of concurrent BatchDeleteAttributes calls can result in Service Unavailable (503) responses.
			This operation is vulnerable to exceeding the maximum URL size when making a REST request using the HTTP GET method.
			This operation does not support conditions using Expected.X.Name, Expected.X.Value, or Expected.X.Exists.

			The following limitations are enforced for this operation:
				* 1MB request size
				* 25 item limit per batchDeleteAttributes operation

			DomainName -- The name of the domain in which to perform the operation.
			Items -- Either a dict or an iterable of (ItemName, attributes) tuples.
				attributes is either a dict or an iterable of (name, value) tuples.

			returns True on success
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'BatchDeleteAttributes', {
				'DomainName': DomainName,
				'Version': self.version,
			}, response)
		if hasattr(Items, 'items'):
			Items = Items.items()
		for itemIdx, (name, attributes) in enumerate(Items):
			r.addParm('Item.%d.ItemName' % itemIdx, name)
			if attributes is not None:
				if hasattr(attributes, 'items'):
					attributes = attributes.items()
				for attrIdx, (name, value) in enumerate(attributes):
					r.addParm('Item.%d.Attribute.%d.Name' % (itemIdx, attrIdx), name)
					r.addParm('Item.%d.Attribute.%d.Value' % (itemIdx, attrIdx), value)
		return r


	def BatchPutAttributes(self, DomainName, Items, replace=True):
		"""With the BatchPutAttributes operation, you can perform multiple PutAttribute operations in a single call. This
			helps you yield savings in round trips and latencies, and enables Amazon SimpleDB to optimize requests, which generally
			yields better throughput.

			You can specify attributes and values for items using a combination of the Item.Y.Attribute.X.Name and
			Item.Y.Attribute.X.Value parameters. To specify attributes and values for the first item, you use Item.1.Attribute.1.Name
			and Item.1.Attribute.1.Value for the first attribute, Item.1.Attribute.2.Name and Item.1.Attribute.2.Value for the second
			attribute, and so on.

			To specify attributes and values for the second item, you use Item.2.Attribute.1.Name and Item.2.Attribute.1.Value for the
			first attribute, Item.2.Attribute.2.Name and Item.2.Attribute.2.Value for the second attribute, and so on.

			Amazon SimpleDB uniquely identifies attributes in an item by their name/value combinations. For example, a single item can
			have the attributes { "first_name", "first_value" } and { "first_name", second_value" }. However, it cannot have two
			attribute instances where both the Item.Y.Attribute.X.Name and Item.Y.Attribute.X.Value are the same.

			Optionally, you can supply the Replace parameter for each individual attribute. Setting this value to true causes the new
			attribute value to replace the existing attribute value(s) if any exist. Otherwise, Amazon SimpleDB simply inserts the
			attribute values. For example, if an item has the attributes { 'a', '1' }, { 'b', '2'}, and { 'b', '3' } and the requester
			calls BatchPutAttributes using the attributes { 'b', '4' } with the Replace parameter set to true, the final attributes of
			the item are changed to { 'a', '1' } and { 'b', '4' }. This occurs because the new 'b' attribute replaces the old value.

			NOTE: You cannot specify an empty string as an item or attribute name.
			The BatchPutAttributes operation succeeds or fails in its entirety. There are no partial puts.
			You can execute multiple BatchPutAttributes operations and other operations in parallel. However, large numbers of
			concurrent BatchPutAttributes calls can result in Service Unavailable (503) responses.
			This operation is vulnerable to exceeding the maximum URL size when making a REST request using the HTTP GET method.
			This operation does not support conditions using Expected.X.Name, Expected.X.Value, or Expected.X.Exists.

			The following limitations are enforced for this operation:
				* 256 attribute name-value pairs per item
				* 1 MB request size
				* 1 billion attributes per domain
				* 10 GB of total user data storage per domain
				* 25 item limit per BatchPutAttributes operation

			DomainName -- The name of the domain in which to perform the operation.
			Items -- Either a dict or an iterable of (ItemName, attributes) tuples.
				attributes is either a dict or an iterable of (name, value, replace) tuples.
			replace -- Flag that supplies the replace parameter when a dict is provided for attributes

			returns True on success
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'BatchPutAttributes', {
				'DomainName': DomainName,
				'Version': self.version,
			}, response)
		if hasattr(Items, 'items'):
			Items = Items.items()
		for itemIdx, (name, attributes) in enumerate(Items):
			r.addParm('Item.%d.ItemName' % itemIdx, name)
			if hasattr(attributes, 'items'):
				attributes = [(name, value, replace) for name, value in attributes.items()]
			for attrIdx, (name, value, repl) in enumerate(attributes):
				r.addParm('Item.%d.Attribute.%d.Name' % (itemIdx, attrIdx), name)
				r.addParm('Item.%d.Attribute.%d.Value' % (itemIdx, attrIdx), value)
				r.addParm('Item.%d.Attribute.%d.Replace' % (itemIdx, attrIdx), repl)
		return r


	def CreateDomain(self, DomainName):
		"""The CreateDomain operation creates a new domain. The domain name must be unique among the domains
			associated with the Access Key ID provided in the request. The CreateDomain operation might take 10
			or more seconds to complete.

			NOTE: CreateDomain is an idempotent operation; running it multiple times using the same domain name will not result
			in an error response.
			You can create up to 250 domains per account.
			If you require additional domains, go to http://aws.amazon.com/contact-us/simpledb-limit-request/.

			DomainName -- The name of the domain to create. The name can range between 3 and 255 characters and can contain the
				following characters: a-z, A-Z, 0-9, '_', '-', and '.'.

			returns True on success
			"""

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

			DomainName -- The name of the domain in which to perform the operation.
				Type: String
				Required: Yes
			ItemName -- The name of the item.
				Type: String
				Required: Yes
			Attributes -- Either a dict or iterable of tuples/strings specifying the attributes to be deleted. Each 'key' specifies the
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
		"""The ListDomains operation lists all domains associated with the Access Key ID. It returns domain names up to
			the limit set by MaxNumberOfDomains. A NextToken is returned if there are more than MaxNumberOfDomains domains.
			Calling ListDomains successive times with the NextToken returns up to MaxNumberOfDomains more domain names each
			time.

			MaxNumberOfDomains -- The maximum number of domain names you want returned.
			NextToken -- String that tells Amazon SimpleDB where to start the next list of domain names.

			returns [domains], nextToken
			"""

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
			}, response, request.ListFollow)


	def PutAttributes(self, DomainName, ItemName, Attributes, Expected=None, replace=True):
		"""The PutAttributes operation creates or replaces attributes in an item. You specify new attributes using a
			combination of the Attribute.X.Name and Attribute.X.Value parameters. You specify the first attribute by the
			parameters Attribute.1.Name and Attribute.1.Value, the second attribute by the parameters Attribute.2.Name
			and Attribute.2.Value, and so on.

			Attributes are uniquely identified in an item by their name/value combination. For example, a single item can
			have the attributes { "first_name", "first_value" } and { "first_name", second_value" }. However, it cannot
			have two attribute instances where both the Attribute.X.Name and Attribute.X.Value are the same.

			Optionally, the requester can supply the Replace parameter for each individual attribute. Setting this value
			to true causes the new attribute value to replace the existing attribute value(s). For example, if an item
			has the attributes { 'a', '1' }, { 'b', '2'} and { 'b', '3' } and the requester calls PutAttributes using
			the attributes { 'b', '4' } with the Replace parameter set to true, the final attributes of the item are
			changed to { 'a', '1' } and { 'b', '4' }, which replaces the previous values of the 'b' attribute with the
			new value.

			Conditional updates are useful for ensuring multiple processes do not overwrite each other. To prevent this
			from occurring, you can specify the expected attribute name and value. If they match, Amazon SimpleDB performs
			the update. Otherwise, the update does not occur.

			NOTE: Using PutAttributes to replace attribute values that do not exist will not result in an error response.
			You cannot specify an empty string as an attribute name.
			When using eventually consistent reads, a GetAttributes or Select request (read) immediately after a
			DeleteAttributes or PutAttributes request (write) might not return the updated data. A consistent read always
			reflects all writes that received a successful response prior to the read. For more information, see Consistency.
			You can perform the expected conditional check on one attribute per operation.

			The following limitations are enforced for this operation:
				* 256 total attribute name-value pairs per item
				* One billion attributes per domain
				* 10 GB of total user data storage per domain

			DomainName -- The name of the domain in which to perform the operation.
			ItemName -- The name of the item.
			Attributes -- either a dict or an iterable of (name, value, replace) tuples.
			Expected -- either a dict or an iterable of (name, value) tuples. When value is None Expected.Exists is set to
			True; when value is not-None Expected.Exists is set to False.
			replace -- Flag that supplies the replace parameter when a dict is provided for attributes

			returns True on success
			"""

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


	def Select(self, SelectExpression, NextToken=None, ConsistentRead=None, boxusage=None):
		"""The Select operation returns a set of Attributes for ItemNames that match the select expression. Select is
			similar to the standard SQL SELECT statement.

			Amazon SimpleDB keeps multiple copies of each domain. When data is written or updated, all copies of the data are
			updated. However, it takes time for the update to propagate to all storage locations. The data will eventually be
			consistent, but an immediate read might not show the change. If eventually consistent reads are not acceptable for
			your application, use ConsistentRead. Although this operation might take longer than a standard read, it always
			returns the last updated value.

			The total size of the response cannot exceed 1 MB. Amazon SimpleDB automatically adjusts the number of items
			returned per page to enforce this limit. For example, even if you ask to retrieve 2500 items, but each individual
			item is 10 KB in size, the system returns 100 items and an appropriate next token so you can get the next page of
			results.

			For information on how to construct select expressions, see Using Select to Create Amazon SimpleDB Queries.

			NOTE: Operations that run longer than 5 seconds return a time-out error response or a partial or empty result set.
			Partial and empty result sets contains a next token which allow you to continue the operation from where it left off.
			Responses larger than one megabyte return a partial result set.
			Your application should not excessively retry queries that return RequestTimeout errors. If you receive too many
			RequestTimeout errors, reduce the complexity of your query expression.
			When designing your application, keep in mind that Amazon SimpleDB does not guarantee how attributes are ordered
			in the returned response.
			For information about limits that affect Select, see Limits.
			The select operation is case-sensitive.

			SelectExpression -- The expression used to query the domain.
			NextToken -- String that tells Amazon SimpleDB where to start the next list of ItemNames.
			ConsistentRead -- When set to true, ensures that the most recent data is returned. For more information, see Consistency

			returns a list of items which are tuples of (ItemName, Attributes) where Attributes is a list of (Name, Value) tuples.
			"""

		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				if boxusage is not None:
					boxusage.append(root.find('.//{%s}BoxUsage' % self.xmlns).text)
				items = []
				for node in root.findall('.//{%s}Item' % self.xmlns):
					name = node.find('{%s}Name' % self.xmlns).text
					attribs = []
					for attr in node.findall('{%s}Attribute' % self.xmlns):
						attribs.append((attr.find('{%s}Name' % self.xmlns).text, attr.find('{%s}Value' % self.xmlns).text))
					items.append((name, attribs))
				token = None
				node = root.find('.//{%s}NextToken')
				if node is not None:
					token = node.text
				return items, token
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'Select', {
				'SelectExpression': SelectExpression,
				'NextToken': NextToken,
				'ConsistentRead': ConsistentRead,
				'Version': self.version,
			}, response, request.ListFollow)



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


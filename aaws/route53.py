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
from urlparse import urlparse
import urllib
import time
import hmac
import hashlib
import base64
import httplib

#XXX: support CallerReference



class Route53Request(request.AWSRequest):

	def __init__(self, host, version, uri, key, secret, parameters, handler=None, follower=None, verb='GET', body=None, contentType=None):
		self._version = version
		self._body = body
		self._contentType = contentType
		request.AWSRequest.__init__(self, host, uri, key, secret, None, parameters, handler, follower, verb)

	def copy(self):
		return Route53Request(self._host, self._version, self._uri, self._key, self._secret, self._parameters, self._handler, self._follower, self._verb, self._body, self._contentType)

	def makePath(self, verb='GET'):
		parms = []
		for key in sorted(self._parameters.keys()):
			parms.append(urllib.quote(key, safe='') + '=' + urllib.quote(self._parameters[key], safe='-_~'))
		if len(parms):
			return '/' + self._version + '/' + urllib.quote(self._uri) + '?' + '&'.join(parms)
		return '/' + self._version + '/' + urllib.quote(self._uri)

	def makeHeaders(self, verb='GET'):
		timestamp = time.strftime('%a, %d %b %Y %H:%M:%S +0000', time.gmtime())
		tosign = timestamp
		h = hmac.new(self._secret, tosign, digestmod=hashlib.sha1)
		signature = base64.b64encode(h.digest())
		auth = 'AWS3-HTTPS AWSAccessKeyId=%s,Algorithm=HmacSHA1,Signature=%s' % (self._key, signature)
#		print repr((tosign, timestamp, auth))
#		raise SystemExit
		if verb != 'GET':
			return {'Date': timestamp, 'X-Amzn-Authorization': auth, 'Content-Type': self._contentType}
		return {'Date': timestamp, 'X-Amzn-Authorization': auth}

	def execute(self):
		conn = httplib.HTTPSConnection(self._host)
		conn.request(self._verb, self.makePath(self._verb), self._body, self.makeHeaders(self._verb))
		resp = conn.getresponse()
		return self.handle(resp.status, resp.reason, resp.read())


class Route53(AWSService):
	endpoints = {
		'us-east-1': 'route53.amazonaws.com',
		'us-west-1': 'route53.amazonaws.com',
		'eu-west-1': 'route53.amazonaws.com',
		'ap-southeast-1': 'route53.amazonaws.com',
		'ap-northeast-1': 'route53.amazonaws.com',
	}
	version = '2011-05-05'
	versionHeader = '/' + version + '/'
	xmlns = 'https://route53.amazonaws.com/doc/2011-05-05/'
	req = Route53Request

	def findadd(self, m, node, attr):
		for part in attr.split('.'):
			if node is None:
				break
			node = node.find('{%s}%s' % (self.xmlns, part))
		if node is not None:
			m[attr] = node.text

	def __init__(self, region, key, secret, version=None):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret


	def ListHostedZones(self, Marker=None, MaxItems=None):
		"""
		"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				zones = {}
				for node in root.findall('.//{%s}HostedZone' % self.xmlns):
					zone = {}
					for attr in ('Id', 'Name', 'CallerReference', 'Config.Comment'):
						self.findadd(zone, node, attr)
					zones[zone['Name']] = zone
				return zones
			raise AWSError(status, reason, data)

		return self.req(self._endpoint, self.version, 'hostedzone', self._key, self._secret, {
				'Marker': Marker,
				'MaxItems': MaxItems,
			}, response, None, 'GET')


	def _tostring(self, el):
		return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(el)


	def CreateHostedZone(self, Name, Comment=None):
		"""
		"""

		def response(status, reason, data):
			if status == 201:	# created
#				print 'created', data
#				<CreateHostedZoneResponse xmlns="https://route53.amazonaws.com/doc/2011-05-05/"><HostedZone><Id>/hostedzone/Z2U5C08S9HUMHF</Id><Name>aksah.com.</Name><CallerReference>1315700475.06</CallerReference><Config/></HostedZone><ChangeInfo><Id>/change/C1T9NL5J086BV6</Id><Status>PENDING</Status><SubmittedAt>2011-09-11T00:21:16.044Z</SubmittedAt></ChangeInfo><DelegationSet><NameServers><NameServer>ns-1623.awsdns-10.co.uk</NameServer><NameServer>ns-752.awsdns-30.net</NameServer><NameServer>ns-287.awsdns-35.com</NameServer><NameServer>ns-1499.awsdns-59.org</NameServer></NameServers></DelegationSet></CreateHostedZoneResponse>
				root = ET.fromstring(data)
				return root.find('.//{%s}HostedZone' % self.xmlns).find('{%s}Id' % self.xmlns).text
			raise AWSError(status, reason, data)

#		ET.register_namespace("", self.xmlns)
#		ET._namespace_map[self.xmlns] = ""
		root = ET.Element('{%s}CreateHostedZoneRequest' % self.xmlns)
		ET.SubElement(root, '{%s}Name' % self.xmlns).text = Name
		if Comment is not None:
			config = ET.SubElement(root, '{%s}HostedZoneConfig' % self.xmlns)
			ET.SubElement(config, '{%s}Comment' % self.xmlns).text = Comment
		ET.SubElement(root, '{%s}CallerReference' % self.xmlns).text = str(time.time())
		body = self._tostring(root)

		return self.req(self._endpoint, self.version, 'hostedzone', self._key, self._secret, {}, response, None, 'POST', self._tostring(root))


	def GetHostedZone(self, ZoneId):
		pass


	def DeleteHostedZone(self, ZoneId):
		"""Delete a zone. It must be empty."""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		return self.req(self._endpoint, self.version, 'hostedzone/' + ZoneId.split('/')[-1], self._key, self._secret, {}, response, None, 'DELETE')


	def ListResourceRecordSets(self, ZoneId, name=None, type=None, identifier=None, maxItems=None):
		"""
		"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				records = []
				for node in root.findall('.//{%s}ResourceRecordSet' % self.xmlns):
					name = node.find('{%s}Name' % self.xmlns).text
					if name.startswith('\\052'):
						name = '*' + name[4:]	# XXX: perform correct replacement here
					typ = node.find('{%s}Type' % self.xmlns).text
					ttl = node.find('{%s}TTL' % self.xmlns).text
					values = []
					for v in node.findall('.//{%s}Value' % self.xmlns):
						values.append(v.text)
					records.append((name, typ, int(ttl), values))
				return records
			raise AWSError(status, reason, data)

		return self.req(self._endpoint, self.version, 'hostedzone/' + ZoneId.split('/')[-1] + '/rrset', self._key, self._secret, {
				'name': name,
				'type': type,
				'identifier': identifier,
				'maxItems': maxItems,
			}, response, None, 'GET')


	def ChangeResourceRecord(self, ZoneId, Create=None, Delete=None, Comment=None):
		"""
			Create and Delete are iterables of change actions, in the form:
				(name, type, ttl, values)
					name - the name of the domain you want to perform the action on
					type - one of A | AAAA | CNAME | MX | NS | PTR | SOA | SPF | SRV | TXT
					ttl - The resource record cache time to live (TTL), in seconds
					values - The current or new DNS record values, not to exceed 4,000 characters each. In the case of a DELETE action,
							if the current value does not match the actual value, an error is returned. For descriptions about how
							to format Value for different record types, see Appendix A: Domain Name and Resource Record Formats
							in the Amazon Route 53 Developer Guide.
			"""

		def response(status, reason, data):
			if status == 200:
				print data
				root = ET.fromstring(data)
				ret = {}
				info = root.find('{%s}ChangeInfo' % self.xmlns)
				self.findadd(ret, info, 'Id')
				self.findadd(ret, info, 'Status')
				self.findadd(ret, info, 'SubmittedAt')
				return ret
			raise AWSError(status, reason, data)

#		ET.register_namespace("", self.xmlns)
#		ET._namespace_map[self.xmlns] = ""
		root = ET.Element('{%s}ChangeResourceRecordSetsRequest' % self.xmlns)
		batch = ET.SubElement(root, '{%s}ChangeBatch' % self.xmlns)
		if Comment is not None:
			ET.SubElement(batch, '{%s}Comment' % self.xmlns).text = Comment
		changes = ET.SubElement(batch, '{%s}Changes' % self.xmlns)

		items = []
		if Delete:
			items.extend([('DELETE', name, typ, ttl, values) for name, typ, ttl, values in Delete])
		if Create:
			items.extend([('CREATE', name, typ, ttl, values) for name, typ, ttl, values in Create])

		for action, name, typ, ttl, values in items:
			change = ET.SubElement(changes, '{%s}Change' % self.xmlns)
			ET.SubElement(change, '{%s}Action' % self.xmlns).text = action
			rrset = ET.SubElement(change, '{%s}ResourceRecordSet' % self.xmlns)
			ET.SubElement(rrset, '{%s}Name' % self.xmlns).text = name
			ET.SubElement(rrset, '{%s}Type' % self.xmlns).text = typ
			ET.SubElement(rrset, '{%s}TTL' % self.xmlns).text = str(ttl)
			records = ET.SubElement(rrset, '{%s}ResourceRecords' % self.xmlns)
			for value in values:
				ET.SubElement(ET.SubElement(records, '{%s}ResourceRecord' % self.xmlns), '{%s}Value' % self.xmlns).text = value

		body = self._tostring(root)
		print body

		return self.req(self._endpoint, self.version, 'hostedzone/' + ZoneId.split('/')[-1] + '/rrset', self._key, self._secret, {}, response, None, 'POST', self._tostring(root))


	def GetChange(self, changeId):
		pass


if __name__ == '__main__':
	import proxy
	key, secret = getBotoCredentials()
	dns = proxy.ServiceProxy(Route53('us-west-1', key, secret))
	zones = dns.ListHostedZones()
	print zones.keys()
	zone = zones['snitch.co.nz.']['Id']
	print zone
	for record in dns.ListResourceRecordSets(zone):
		print repr(record)
#	print dns.ChangeResourceRecord(zone, Create=[('smith.snitch.co.nz', 'A', 86400, ('209.160.32.205',))])
#	print dns.CreateHostedZone('snitch.co.nz.')


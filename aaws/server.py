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
#	Run a server that acts similarly to the Amazon Web Services.
#	This uses the built-in python HTTP server. Please do not expose this
#	to the internet at large; it's intended for use in physical or virtual
#	local area networks.
#

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import urlparse
import cgi
import aws
from formencode import validators, Schema, Invalid, ForEach, variabledecode
import request
import urllib
import hmac
import hashlib
import base64


class ServiceRequestHandler(BaseHTTPRequestHandler):
	maxContentLength = 65536

	def updatekws(self, kws, parms):
		for (key, value) in parms:
			if key in kws:
				if isinstance(kws[key], list):
					kws[key].append(value)
				else:
					kws[key] = [kws[key], value]
			else:
				kws[key] = value

	def updatekws_dict(self, kws, parms):
		for key, value in parms.items():
			if len(value) == 1:
				kws[key] = value[0]
			else:
				kws[key] = value

	def do_POST(self):
		scheme, netloc, path, params, query, fragment = urlparse.urlparse(self.path)
		kws = {}
		contentLength = int(self.headers.get('Content-Length', 0))
		if contentLength > 0:
			contentLength = max(contentLength, self.maxContentLength)
			postdata = self.rfile.read(contentLength)
			self.updatekws(kws, cgi.parse_qsl(postdata, True))
		self.updatekws(kws, cgi.parse_qsl(query, True))
		self.do('POST', path, kws)

	def do_GET(self):
		scheme, netloc, path, params, query, fragment = urlparse.urlparse(self.path)
		kws = {}
		self.updatekws(kws, cgi.parse_qsl(query, True))
		self.do('GET', path, kws)

	def do(self, verb, path, kws):
#		print self.headers
		host = self.headers.get('Host')
#		print verb, path, kws
		if self.server.authenticate(verb, host, path, kws):
			action = kws.pop('Action')
			(status, message), data = self.server.dispatch(path, action, kws)
		else:
			(status, message), data = self.server.error(401)

		self.send_response(status, message)
		self.send_header('Content-type', 'text/xml')
		self.end_headers()
		self.wfile.write(data)

	def log_request(self, code=None, size=None):
		return BaseHTTPRequestHandler.log_request(self, code, size)


class ServiceServer(HTTPServer):

	def __init__(self, server_address, getCredentials, errorHandler, requestHandler=ServiceRequestHandler):
		HTTPServer.__init__(self, server_address, requestHandler)
		self._dispatch = {}
		self._getCredentials = getCredentials
		self._errorHandler = errorHandler

	def authenticate(self, verb, host, path, kws):
		secret = self._getCredentials(kws['AWSAccessKeyId'])
		if secret is None:
			return False
		Signature = kws.pop('Signature')
		parms = []
		for key in sorted(kws.keys()):
			parms.append(urllib.quote(key, safe='') + '=' + urllib.quote(kws[key], safe='-_~'))
		tosign = '%s\n%s\n%s\n%s' % (verb, host, path, '&'.join(parms))
		h = hmac.new(secret, tosign, digestmod=hashlib.sha256)
		digest = base64.b64encode(h.digest())
		SignatureVersion = kws.pop('SignatureVersion')
		AWSAccessKeyId = kws.pop('AWSAccessKeyId')
		SignatureMethod = kws.pop('SignatureMethod')
		Timestamp = kws.pop('Timestamp')
		return digest == Signature

	def error(self, code):
		"""Create an error and return it. XXX: code needs work"""
		fmt = """\
<ErrorResponse xmlns="http://webservices.amazon.com/AWSFault/2005-15-09">
	<Error>
		<Type>%(type)s</Type>
		<Code>%(code)s</Code>
		<Message>%(msg)s</Message>
	</Error>
	<RequestId>6470320e-a2f0-11e0-b35e-3d0d5abb43c4</RequestId>
</ErrorResponse>"""
		if code == 400:
			return ((400, 'Invalid Parameters'), fmt % {'type': 'Sender', 'code': 'InvalidParameters', 'msg': 'Supplied parameters not correct for this action'})
		elif code == 401:
			return ((401, 'Not authorised'), fmt % {'type': 'Sender', 'code': 'NotAuthorised', 'msg': 'Supplied accessId or signature is incorrect'})
		elif code == 404:
			return ((404, 'Not found'), fmt % {'type': 'Sender', 'code': 'NotFound', 'msg': 'Requested action not found'})
		else:	# code == 500:
			return ((500, 'Internal Server Error'), fmt % {'type': 'Receiver', 'code': 'InternalError', 'msg': 'Internal error in service'})

	def register(self, cls, actionName=None):
		if actionName is None:
			actionName = cls.__name__
		if actionName not in self._dispatch:
			self._dispatch[actionName] = []
		self._dispatch[actionName].append(cls())

	def dispatch(self, path, action, parms):
#		print 'DISPATCH', path, action, parms
		if action in self._dispatch:
			version = parms.pop('Version')
			for obj in self._dispatch[action]:
				if version in obj.versions:
					break
			else:
				return self.error(404)
			parms = variabledecode.variable_decode(parms, dict_char='-', list_char='.')
			try:
				parms = obj.schema.to_python(parms)
				return obj.invoke(**parms)
			except Invalid, e:
				return self.error(400)
			except:
				_, t, v, tbinfo = request.compact_traceback()
				self._errorHandler(t, v, tbinfo)
				return self.error(500)
		else:
			return self.error(404)


if __name__ == '__main__':
	key, secret = aws.getBotoCredentials()

	class ExampleAction(object):
		versions = ['2011-06-30']

		class schema(Schema):
			Title = validators.String()
			FirstName = validators.String()
			Surname = validators.String(if_missing=None)

		def invoke(self, Title, FirstName, Surname):
			print "Hello %s %s %s" % (Title, FirstName, Surname)
			return ((200, 'OK'), '')

	class ListAction(object):
		versions = ['2011-06-30']

		class schema(Schema):
			Element = ForEach(validators.String())

		def invoke(self, Element):
			print repr(Element)
			return ((200, 'OK'), '')

	def getCredentials(accessKeyId):
		if accessKeyId == key:
			return secret
		return None

	def errHandler(t, v, tbinfo):
		print 'exception %s:%s %s' % (t, v, tbinfo)

	server = ServiceServer(('', 8080), getCredentials, errHandler)
	server.register(ExampleAction)
	server.register(ListAction)

	try:
		print "Serving now..."
		server.serve_forever()
	except KeyboardInterrupt:
		server.socket.close()


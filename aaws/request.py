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
#		request.py,
#
#			This module handles requests to AWS services. It tries to be agnostic to which AWS service it is talking to.
#			It is responsible for urlencoding, AWS signing, and creation of URLs to make requests.
#			It is also responsible for doing the HTTP to make the request itself.
#			Both synchronous and asynchronous requests are supported.
#
#

# XXX: improve error handling (for retries, etc):
# e.g.
#    raise AWSError(status, reason, data)
#aaws.aws.AWSError: (503 Service Unavailable)
#<?xml version="1.0"?>
#<Response><Errors><Error><Code>ServiceUnavailable</Code>
#<Message>Service AmazonSimpleDB is currently unavailable. Please try again later</Message></Error></Errors>
#<RequestID>637e5d3f-e869-57a0-0d69-9e2023a39ca3</RequestID></Response>


import urllib
import hmac
import hashlib
import base64
import time
import httplib
import asyncore
import StringIO
import mimetools
import socket
import sys


def compact_traceback():
	t, v, tb = sys.exc_info()
	tbinfo = []
	if not tb: # Must have a traceback
		raise AssertionError("traceback does not exist")
	while tb:
		tbinfo.append((tb.tb_frame.f_code.co_filename, tb.tb_frame.f_code.co_name, str(tb.tb_lineno)))
		tb = tb.tb_next

	# just to be safe
	del tb

	file, function, line = tbinfo[-1]
	info = ' '.join(['[%s|%s|%s]' % x for x in tbinfo])
	return (file, function, line), t, v, info


class AWSRequestManager(object):

	def __init__(self):
		self._map = {}
		self._incomplete = []
		self._good = []
		self._bad = []

	def add(self, request):
		request.GETAsync(self, self._map)
		self._incomplete.append(request)

	def reqComplete(self, request, success, result):
		if request in self._incomplete:
			self._incomplete.remove(request)
			request.result = result
			if success:
				self._good.append(request)
			else:
				self._bad.append(request)

	def run(self, timeout=None):
		"""Process all added requests until they are complete or timeout is reached (if supplied)"""
		# XXX: timeout not supported yet, supplying timeout to loop will not work
		asyncore.loop(map=self._map)
		return self._good, self._bad, self._incomplete



class AWSRequest(asyncore.dispatcher_with_send):

	def __init__(self, host, uri, key, secret, action, parameters, handler=None):
		asyncore.dispatcher_with_send.__init__(self)

		self._host = host
		self._uri = uri
		self._key = key
		self._secret = secret
		self._parameters = {}
		for key, value in parameters.items():
			if value is not None:
				self._parameters[key] = str(value)
		self._action = action
		if handler is not None:
			self.handle = handler

	def addParm(self, name, value):
		if value is not None:
			if value == True:
				self._parameters[name] = 'true'
			elif value == False:
				self._parameters[name] = 'false'
			else:
				self._parameters[name] = str(value)

	def handle(self, status, reason, data):
		print 'Default Handler: %r %r %r' % (status, reason, data)
		return data

	# Sync methods
	def GET(self):
		conn = httplib.HTTPConnection(self._host)
		conn.request('GET', self.makePath())
		resp = conn.getresponse()
		data = resp.read()
		return self.handle(resp.status, resp.reason, data)

	def POST(self):
		raise NotImplementedError

	# Async methods
	def GETAsync(self, manager, _map):
		self._map = _map
		self._manager = manager
		self._rx = []
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect((self._host, 80))

	def handle_connect(self):
		self.send("GET %s HTTP/1.0\r\nHost: %s\r\n\r\n" % (self.makePath(), self._host))

	def handle_expt(self):
		self._manager.reqComplete(self, False, 'connect')
		self.close()

	def handle_error(self):
		_, t, v, tbinfo = compact_traceback()
		self._manager.reqComplete(self, False, 'exception %s:%s %s' % (t, v, tbinfo))
		self.close()

	def handle_read(self):
		data = self.recv(2048)
		if len(data) > 0:
			self._rx.append(data)

	def handle_close(self):
		try:
			data = ''.join(self._rx)
			header, data = data.split('\r\n\r\n', 1)
			fp = StringIO.StringIO(header)
			_, status, reason = fp.readline().split(' ', 2)
			header = mimetools.Message(fp)
			result = self.handle(int(status), reason, data)
			self._manager.reqComplete(self, True, result)
		except ValueError, e:
			self._manager.reqComplete(self, False, e)
		self.close()

	def makeURL(self):
		return 'http://' + self._host + self.makePath()

	def makePath(self):
		parameters = self._parameters
		parameters['Action'] = self._action
		parameters['AWSAccessKeyId'] = self._key
		parameters['SignatureMethod'] = 'HmacSHA256'
		parameters['SignatureVersion'] = '2'
		if 'Timestamp' not in parameters:
			parameters['Timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
		parms = []
		for key in sorted(parameters.keys()):
			parms.append(urllib.quote(key, safe='') + '=' + urllib.quote(parameters[key], safe='-_~'))
#			parms.append('%s=%s' % (key, urllib.quote(parameters[key])))
		tosign = '%s\n%s\n%s\n%s' % ('GET', self._host, self._uri, '&'.join(parms))
		h = hmac.new(self._secret, tosign, digestmod=hashlib.sha256)
#		print '%r' % tosign
		digest = base64.b64encode(h.digest())
#		print 'base64 digest %r (%s)' % (digest, h.hexdigest())
		parms.append('Signature=' + urllib.quote(digest, safe='-_~'))
		return self._uri + '?' + '&'.join(parms)


if __name__ == '__main__':
	key, secret = getBotoCredentials()

	if False:
		r = AWSRequest('sqs.us-west-1.amazonaws.com', '/', key, secret)
		print r.makeURL('ListQueues', {
	#			'MessageBody': 'Your Message Text',
				'Version': '2009-02-01',
	#			'Expires': '2008-02-10T12:00:00Z',
			})
	r = AWSRequest('us-west-1.queue.amazonaws.com', '/', key, secret, 'CreateQueue', {
			'Version': '2009-02-01',
			'QueueName': 'pointstore',
		})
	print r.makeURL()

	m = AWSRequestManager()
	m.add(AWSRequest('sqs.us-west-1.amazonaws.com', '/', key, secret, 'CreateQueue', {
			'Version': '2009-02-01',
			'QueueName': 'pointstore',
		}))
	m.add(AWSRequest('sqs.us-west-1.amazonaws.com', '/', key, secret, 'CreateQueue', {
			'Version': '2009-02-01',
			'QueueName': 'pointstore',
		}))
	g, b, i = m.run()
	for req in g:
		print req.result
	for req in b:
		print 'Fail', req.result


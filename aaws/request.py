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
import aws
import proxy


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
		self.clear()

	def clear(self):
		self._map = {}
		self._incomplete = []
		self._good = []
		self._bad = []

	def add(self, request):
		request.ExecAsync(self, self._map)
		self._incomplete.append(request)

	def addService(self, name, service):
		setattr(self, name, proxy.ManagerProxy(self, service))

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

	def execute(self, retries=5, follow=10):
		def byIndex(l, r):
			return cmp(l._idx, r._idx)
		done = []
		errors = []
		for idx, req in enumerate(self._incomplete):
			if req._follows is None:
				req._follows = follow
			if req._retries is None:
				req._retries = retries
			req._idx = idx
			req._accum = None
		while True:
			g, b, i = self.run()
			tofollow = []
			if follow:
				for req in g:
					if req.follow(req):
						req._follows -= 1
						if req._follows < 0:
							errors.append(aws.AWSError(-1, 'follows exceeded', req))
							raise aws.AWSCompoundError(errors)
						tofollow.append(req)
					else:
						req.result = req._accum
						done.append(req)
			else:
				done.extend(g)
			if len(b) == 0 and len(i) == 0 and len(tofollow) == 0:
				done.sort(cmp=byIndex)
				return done
			self.clear()
			for req in tofollow:
				self.add(req)
			for req in b + i:
				self.add(req)
				errors.append(req.result)
				req._retries -= 1
				if req._retries < 0:
					raise aws.AWSCompoundError(errors)


def ListFollow(req):
	"""This is a follower that expects a result in the form (list_of_things, NextToken).
		If NextToken is not None then we return a copied request with the NextToken parameter set
		to the returned NextToken, and accumulate the list_of_things.
		"""
	if req._accum is None:
		req._accum = []
	items, token = req.result
	req._accum.extend(items)
	if token is not None:
		req.setParm('NextToken', token)
		return True


class AWSRequest(asyncore.dispatcher_with_send):

	def __init__(self, host, uri, key, secret, action, parameters, handler=None, follower=None, verb='GET'):
		asyncore.dispatcher_with_send.__init__(self)

		self._host = host
		self._uri = uri
		self._key = key
		self._secret = secret
		self._parameters = {}
		self._verb = verb
		for key, value in parameters.items():
			if value is not None:
				self._parameters[key] = str(value)
		self._action = action
		if handler is not None:
			self.handle = handler
		if follower is not None:
			self.follow = follower
		self._follows = None
		self._retries = None

	def copy(self):
		return AWSRequest(self._host, self._uri, self._key, self._secret, self._action, self._parameters, self.handle)

	def addParm(self, name, value):
		if value is not None:
			if value == True:
				self._parameters[name] = 'true'
			elif value == False:
				self._parameters[name] = 'false'
			else:
				self._parameters[name] = str(value)

	def setParm(self, name, value):
		if name in self._parameters:
			self._parameters.pop(name)
		self.addParm(name, value)

	def handle(self, status, reason, data):
		print 'Default Handler: %r %r %r' % (status, reason, data)
		return data

	def follow(self, request):
		# Default follow handler does not follow (We can't assume a NextToken)
		request._accum = request.result

	# Sync methods
	def _attemptReq(self, req, verb):
		conn = httplib.HTTPConnection(req._host)
		conn.request(verb, req.makePath(verb), req.makeBody(), req.makeHeaders(verb))
		resp = conn.getresponse()
		return resp.status, resp.reason, resp.read()

	def _execute(self, verb, retries=5, follow=10):
		nFollow = follow
		current = self
		accumulator = None
		while True:
			try:
				status, reason, data = self._attemptReq(current, verb)
				result = self.handle(status, reason, data)
				if follow:
					current, accumulator = self.follow(current, result, accumulator)
					if current is None:
						return accumulator
					if --nFollow <= 0:
						raise aws.AWSError(-1, 'Number of follows exceeded', data)
				else:
					return result
			except aws.AWSError:
				if retries <= 0:
					raise		# out of retries
				retries -= 1

	def execute(self, retries=5, follow=10):
		mgr = AWSRequestManager()
		mgr.add(self)
		return mgr.execute(retries, follow)[0].result

	def GET(self, retries=5, follow=10):
		# XXX: deprecated
		return self._execute('GET', retries, follow)

	# Async methods
	def ExecAsync(self, manager, _map):
		self._map = _map
		self._manager = manager
		self._rx = []
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect((self._host, 80))

	def handle_connect(self):
		request = '%s %s HTTP/1.0\r\n' % (self._verb, self.makePath())
		headers = ['Host: %s' % self._host]
		if self._verb in ('PUT', 'POST'):
			headers.append('Content-Length: %d' % self.getContentLength())
		extra = self.makeHeaders(self._verb)
		if extra is not None:
			headers.extend(['%s: %s' % (key, value) for key, value in extra.items()])
		self.send(request + '\r\n'.join(headers) + '\r\n\r\n')
		body = self.makeBody()
		if body:
			self.send(body)

	def handle_expt(self):
		self._manager.reqComplete(self, False, 'connect')
		self.close()

	def handle_error(self):
		_, t, v, tbinfo = compact_traceback()
		print 'channel error', str(v)
		self._manager.reqComplete(self, False, v)#'exception %s:%s %s' % (t, v, tbinfo))
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

	def makePath(self, verb='GET'):
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
		tosign = '%s\n%s\n%s\n%s' % ('GET', self._host, urllib.quote(self._uri), '&'.join(parms))
		h = hmac.new(self._secret, tosign, digestmod=hashlib.sha256)
#		print '%r' % tosign
		digest = base64.b64encode(h.digest())
#		print 'base64 digest %r (%s)' % (digest, h.hexdigest())
		parms.append('Signature=' + urllib.quote(digest, safe='-_~'))
		return urllib.quote(self._uri) + '?' + '&'.join(parms)

	def makeHeaders(self, verb='GET'):
		pass

	def makeBody(self):
		return ''

	def getContentLength(self):
		return len(self.makeBody())


if __name__ == '__main__':
	key, secret = aws.getBotoCredentials()

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


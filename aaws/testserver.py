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
#	Tests server.py
#

from aws import AWSService, AWSError, getBotoCredentials
import request


class ExampleService(AWSService):
	endpoints = {
		'localhost': 'localhost:8080',
	}
	xmlns = 'http://aaws.code.google.com/doc/2010-06-30/'
	version = '2011-06-30'

	def __init__(self, region, key, secret, version=None):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret

	def ExampleAction(self, Title, FirstName, Surname=None):
		"""exampleAction prints a greeting on the console where the server runs.

			Returns -- True if HTTP request succeeds
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ExampleAction', {
				'Version': self.version,
				'Title': Title,
				'FirstName': FirstName,
				'Surname': Surname,
			}, response)


	def ListAction(self, Elements):
		"""listAction tests that lists pass correctly through the system

			Returns -- True if HTTP request succeeds
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)

		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ListAction', {
				'Version': self.version,
			}, response)
		for idx, e in enumerate(Elements):
			r.addParm('Element.%d' % idx, e)
		return r


if __name__ == '__main__':
	from proxy import ServiceProxy
	k, s = getBotoCredentials()
	es = ServiceProxy(ExampleService('localhost', k, s))
	es.ExampleAction('Mr', 'Joe', 'Bloggs')
	es.ExampleAction('Mrs', 'Madonna')
	es.ListAction(['one', 'two', 'three'])
	es.ListAction([])
	es.ExampleAction('Bad', None)


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

class ServiceProxy(object):
	_is_proxy = True

	def __init__(self, service):
		if hasattr(service, '_is_proxy'):
			self._service = service._service
		else:
			self._service = service
		for methname in dir(self._service):
			if 'A' <= methname[0] <= 'Z':
				method = getattr(self._service, methname)
				if hasattr(method, '__call__'):
#					print methname, method, dir(method)
					self.proxy(methname, method)

	def proxy(self, methname, method):
		def thunk(*args, **kws):
			return method(*args, **kws).execute()
		setattr(self, methname, thunk)


class ManagerProxy(object):
	_is_proxy = True

	def __init__(self, mgr, service):
		if hasattr(service, '_is_proxy'):
			self._service = service._service
		else:
			self._service = service
		for methname in dir(self._service):
			if 'A' <= methname[0] <= 'Z':
				method = getattr(self._service, methname)
				if hasattr(method, '__call__'):
#					print methname, method, dir(method)
					self.proxy(mgr, methname, method)

	def proxy(self, mgr, methname, method):
		def thunk(*args, **kws):
			mgr.add(method(*args, **kws))
		setattr(self, methname, thunk)


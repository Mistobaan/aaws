#!/usr/bin/env python
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

import sys
import optparse
import subprocess
import os
# add parent directory to path (works even when cwd is not script's directory)
sys.path.append(os.path.normpath(os.path.join(sys.path[0], '..')))
import aaws


def ec2ssh(ec2, kws, iid, options):
	def lower(s):
		if s is None:
			return None
		return s.lower()
	connectTo = None
	for i in ec2.DescribeInstances().execute():
		if iid is None:
			tags = [(lower(k), lower(v)) for k, v in i['tags'].items()]
			for k, v in kws.items():
				if (k.lower(), v.lower()) not in tags:
#					print i['tags']
#					print (k.lower(), v.lower())
#					print tags
					break
			else:
				connectTo = i
		else:
			if iid == i['instanceId']:
				connectTo = i
	cmd = None
	if connectTo['instanceState.name'] == 'running':
		cmd = '%s %s@%s %s' % (options.externalcommand, options.user, connectTo['ipAddress'], options.command)
	print cmd
	if not options.dryrun:
		os.system(cmd)


if __name__ == '__main__':
	k, s = aaws.getBotoCredentials()
	parser = optparse.OptionParser(usage='usage: %prog [options] (key=value)* [instanceid]', version='%prog 1.00')
	parser.add_option('-k', '--key', help='Specify AWS key (default from .boto)', default=k)
	parser.add_option('-s', '--secret', help='Specify AWS secret (default from .boto)', default=s)
	parser.add_option('-r', '--region', help='Specify region to connect to (default us-west-1)', default='us-west-1')
	parser.add_option('-n', '--dryrun', action='store_true', help='Dont execute anything, just print what command would have been executed', default=False)
	parser.add_option('-e', '--externalcommand', help='External command', default='ssh')
	parser.add_option('-u', '--user', help='User to use for ssh', default='root')
	parser.add_option('-c', '--command', help='Remote command to execute', default='')

	(options, args) = parser.parse_args()

	ec2 = aaws.EC2(options.region, options.key, options.secret)

	if len(args) > 0:
		kws = {}
		iid = None
		for arg in args:
			if '=' in arg:
				k, v = arg.split('=', 2)
				kws[k] = v
			else:
				iid = arg
		ec2ssh(ec2, kws, iid, options)
		sys.exit(0)

	cols = '%-12s%-18s%-12s%-35s'
	print cols % ('Instance', 'IP', 'Type', 'Tags')
	for i in ec2.DescribeInstances().execute():
		if i['instanceState.name'] == 'running':
			print cols % (i['instanceId'], i['ipAddress'], i['instanceType'], ','.join('%s=%s' % (k, v) for k, v in i['tags'].items()))
		else:
			print cols % (i['instanceId'], '', i['instanceType'], ','.join('%s=%s' % (k, v) for k, v in i['tags'].items()))


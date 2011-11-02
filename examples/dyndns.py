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
#

import sys
import optparse
import subprocess
import os
import time
# add parent directory to path (works even when cwd is not script's directory)
sys.path.append(os.path.normpath(os.path.join(sys.path[0], '..')))
import aaws


def findHost(rrs, host):
	for name, typ, ttl, values in rrs:
		if name == host and typ == 'A':
			return (name, typ, ttl, values)
	return None


def dyndns(dns, options):
	ip = subprocess.Popen(options.ipcmd, shell=True, stdout=subprocess.PIPE).communicate()[0].strip()
	if options.verbose:
		print 'Local IP', ip
	rrs = dns.ListResourceRecordSets(options.zoneId)
	existing = findHost(rrs, options.host)
	prevIP = None
	if existing is not None:
		prevIP = existing[3][0]
	if options.verbose:
		print 'Previous', prevIP
	delete = []
	if prevIP != ip:
		if options.changecmd:
			if options.verbose:
				print 'Changecmd %r %r' % (options.changecmd, ip)
			subprocess.call([options.changecmd, ip], shell=False)
		if existing is not None:
			delete = [existing]
		create = [(options.host, 'A', options.ttl, [ip])]
		print dns.ChangeResourceRecord(options.zoneId, Create=create, Delete=delete)


if __name__ == '__main__':
	k, s = aaws.getBotoCredentials()
	parser = optparse.OptionParser(usage='usage: %prog [options] <zone> <host> <ipcmd>', version='%prog 1.00')
	parser.add_option('-k', '--key', help='Specify AWS key (default from .boto)', default=k)
	parser.add_option('-s', '--secret', help='Specify AWS secret (default from .boto)', default=s)
	parser.add_option('-r', '--region', help='Specify region to connect to (default us-west-1)', default='us-west-1')
	parser.add_option('-t', '--ttl', type='int', help='Time to live for DNS record', default=30)
	parser.add_option('-c', '--changecmd', help='Execute command when IP changes', default=None)
	parser.add_option('-v', '--verbose', action='store_true', help='Verbose output', default=False)

	(options, args) = parser.parse_args()
	if len(args) < 3:
		parser.print_usage()
		raise SystemExit
	zname, host, ipcmd = args[:3]

	dns = aaws.ServiceProxy(aaws.Route53(options.region, options.key, options.secret))
	zones = dns.ListHostedZones()
	if zname not in zones:
		print "Invalid zone, available", zones.keys()
	else:
		zoneId = zones[zname]['Id']
		options.zoneId = zoneId
		options.host = host
		options.ipcmd = ipcmd
		dyndns(dns, options)


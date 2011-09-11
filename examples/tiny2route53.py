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
# Reference: http://cr.yp.to/djbdns/tinydns-data.html
#

import sys
sys.path.append('..')
import aaws
import optparse
import subprocess
import os

def getData(dns):
	zones = dns.ListHostedZones()
	for key in zones.keys():
		zoneId = zones[key]['Id']
		rrs = dns.ListResourceRecordSets(zoneId)
		zones[key]['Records'] = rrs
	return zones

def untrail(v):
	if v.endswith('.'):
		return v[:-1]
	return v

def fromTiny(lines, defaultTtl=86400):
	def toInt(v, default):
		if v in ('', '\n'):
			return default
		return int(v)

	def add(typ, name, value, ttl):
		key = (name + '.', typ, toInt(ttl, defaultTtl))
		if key in rrs:
			rrs[key].append(value)
		else:
			rrs[key] = [value]

	def parms(line):
		a = line[1:].strip().split(':')
		while len(a) < 5:
			a.append('')
		return a

	rrs = {}
	for n, line in enumerate(lines):
		try:
			a = parms(line)
			if line.startswith('#') or line.startswith('\n'):
				pass
			elif line.startswith('.'):
				add('NS', a[0], a[2] + '.', a[3])
			elif line.startswith('+'):
				add('A', *a[:3])
			elif line.startswith('&'):
				add('&', a[0], a[2] + '.', a[3])		# must be fixed up when placing in zones
			elif line.startswith('='):
				add('A', *a[:3])
			elif line.startswith('@'):
				add('MX', a[0], '%d %s' % (toInt(a[3], 0), a[2]), a[4])
			elif line.startswith('\''):
				pass		# SPF
			elif line.startswith(':'):
				pass		# General
			elif line.startswith('C'):
				add('CNAME', *a[:3])
			else:
				raise ValueError('line type: %s' % line[0])
		except:
			print "Error on line %d '%s'" % (n, line)
			raise
	return rrs


def byZone(data):
	zones = {}
	zonerrs = onlyrrs(data, ('NS',))
	for (name, typ, ttl) in zonerrs.keys():
		zones[name] = {}
	zonenames = zones.keys()
	for (name, typ, ttl), values in data.items():
		if typ != 'NS':
			for z in zonenames:
				if name.endswith(z):
					if typ == '&':
						typ = 'NS'		# fixup re fromTiny; there should be a better way of doing this
					zones[z][(name, typ, ttl)] = values
					break
			else:
				raise ValueError("Record %s is not in a zone" % repr((name, typ, ttl)))
	return zones


def toTiny(zones):
	lines = []
	for zone in zones.values():
		for name, typ, ttl, values in zone['Records']:
			name = untrail(name)
			if typ == 'A':
				for val in values:
					lines.append('+%s:%s:%d' % (name, val, ttl))
			elif typ == 'AAAA':
				raise NotImplemented()
			elif typ == 'CNAME':
				raise NotImplemented()
			elif typ == 'MX':
				raise NotImplemented()
			elif typ == 'NS':
				for val in values:
					lines.append('.%s::%s:%d' % (name, untrail(val), ttl))
			elif typ == 'PTR':
				raise NotImplemented()
			elif typ == 'SOA':
				pass
#				raise NotImplemented()
			elif typ == 'SPF':
				raise NotImplemented()
			elif typ == 'SRV':
				raise NotImplemented()
			elif typ == 'TXT':
				raise NotImplemented()
			else:
				raise NotImplemented()
	return lines


def onlyrrs(data, filt):
	res = {}
	for (name, typ, ttl), values in data.items():
		if typ in filt:
			res[(name, typ, ttl)] = values
	return res


def notrrs(data, filt):
	res = {}
	for (name, typ, ttl), values in data.items():
		if typ not in filt:
			res[(name, typ, ttl)] = values
	return res


def onlyzone(data, zones):
	res = {}
	for name, rrs in data.items():
		if name in zones:
			res[name] = rrs
	return res


def diff(frm, to):
	create = []
	delete = []
	for (name, typ, ttl), frmvalues in frm.items():
		frmvalues.sort()
		if (name, typ, ttl) not in to:
			delete.append((name, typ, ttl, frmvalues))
		else:
			tovalues = to[(name, typ, ttl)]
			tovalues.sort()
			if frmvalues != tovalues:
				delete.append((name, typ, ttl, frmvalues))
				create.append((name, typ, ttl, tovalues))
	for (name, typ, ttl), tovalues in to.items():
		if (name, typ, ttl) not in frm:
			create.append((name, typ, ttl, tovalues))
	return create, delete

def toDict(rrs):
	res = {}
	for name, typ, ttl, values in rrs:
		res[(name, typ, ttl)] = values
	return res


if __name__ == '__main__':
	k, s = aaws.getBotoCredentials()
	parser = optparse.OptionParser(usage='usage: %prog [options]', version='%prog 1.00')
	parser.add_option('-k', '--key', help='Specify AWS key (default from .boto)', default=k)
	parser.add_option('-s', '--secret', help='Specify AWS secret (default from .boto)', default=s)
	parser.add_option('-r', '--region', help='Specify region to connect to (default us-west-1)', default='us-west-1')
	parser.add_option('-n', '--dryrun', action='store_true', help='Dont execute anything, just print what command would have been executed', default=False)
	parser.add_option('-f', '--file', help='Specify the file to save to / read from (otherwise stdin / stdout is used)', default=None)
	parser.add_option('-t', '--type', action='append', help='Add a rrtype to operate on (all when none are provided)', default=[])
	parser.add_option('-z', '--zone', action='append', help='Add a zone to operate on (all when none are provided)', default=[])
	parser.add_option('', '--authority', dest='action', action='store_const', const='authority', help='Get authority for zones', default=None)
	parser.add_option('-u', '--upload', dest='action', action='store_const', const='upload', help='Upload to route53')
#	parser.add_option('-d', '--download', dest='action', action='append_const', const='download', help='Download from route53')

	(options, args) = parser.parse_args()

	dns = aaws.ServiceProxy(aaws.Route53(options.region, options.key, options.secret))
#	lines = toTiny(getData(dns))
#	print '\n'.join(lines)

#	print fromTiny(lines)

	if options.action == 'upload':
		# get and parse a tinydns file
		if options.file is not None:
			fh = file(options.file)
		else:
			fh = sys.stdin
		data = byZone(fromTiny(fh.readlines()))
		if len(options.zone):
			data = onlyzone(data, options.zone)
		if len(options.type):
			for name, rrs in data.items():
				data[name] = onlyrrs(rrs, options.type)
		print data
		# Upload by zone
		zones = dns.ListHostedZones()

		for zname in data.keys():
			if zname not in zones:
				print 'Create zone', zname
				if not options.dryrun:
					zoneId = dns.CreateHostedZone(zname)
			else:
				zoneId = zones[zname]['Id']
			awsrrs = dns.ListResourceRecordSets(zoneId)
			ourrrs = data[zname]
			c, d = diff(notrrs(toDict(awsrrs), ('NS', 'SOA')), ourrrs)
			print 'Sync zone', zname
			for op in d:
				print 'DELETE', op
			for op in c:
				print 'CREATE', op
			if not options.dryrun:
				print dns.ChangeResourceRecord(zoneId, Create=c, Delete=d)

#		for key in zones.keys():
#			zoneId = zones[key]['Id']
#			rrs = dns.ListResourceRecordSets(zoneId)
#			zones[key]['Records'] = rrs


#		for name, typ, ttl in data.keys():
#			print name, typ
#		print data
	elif options.action == 'authority':
		zones = dns.ListHostedZones()
		for zname in zones.keys():
			zoneId = zones[zname]['Id']
			print zname
			for (name, typ, ttl), values in onlyrrs(toDict(dns.ListResourceRecordSets(zoneId)), ('NS,')).items():
				if name == zname:
					print '   %s %d %r' % (typ, ttl, values)
				else:
					print '   => %s %s %d %r' % (name, typ, ttl, values)


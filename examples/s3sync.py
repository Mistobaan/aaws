
import sys
sys.path.append('..')
import aaws
import optparse
import subprocess
import os
import os.path
import time


KB = 1024
MB = 1024 * KB
GB = 1024 * MB

class Meter(object):

	def __init__(self, fname):
		border = '[' + (' ' * 50) + '] ' + fname
		sys.stderr.write(border)
		sys.stderr.write('\x08' * (len(border) - 1))
		sys.stderr.flush()
		self.stars = 0

	def __call__(self, sent, outof):
		if self.stars >= 50:
			return
		target = sent * 50 / outof
		while target > self.stars:
			sys.stderr.write('*')
			sys.stderr.flush()
			self.stars += 1
		if self.stars >= 50:
			sys.stderr.write('\n')
			sys.stderr.flush()


class Counter(object):

	def __init__(self, title):
		sys.stderr.write(title)
		sys.stderr.flush()
		self.nOut = 0

	def __call__(self, counter, done):
		sys.stderr.write('\x08' * self.nOut)
		counter = str(counter)
		sys.stderr.write(counter)
		self.nOut = len(counter)
		if done:
			sys.stderr.write('\n')
		sys.stderr.flush()


def md5sum(path, cache, options):
	if path in cache:
		return cache[path]
	sig = None
	if options.cachesum:
		# Check filesystem
		if os.path.exists(path + '.md5sum'):
			if os.stat(path + '.md5sum').st_mtime > os.stat(path).mtime:
				sig = file(path + '.md5sum').read()
	if sig is None:
		print 'check', path
		if sys.platform.startswith('freebsd'):
			sig = subprocess.Popen(['md5', '-q', path], stdout=subprocess.PIPE).communicate()[0]
		else:
			sig = subprocess.Popen(['md5sum', path], stdout=subprocess.PIPE).communicate()[0]
		sig = sig.split(' ')[0].strip()
	cache[path] = sig
	if options.cachesum:
		file(path + '.md5sum', 'w').write(sig)
	return sig

def mimetype(path):
	return subprocess.Popen(['file', '-b', '--mime-type', path], stdout=subprocess.PIPE).communicate()[0].strip()

def s3pathjoin(prefix, p, options):
	return os.path.join(prefix, p)		# XXX: use options.delimiter

def s3relpath(prefix, p, options):
	if p.startswith(prefix):
		return p[len(prefix):]
	raise ValueError("Smarter relpath required")

def s3syncops(s3, bucket, prefix, path, operations, options):
	for op in operations:
		if options.inhibit:
			while os.path.exists(options.inhibit):
				time.sleep(1.0)
		if op[0] == 'same':
			pass
		elif op[0] == 'download':
			# XXX: use tmpfile for downloads
			_, f = op
			d, _ = os.path.split(f)
			if not os.path.exists(os.path.join(path, d)):
				os.makedirs(os.path.join(path, d))
			s3.GetObject(bucket, s3pathjoin(prefix, f, options), file(os.path.join(path, f), 'wb'), Progress=Meter('D %s' % f)).execute()
		elif op[0] == 'upload':
			_, f, mime = op
			s3.PutObject(bucket, s3pathjoin(prefix, f, options), file(os.path.join(path, f), 'rb'), mime, Progress=Meter('U %s (%s)' % (f, mime))).execute()


def s3sync(bucket, path, options):
	parts = bucket.split(options.delimiter, 1)
	bucket = parts[0]
	prefix = None
	if len(parts) == 2:
		prefix = parts[1]
		if not prefix.endswith(options.delimiter):
			prefix += options.delimiter	# required for the rest of this function to work correctly
	s3 = aaws.S3(options.region, options.key, options.secret)

	# Get filesystem list
	flist = []
	if options.recursive:
		for root, dirs, files in os.walk(path):
			for f in files:
				if not f.endswith('.md5sum'):
					flist.append(os.path.join(os.path.relpath(root, path), f))
		# Get S3 list
		objects = s3.ListObjects(bucket, prefix=prefix, Progress=Counter('Listing files...')).execute()
	else:
		for f in os.listdir(path):
			if not os.path.isdir(os.path.join(path, f)):
				flist.append(f)
		raise NotImplemented("Non-recursive sync is not implemented correctly yet, need to change s3.ListObjects below")

	# Create a list of operations to perform
	operations = []
	sigcache = {}
	for action in options.actions:
		if action == 'download':
			# s3 -> filesystem
			for k, obj in objects.items():
				f = s3relpath(prefix, k, options)
				if os.path.exists(os.path.join(path, f)):
					oursum = md5sum(os.path.join(path, f), sigcache, options)
					if obj['ETag'] == '"%s"' % oursum:
						operations.append(['same', f])
						continue
				operations.append(['download', f])
		elif action == 'upload':
			# filesystem -> s3
			for f in flist:
				if s3pathjoin(prefix, f, options) in objects:
					oursum = md5sum(os.path.join(path, f), sigcache, options)
					if objects[s3pathjoin(prefix, f, options)]['ETag'] == '"%s"' % oursum:
						operations.append(['same', f])
						continue
				mime = mimetype(os.path.join(path, f))
				operations.append(['upload', f, mime])

	if options.dryrun:
		for op in operations:
			print op
	else:
		s3syncops(s3, bucket, prefix, path, operations, options)
#	s3syncfiles(s3, bucket, prefix, path, flist, options)


if __name__ == '__main__':
	k, s = aaws.getBotoCredentials()
	parser = optparse.OptionParser(usage='usage: %prog [options] <bucket> <path>', version='%prog 1.00')
	parser.add_option('-k', '--key', help='Specify AWS key (default from .boto)', default=k)
	parser.add_option('-s', '--secret', help='Specify AWS secret (default from .boto)', default=s)
	parser.add_option('-R', '--recursive', action='store_true', help='Recurse into subdirectories', default=False)
	parser.add_option('-r', '--region', help='Specify region to connect to (default us-west-1)', default='us-west-1')
	parser.add_option('', '--delimiter', help='Specify path delimiter for S3 (default /)', default='/')
	parser.add_option('-i', '--inhibit', help='Pause while the specified file exists (default None)', default=None)
	parser.add_option('-u', '--upload', dest='actions', action='append_const', const='upload', help='Upload to S3', default=[])
	parser.add_option('-d', '--download', dest='actions', action='append_const', const='download', help='Download from S3')
	parser.add_option('-n', '--dryrun', action='store_true', help='Dont copy anything, just tell us what you would have copied', default=False)
	parser.add_option('-c', '--cachesum', action='store_true', help='Cache md5sum on filesystem', default=False)

	(options, args) = parser.parse_args()

	if len(args) == 2:
		s3sync(args[0], args[1], options)
	else:
		parser.print_usage()
		sys.exit(1)


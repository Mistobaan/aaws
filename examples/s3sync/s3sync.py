
import sys
sys.path.append('../..')
import aaws
import optparse
import subprocess


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


def s3syncfiles(s3, bucket, prefix, flist, options):
#	objects, info = s3.ListObjects(bucket, prefix=prefix, delimiter=options.delimiter).execute()

	for f in flist:
		mimetype = subprocess.check_output(['file', '-b', '--mime-type', f])
		s3.PutObject(bucket, f, file(f, 'rb'), mimetype, Progress=Meter('%s (%s)' % (f, mimetype.strip()))).execute()


def s3sync(bucket, path, options):
	parts = bucket.split(options.delimiter, 1)
	bucket = parts[0]
	prefix = None
	if len(parts) == 2:
		prefix = parts[1]
	s3 = aaws.S3(options.region, options.key, options.secret)

	if options.recursive:
		flist = []
		for root, dirs, files in os.walk(path):
			for f in files:
				flist.append(os.path.join(os.path.relpath(root), f))
	else:
		pass	# XXX: flist = os.listdir (without dirs) os.isdir

	print flist
	s3syncfiles(s3, bucket, prefix, flist, options)


if __name__ == '__main__':
	k, s = aaws.getBotoCredentials()
	parser = optparse.OptionParser(usage='usage: %prog [options] <bucket> <path>', version='%prog 1.00')
	parser.add_option('-k', '--key', help='Specify AWS key (default from .boto)', default=k)
	parser.add_option('-s', '--secret', help='Specify AWS secret (default from .boto)', default=s)
	parser.add_option('-R', '--recursive', action='store_true', help='Recurse into subdirectories')
	parser.add_option('-r', '--region', help='Specify region to connect to (default us-west-1)', default'us-west-1')
	parser.add_option('-d', '--delimiter', help='Specify path delimiter for S3 (default /)', default'/')

	(options, args) = parser.parse_args()

	if len(args) == 2:
		s3sync(args[0], args[1], options)
	else:
		if options.action not in ('bootloader', 'killbootloader'):
			parser.print_usage()
			sys.exit(1)



import sys
sys.path.append('../..')
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


def s3syncfiles(s3, bucket, prefix, path, flist, options):
	# XXX: broken for non-recursive
	objects = s3.ListObjects(bucket, prefix=prefix).execute()

	# Sync upload
	for f in flist:
		while os.path.exists(options.inhibit):
			time.sleep(1.0)
		if f in objects:
			oursum = subprocess.Popen(['md5sum', os.path.join(path, f)], stdout=subprocess.PIPE).communicate()[0].strip()
			print oursum, objects[f]['ETag']
			if objects[f]['ETag'] == '"%s"' % oursum:
				print 'Skipping', f
				continue
		mimetype = subprocess.Popen(['file', '-b', '--mime-type', os.path.join(path, f)], stdout=subprocess.PIPE).communicate()[0].strip()
		s3.PutObject(bucket, f, file(os.path.join(path, f), 'rb'), mimetype, Progress=Meter('%s (%s)' % (f, mimetype))).execute()

	return

	# Sync download
	for k, v in objects.items():
		if not os.path.exists(os.path.join(path, k)):
			d, _ = os.path.split(k)
			if not os.path.exists(os.path.join(path, d)):
				os.makedirs(os.path.join(path, d))
			s3.GetObject(bucket, k, file(os.path.join(path, k), 'wb'), Progress=Meter('%s' % k)).execute()


def s3sync(bucket, path, options):
	parts = bucket.split(options.delimiter, 1)
	bucket = parts[0]
	prefix = None
	if len(parts) == 2:
		prefix = parts[1]
	s3 = aaws.S3(options.region, options.key, options.secret)

	flist = []
	if options.recursive:
		for root, dirs, files in os.walk(path):
			for f in files:
				flist.append(os.path.join(os.path.relpath(root, path), f))
	else:
		for f in os.listdir(path):
			if not os.path.isdir(os.path.join(path, f)):
				flist.append(f)

#	print flist
	s3syncfiles(s3, bucket, prefix, path, flist, options)


if __name__ == '__main__':
	k, s = aaws.getBotoCredentials()
	parser = optparse.OptionParser(usage='usage: %prog [options] <bucket> <path>', version='%prog 1.00')
	parser.add_option('-k', '--key', help='Specify AWS key (default from .boto)', default=k)
	parser.add_option('-s', '--secret', help='Specify AWS secret (default from .boto)', default=s)
	parser.add_option('-R', '--recursive', action='store_true', help='Recurse into subdirectories')
	parser.add_option('-r', '--region', help='Specify region to connect to (default us-west-1)', default='us-west-1')
	parser.add_option('', '--delimiter', help='Specify path delimiter for S3 (default /)', default='/')
	parser.add_option('-i', '--inhibit', help='Pause while the specified file exists (default None)', default=None)
	parser.add_option('-u', '--upload', dest='actions', action='append_const', const='upload', help='Download from S3', default=[])
	parser.add_option('-d', '--download', dest='actions', action='append_const', const='download', help='Upload to S3')

	(options, args) = parser.parse_args()

	if len(args) == 2:
		s3sync(args[0], args[1], options)
	else:
		parser.print_usage()
		sys.exit(1)


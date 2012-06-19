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
#               s3.py,
#
#                       This module provides an interface to Amazon Simple Storage Service (S3).
#                       It is a little different to the other modules because it has to handle the different
#                       authentication mechanism of S3, and allows sending files larger than available memory,
#                       etc.
#
#

from aws import AWSService, AWSError, getBotoCredentials
import request
from xml.etree import ElementTree as ET
from urlparse import urlparse
import time
import hmac
import hashlib
import base64
import urllib
import os
import StringIO
import mimetools


class S3Request(request.AWSRequest):

    def __init__(self, host, uri, key, secret, bucket, parameters, handler=None, follower=None, verb='GET', body='', contentType='text/plain', progresscb=None):
        self._bucket = bucket
        self._body = body
        self._progress = progresscb
        self._sendfile = None
        self._recvfile = None
        self._cl = None
        self._rxtot = 0
        if hasattr(body, 'read'):
            if verb == 'GET':
                self._recvfile = body
                self._body = ''
            else:
                self._sendfile = self._body
        self._contentType = contentType
        request.AWSRequest.__init__(self, host, uri, key, secret, None, parameters, handler, follower, verb)

    def copy(self):
        return S3Request(self._host, self._uri, self._key, self._secret, self._bucket, self._parameters, self.handle, self.follow, self._verb)

    def makePath(self, verb='GET'):
        parms = []
        for key in sorted(self._parameters.keys()):
            parms.append(urllib.quote(key, safe='') + '=' + urllib.quote(self._parameters[key], safe='-_~'))
        if len(parms):
            return urllib.quote(self._uri) + '?' + '&'.join(parms)
        return urllib.quote(self._uri)

    def makeHeaders(self, verb='GET'):
        timestamp = time.strftime('%a, %d %b %Y %H:%M:%S +0000', time.gmtime())
#               tosign = verb + "\n" + Content-MD5 + "\n" + Content-Type + "\n" + timestamp + "\n" + CanonicalizedAmzHeaders + CanonicalizedResource
        if verb != 'GET':
            md5 = self.makeBodyMD5()
            tosign = verb + "\n" + md5 + "\n" + self._contentType + "\n" + timestamp + "\n"
        else:
            tosign = verb + "\n" + "\n" + "\n" + timestamp + "\n"
        if self._bucket:
            tosign += '/' + self._bucket
        tosign += urllib.quote(self._uri)
        h = hmac.new(self._secret, tosign, digestmod=hashlib.sha1)
        signature = base64.b64encode(h.digest())
        auth = 'AWS %s:%s' % (self._key, signature)
#               print repr((tosign, timestamp, auth))
#               raise SystemExit
        if verb != 'GET':
            return {'Date': timestamp, 'Authorization': auth, 'Content-Type': self._contentType, 'Content-MD5': md5}
        return {'Date': timestamp, 'Authorization': auth}

    def getContentLength(self):
        if self._sendfile:
            self._tosend = os.fstat(self._sendfile.fileno()).st_size
            return self._tosend
        else:
            return len(self._body)

    def makeBody(self):
        if self._sendfile:
            return ''               # no body for initial part of request
        return self._body

    def makeBodyMD5(self):
        if self._sendfile:
            md = hashlib.md5()
            while True:
                data = self._sendfile.read(4096)
                md.update(data)
                if len(data) < 4096:
                    break
            self._sendfile.seek(0)
            return base64.b64encode(md.digest())
        else:
            return base64.b64encode(hashlib.md5(self._body).digest())

    def handle_write(self):
        request.AWSRequest.handle_write(self)
        remain = len(self.out_buffer)
        # callback our Progress meter
        if self._progress and self._verb != 'GET':
            self._progress(self._buffered - remain, self._tosend)
        if remain < 4096 and self._sendfile:
            data = self._sendfile.read(4096)
            if len(data) < 4096:
                self._sendfile = None
            self._buffered += len(data)
            self.send(data)

    def handle_connect(self):
        request.AWSRequest.handle_connect(self)
        # buffer up some file (call 'send' on it)
        if self._sendfile:
            data = self._sendfile.read(4096)
            if len(data) < 4096:
                self._sendfile = None
            self._buffered = len(data)
            self.send(data)

    def handle_read(self):
        if self._recvfile:
            data = self.recv(4096)
            if len(data) > 0:
                if self._cl:
                    self._recvfile.write(data)
                    self._rxtot += len(data)
                    if self._progress:
                        self._progress(self._rxtot, self._cl)
                else:
                    self._rx.append(data)
                    data = ''.join(self._rx)
                    header, data = data.split('\r\n\r\n', 1)
                    fp = StringIO.StringIO(header)
                    _, status, reason = fp.readline().split(' ', 2)
                    header = mimetools.Message(fp)
                    self._cl = int(header.get('Content-Length'))
                    self._recvfile.write(data)
                    if self._progress:
                        self._progress(len(data), self._cl)
                    self._rxtot = len(data)
        else:
            request.AWSRequest.handle_read(self)




class S3(AWSService):
    endpoints = {
            'us-east-1': 's3.amazonaws.com',
            'us-west-1': 's3-us-west-1.amazonaws.com',
            'eu-west-1': 's3-eu-west-1.amazonaws.com',
            'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com',
            'ap-northeast-1': 's3-ap-northeast-1.amazonaws.com',
    }
    locationConstraint = {
            'us-east-1': None,
            'us-west-1': 'us-west-1',
            'eu-west-1': 'EU',
            'ap-southeast-1': 'ap-southeast-1',
            'ap-northeast-1': 'ap-northeast-1',
    }
    xmlns = 'http://s3.amazonaws.com/doc/2006-03-01/'

    def __init__(self, region, key, secret, version=None):
        self._region = region
        self._endpoint = self.endpoints[region]
        self._constraint = self.locationConstraint[region]
        self._key = key
        self._secret = secret


    def ListBuckets(self):
        """"""
        def findadd(m, node, attr):
            node = node.find('{%s}%s' % (self.xmlns, attr))
            if node is not None:
                m[attr] = node.text

        def response(status, reason, data):
            if status == 200:
#                               print data
                root = ET.fromstring(data)
                buckets = []
                for node in root.findall('.//{%s}Bucket' % self.xmlns):
                    b = {}
                    findadd(b, node, 'Name')
                    findadd(b, node, 'CreationDate')
                    buckets.append(b)
                return buckets
            raise AWSError(status, reason, data)

        return S3Request(self._endpoint, '/', self._key, self._secret, None, {}, response)


    def CreateBucket(self, BucketName):

        def response(status, reason, data):
            if status == 200:
#                               print data
                return True
            raise AWSError(status, reason, data)

        body = None
        if self._constraint is not None:
            body = '''\
<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<LocationConstraint>%s</LocationConstraint>
</CreateBucketConfiguration >''' % self._constraint
        return S3Request(BucketName + '.' + self._endpoint, '/', self._key, self._secret, BucketName, {}, response, body=body, verb='PUT')


    def ListObjects(self, BucketName, delimiter=None, marker=None, maxKeys=None, prefix=None, Progress=None):
        """"""
        def findadd(m, node, attr, dictattr=None):
            if dictattr is None:
                dictattr = attr
            node = node.find('{%s}%s' % (self.xmlns, attr))
            if node is not None:
                m[dictattr] = node.text

        def response(status, reason, data):
            if status == 200:
#                               print data
                root = ET.fromstring(data)
                objects = {}
                limited = root.find('.//{%s}IsTruncated' % self.xmlns).text == 'true'
                for node in root.findall('.//{%s}CommonPrefixes' % self.xmlns):
                    prefix = node.find('{%s}Prefix' % self.xmlns).text
                    objects[prefix] = None
                for node in root.findall('.//{%s}Contents' % self.xmlns):
                    obj = {}
                    key = node.find('{%s}Key' % self.xmlns).text
                    findadd(obj, node, 'LastModified')
                    findadd(obj, node, 'ETag')
                    findadd(obj, node, 'Size')
                    findadd(obj, node, 'StorageClass')
                    owner = node.find('{%s}Owner' % self.xmlns)
                    findadd(obj, owner, 'ID', 'Owner.ID')
                    findadd(obj, owner, 'DisplayName', 'Owner.DisplayName')
                    objects[key] = obj
                return objects, limited
            raise AWSError(status, reason, data)

        def follow(req):
            """This is a follower that expects a result in the form (list_of_things, NextToken).
                    If NextToken is not None then we return a copied request with the NextToken parameter set
                    to the returned NextToken, and accumulate the list_of_things.
                    """
            if req._accum is None:
                req._accum = {}
            objects, limited = req.result
            req._accum.update(objects)
            if limited:
                req.setParm('marker', max(objects.keys()))
                if Progress:
                    Progress(len(req._accum.keys()), False)
                return True
            if Progress:
                Progress(len(req._accum.keys()), True)

        return S3Request(BucketName + '.' + self._endpoint, '/', self._key, self._secret, BucketName, {
                        'delimiter': delimiter,
                        'prefix': prefix,
                        'marker': marker,
                }, response, follow)


    def PutObject(self, BucketName, Key, Data, ContentType='text/plain', Progress=None):
        def response(status, reason, data):
            if status == 200:
#                               print data
                return True
            raise AWSError(status, reason, data)

        return S3Request(BucketName + '.' + self._endpoint, '/' + Key, self._key, self._secret, BucketName, {}, response, verb='PUT', body=Data, contentType=ContentType, progresscb=Progress)


    def GetObject(self, BucketName, Key, PutData, Progress=None):
        def response(status, reason, data):
            if status == 200:
#                               print data
                return True
            raise AWSError(status, reason, data)

        return S3Request(BucketName + '.' + self._endpoint, '/' + Key, self._key, self._secret, BucketName, {}, response, verb='GET', body=PutData, progresscb=Progress)


if __name__ == '__main__':
    import proxy
    import sys

    key, secret = getBotoCredentials()
    s3 = S3('us-west-1', key, secret)
    buckets = s3.ListBuckets().execute(retries=0)
    print buckets
    if True:
        objects, info = s3.ListObjects(sys.argv[1], delimiter='/').execute(retries=0)
        for key, obj in objects.items():
            if obj:
                print key, obj['Size']
            else:
                print key

    # Upload a file
    def cb(sent, outof):
        print 'Sent %d bytes out of %d' % (sent, outof)
    print s3.PutObject(sys.argv[1], 'cloudwatch.py', file('cloudwatch.py', 'rb'), 'text/plain', Progress=cb).execute(retries=0)

#       print s3.CreateBucket('mapdata.snitchinc.com').execute(retries=0)

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

from aws import AWSService, AWSError, getBotoCredentials
import request
from xml.etree import ElementTree as ET
from urlparse import urlparse


class SQS(AWSService):
	endpoints = {
		'us-east-1': 'sqs.us-east-1.amazonaws.com',
		'us-west-1': 'sqs.us-west-1.amazonaws.com',
		'eu-west-1': 'sqs.eu-west-1.amazonaws.com',
		'ap-southeast-1': 'sqs.ap-southeast-1.amazonaws.com',
		'ap-northeast-1': 'sqs.ap-northeast-1.amazonaws.com',
	}

	def __init__(self, region, key, secret):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret

	def Response_CreateQueue(self, status, reason, data):
		"""Response Elements (in addition to standard elements).
			QueueUrl -- The queue URL for the queue you created.
				Type: String
				Ancestor: CreateQueueResult

			Special Errors:
				AWS.SimpleQueueService.QueueDeletedRecently
					You must wait 60 seconds after deleting a queue before you can create another with the same name.
					HTTP 400
				AWS.SimpleQueueService.QueueNameExists
					Queue already exists. SQS returns this error only if the request includes a DefaultVisibilityTimeout
					value that differs from the value for the existing queue.
					HTTP 400
		"""
		if status == 200:
			root = ET.fromstring(data)
			node = root.find('.//{http://queue.amazonaws.com/doc/2009-02-01/}QueueUrl')
			if node is not None:
				return node.text
		raise AWSError(status, reason, data)

	def CreateQueue(self, QueueName, DefaultVisibilityTimeout=None):
		"""The CreateQueue action creates a new queue.
			When you request CreateQueue, you provide a name for the queue. To successfully create a new queue, you must provide
			a name that is unique within the scope of your own queues. If you provide the name of an existing queue, a new queue
			isn't created and an error isn't returned. Instead, the request succeeds and the queue URL for the existing queue
			is returned (for more information about queue URLs, see Queue and Message Identifiers in the Amazon SQS Developer Guide).
			Exception: if you provide a value for DefaultVisibilityTimeout that is different from the value for the existing queue, you receive an error.
			Note: If you delete a queue, you must wait at least 60 seconds before creating a queue with the same name.

			QueueName -- The name to use for the queue created.
				Type: String
				Constraints: Maximum 80 characters; alphanumeric characters, hyphens (-), and underscores (_) are allowed
			DefaultVisibilityTimeout -- The visibility timeout (in seconds) to use for this queue.
				Type: Integer
				Constraints: 0 to 43200 (maximum 12 hours)
				Default: 30 seconds
		"""
		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'CreateQueue', {
			'QueueName': QueueName,
			'Version': '2009-02-01',
		}, self.Response_CreateQueue)
		if DefaultVisibilityTimeout is not None:
			r.parameters['DefaultVisibilityTimeout'] = DefaultVisibilityTimeout
		return r

	def Response_ListQueues(self, status, reason, data):
		"""Response Elements (in addition to standard elements).
			QueueUrl -- The queue URL for the queue you created.
				Type: String
				Ancestor: CreateQueueResult
		"""
		if status == 200:
			root = ET.fromstring(data)
			queues = []
			for node in root.findall('.//{http://queue.amazonaws.com/doc/2009-02-01/}QueueUrl'):
				queues.append(node.text)
			return queues
		raise AWSError(status, reason, data)

	def ListQueues(self, QueueNamePrefix=None):
		"""The ListQueues action returns a list of your queues. The maximum number of queues that can be returned is 1000.
			If you specify a value for the optional QueueNamePrefix parameter, only queues with a name beginning with the
			specified value are returned.

			QueueNamePrefix -- String to use for filtering the list results. Only those queues whose name begins with the specified string are returned.
				Type: String
				Constraints: Maximum 80 characters; alphanumeric characters, hyphens (-), and underscores (_) are allowed.
				Required: No
		"""
		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ListQueues', {
			'Version': '2009-02-01',
		}, self.Response_ListQueues)
		if QueueNamePrefix is not None:
			r.parameters['QueueNamePrefix'] = QueueNamePrefix
		return r

	def DeleteQueue(self, queueUrl):
		pass

	def Response_SendMessage(self, status, reason, data):
		"""Response Elements (in addition to standard elements).
			MD5OfMessageBody -- An MD5 digest of the non-URL-encoded message body string. You can use this to verify that SQS
					received the message correctly.
				Type: String
			MessageId -- An element containing the message ID of the message sent to the queue.
				Type: string

			Special Errors:
				InvalidMessageContents
					The message contains characters outside the allowed set.
					HTTP 400
				MessageTooLong
					The message size cannot exceed 64 KB.
					HTTP 400
		"""
		if status == 200:
			root = ET.fromstring(data)
			n1 = root.find('.//{http://queue.amazonaws.com/doc/2009-02-01/}MD5OfMessageBody')
			n2 = root.find('.//{http://queue.amazonaws.com/doc/2009-02-01/}MessageId')
			if n1 is not None and n2 is not None:
				return n2.text, n1.text
		raise AWSError(status, reason, data)

	def SendMessage(self, queueUrl, MessageBody):
		"""The SendMessage action delivers a message to the specified queue. The maximum allowed message size is 64 KB.

			MessageBody -- The message to send.
				Type: String maximum 64 KB in size.
				Conditions: Characters are in the following range #x9 | #xA | #xD | [#x20 to #xD7FF] | [#xE000 to #xFFFD] | [#x10000 to #x10FFFF]
				Required: Yes
		"""
		p = urlparse(queueUrl)
		r = request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'SendMessage', {
			'Version': '2009-02-01',
			'MessageBody': MessageBody,
		}, self.Response_SendMessage)
		return r

	def Response_ReceiveMessage(self, status, reason, data):
		"""Response Elements (in addition to standard elements).
			Message -- An element containing the information about the message.

				Children:
					Body -- The message's contents (not URL encoded)
					MD5OfBody -- An MD5 digest of the non-URL-encoded message body string
					MessageId -- The message's SQS-assigned ID
					ReceiptHandle -- A string associated with a specific instance of receiving the message
					Attribute -- SenderId, SentTimestamp, ApproximateReceiveCount, and/or ApproximateFirstReceiveTimestamp.
						The SentTimestamp and ApproximateFirstReceiveTimestamp are each returned as an integer representing
						the epoch time in milliseconds.

			Special Errors:
				ReadCountOutOfRange
					The value for MaxNumberOfMessages is not valid (must be from 1 to 10).
					HTTP 400
		"""
		def findadd(m, node, attr):
			node = node.find('{http://queue.amazonaws.com/doc/2009-02-01/}%s' % attr)
			if node is not None:
				m[attr] = node.text

		if status == 200:
			root = ET.fromstring(data)
			msgs = []
			for node in root.findall('.//{http://queue.amazonaws.com/doc/2009-02-01/}Message'):
				m = {}
				findadd(m, node, 'Body')
				findadd(m, node, 'MD5OfBody')
				findadd(m, node, 'MessageId')
				findadd(m, node, 'ReceiptHandle')
				# XXX: attributes
				msgs.append(m)
			return msgs
		raise AWSError(status, reason, data)

	def ReceiveMessage(self, queueUrl, AttributeNames=None, MaxNumberOfMessages=None, VisibilityTimeout=None):
		"""The ReceiveMessage action retrieves one or more messages from the specified queue.

			AttributeName.n -- The attribute(s) you want to get.
				Type: List
				Valid values: All | SenderId | SentTimestamp | ApproximateReceiveCount | ApproximateFirstReceiveTimestamp
				Default: []

			MaxNumberOfMessages -- Maximum number of messages to return. SQS never returns more messages than this value but might return fewer.
				Type: Integer from 1 to 10
				Default: 1

			VisibilityTimeout -- The duration (in seconds) that the received messages are hidden from subsequent retrieve requests after being
					retrieved by a ReceiveMessage request.
				Type: Integer
				Constraints: 0 to 43200 (maximum 12 hours)
				Default: The visibility timeout for the queue
		"""

		p = urlparse(queueUrl)
		r = request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'ReceiveMessage', {
			'Version': '2009-02-01',
		}, self.Response_ReceiveMessage)
		if MaxNumberOfMessages is not None:
			r._parameters['MaxNumberOfMessages'] = str(MaxNumberOfMessages)
		if VisibilityTimeout is not None:
			r._parameters['VisibilityTimeout'] = str(VisibilityTimeout)
		if AttributeNames is not None:
			pass	# XXX: implement
		return r

	def Response_DeleteMessage(self, status, reason, data):
		if status == 200:
			return True
		raise AWSError(status, reason, data)

	def DeleteMessage(self, queueUrl, receiptHandle):
		"""The DeleteMessage action deletes the specified message from the specified queue. You specify the message by using the message's receipt
			handle and not the message ID you received when you sent the message. Even if the message is locked by another reader due to the visibility
			timeout setting, it is still deleted from the queue. If you leave a message in the queue for more than 4 days, SQS automatically deletes it."""

		p = urlparse(queueUrl)
		r = request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'DeleteMessage', {
			'Version': '2009-02-01',
			'ReceiptHandle': receiptHandle,
		}, self.Response_DeleteMessage)
		return r

	def AddPermission(self):
		pass

	def ChangeMessageVisibility(self):
		pass

	def GetQueueAttributes(self):
		pass

	def RemovePermission(self):
		pass

	def SetQueueAttributes(self):
		pass


if __name__ == '__main__':
	key, secret = getBotoCredentials()
	sqs = SQS('us-west-1', key, secret)
	q = sqs.CreateQueue('test').GET()
	print repr(q)
	req = sqs.SendMessage(q, 'hello world')
	print req.makeURL()
	res = req.GET()
	print repr(res)
	while True:
		req = sqs.ReceiveMessage(q)
		print req.makeURL()
		res = req.GET()
		print repr(res)
		print
		if not res:
			break
		req = sqs.DeleteMessage(q, res[0]['ReceiptHandle'])
		print req.makeURL()
		res = req.GET()
		print repr(res)
		print



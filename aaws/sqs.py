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
	version = '2009-02-01'
	xmlns = 'http://queue.amazonaws.com/doc/2009-02-01/'

	def __init__(self, region, key, secret, version=None):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret


	def CreateQueue(self, QueueName, DefaultVisibilityTimeout=None):
		"""The CreateQueue action creates a new queue.
			When you request CreateQueue, you provide a name for the queue. To successfully create a new queue, you must provide
			a name that is unique within the scope of your own queues. If you provide the name of an existing queue, a new queue
			isn't created and an error isn't returned. Instead, the request succeeds and the queue URL for the existing queue
			is returned (for more information about queue URLs, see Queue and Message Identifiers in the Amazon SQS Developer Guide).
			Exception: if you provide a value for DefaultVisibilityTimeout that is different from the value for the existing queue, you receive an error.
			Note: If you delete a queue, you must wait at least 60 seconds before creating a queue with the same name.

			QueueName -- String name to use for the queue created.
				Constraints: Maximum 80 characters; alphanumeric characters, hyphens (-), and underscores (_) are allowed
			DefaultVisibilityTimeout -- Integer visibility timeout (in seconds) to use for this queue.
				Constraints: 0 to 43200 (maximum 12 hours)
				Default: 30 seconds

			Returns queueUrl (to be supplied to other SQS methods)
			"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				node = root.find('.//{http://queue.amazonaws.com/doc/2009-02-01/}QueueUrl')
				if node is not None:
					return node.text
			raise AWSError(status, reason, data)
		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'CreateQueue', {
			'QueueName': QueueName,
			'Version': self.version,
			'DefaultVisibilityTimeout': DefaultVisibilityTimeout,
		}, response)


	def ListQueues(self, QueueNamePrefix=None):
		"""The ListQueues action returns a list of your queues. The maximum number of queues that can be returned is 1000.
			If you specify a value for the optional QueueNamePrefix parameter, only queues with a name beginning with the
			specified value are returned.

			QueueNamePrefix -- String to use for filtering the list results. Only those queues whose name begins with the specified string are returned.
				Constraints: Maximum 80 characters; alphanumeric characters, hyphens (-), and underscores (_) are allowed.
			"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				queues = []
				for node in root.findall('.//{http://queue.amazonaws.com/doc/2009-02-01/}QueueUrl'):
					queues.append(node.text)
				return queues
			raise AWSError(status, reason, data)
		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ListQueues', {
			'Version': self.version,
			'QueueNamePrefix': QueueNamePrefix,
		}, response)


	def DeleteQueue(self, queueUrl):
		"""The DeleteQueue action deletes the queue specified by the queue URL, regardless of whether the queue is empty.
			If the specified queue does not exist, SQS returns a successful response.
			Caution: Use DeleteQueue with care; once you delete your queue, any messages in the queue are no longer available.

			When you delete a queue, the deletion process takes up to 60 seconds. Requests you send involving that queue
			during the 60 seconds might succeed. For example, a sendMessage request might succeed, but after the 60 seconds,
			the queue and that message you sent no longer exist. Also, when you delete a queue, you must wait at least 60
			seconds before creating a queue with the same name.

			Amazon reserves the right to delete queues that have had no activity for more than 30 days. For more information,
			see About SQS Queues in the Amazon SQS Developer Guide.

			queueUrl - As obtained by CreateQueue
			Returns True on success
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		p = urlparse(queueUrl)
		return request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'DeleteQueue', {
			'Version': self.version,
		}, response)


	def SendMessage(self, queueUrl, MessageBody):
		"""The sendMessage action delivers a message to the specified queue. The maximum allowed message size is 64 KB.

			MessageBody -- The string message to send. maximum 64 KB in size.
				Conditions: Characters are in the following range #x9 | #xA | #xD | [#x20 to #xD7FF] | [#xE000 to #xFFFD] | [#x10000 to #x10FFFF]

			Returns MessageId, MD5OfMessageBody (both strings)
		"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				n1 = root.find('.//{http://queue.amazonaws.com/doc/2009-02-01/}MD5OfMessageBody')
				n2 = root.find('.//{http://queue.amazonaws.com/doc/2009-02-01/}MessageId')
				if n1 is not None and n2 is not None:
					return n2.text, n1.text
			raise AWSError(status, reason, data)

		p = urlparse(queueUrl)
		return request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'SendMessage', {
			'Version': self.version,
			'MessageBody': MessageBody,
		}, response)


	def ReceiveMessage(self, queueUrl, AttributeNames=None, MaxNumberOfMessages=None, VisibilityTimeout=None):
		"""The receiveMessage action retrieves one or more messages from the specified queue.

			AttributeNames -- A list of strings giving the attribute names you wish to receive. If not supplied then no attributes are returned.
				Valid values: All | SenderId | SentTimestamp | ApproximateReceiveCount | ApproximateFirstReceiveTimestamp

			MaxNumberOfMessages -- Maximum number of messages to return (1 to 10). SQS never returns more messages than this value but might return fewer.
				Default: 1

			VisibilityTimeout -- The duration (in seconds) that the received messages are hidden from subsequent retrieve requests after being
					retrieved by a ReceiveMessage request.
				Constraints: 0 to 43200 (maximum 12 hours)
				Default: The visibility timeout for the queue

			Returns a list of dicts. Each dict contains the following keys (and any AttributeNames requested):
				Body - The message body
				MD5OfBody - MD5 checksum
				MessageId - Unique Id for this message
				ReceiptHandle - required for calls to ChangeMessageVisibility + DeleteMessage
			"""

		def response(status, reason, data):
			def findadd(m, node, attr):
				node = node.find('{%s}%s' % (self.xmlns, attr))
				if node is not None:
					m[attr] = node.text
#			print data
			if status == 200:
				root = ET.fromstring(data)
				msgs = []
				for node in root.findall('.//{%s}Message' % self.xmlns):
					m = {}
					findadd(m, node, 'Body')
					findadd(m, node, 'MD5OfBody')
					findadd(m, node, 'MessageId')
					findadd(m, node, 'ReceiptHandle')
					for attrnode in node.findall('{%s}Attribute' % self.xmlns):
						name = attrnode.find('{%s}Name' % self.xmlns).text
						value = attrnode.find('{%s}Value' % self.xmlns).text
						m[name] = value
					msgs.append(m)
				return msgs
			raise AWSError(status, reason, data)

		p = urlparse(queueUrl)
		r = request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'ReceiveMessage', {
			'Version': self.version,
			'MaxNumberOfMessages': MaxNumberOfMessages,
			'VisibilityTimeout': VisibilityTimeout,
		}, response)
		if AttributeNames is not None:
			for idx, attr in enumerate(AttributeNames):
				r.addParm('AttributeName.%d' % (idx + 1), attr)
		return r


	def DeleteMessage(self, queueUrl, receiptHandle):
		"""The DeleteMessage action deletes the specified message from the specified queue. You specify the message by using the message's receipt
			handle and not the message ID you received when you sent the message. Even if the message is locked by another reader due to the visibility
			timeout setting, it is still deleted from the queue. If you leave a message in the queue for more than 4 days, SQS automatically deletes it."""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		p = urlparse(queueUrl)
		return request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'DeleteMessage', {
			'Version': '2009-02-01',
			'ReceiptHandle': receiptHandle,
		}, response)


	def AddPermission(self, queueUrl, Label, Permissions):
		"""The AddPermission action adds a permission to a queue for a specific principal. This allows for sharing access to the queue.
			When you create a queue, you have full control access rights for the queue. Only you (as owner of the queue) can grant or deny
			permissions to the queue. For more information about these permissions, see Shared Queues in the Amazon SQS Developer Guide.

			NOTE: addPermission writes an SQS-generated policy. If you want to write your own policy, use setQueueAttributes to upload your policy.
			For more information about writing your own policy, see Appendix: The Access Policy Language in the Amazon SQS Developer Guide.

			Label -- The unique identification of the permission you're setting (a string).
				Constraints: Maximum 80 characters; alphanumeric characters, hyphens (-), and underscores (_) are allowed.
			Permissions -- Either a dict or an iterable of tuples of the form (AWSAccountId, ActionName)
				AWSAccountId - The AWS account number of the principal who will be given permission (a string). The principal must have an AWS
					account, but does not need to be signed up for Amazon SQS. For information about locating the AWS account identification,
					see Your AWS Identifiers in the Amazon SQS Developer Guide.
					Constraints: Valid 12-digit AWS account number, without hyphens
				ActionName - The action you want to allow for the specified principal (a string). For more information about these actions,
					see Understanding Permissions in the Amazon SQS Developer Guide.
					Valid values: * | SendMessage | ReceiveMessage | DeleteMessage | ChangeMessageVisibility | GetQueueAttributes

			Returns True on success
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		p = urlparse(queueUrl)
		r = request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'AddPermission', {
			'Version': self.version,
		}, response)
		if hasattr(Permissions, 'items'):
			Permissions = Permissions.items()
		for idx, (account, action) in enumerate(Permissions):
			r.addParm('AWSAccountId.%d' % (idx + 1), account)
			r.addParm('ActionName.%d' % (idx + 1), action)
		return r


	def ChangeMessageVisibility(self, queueUrl, ReceiptHandle, VisibilityTimeout):
		"""The changeMessageVisibility action changes the visibility timeout of a specified message in a queue to a new value. The maximum
			allowed timeout value you can set the value to is 12 hours. This means you can't extend the timeout of a message in an existing
			queue to more than a total visibility timeout of 12 hours. (For more information visibility timeout, see Visibility Timeout in
			the Amazon SQS Developer Guide.)

			For example, let's say the timeout for the queue is 30 seconds, and you receive a message. Once you're 20 seconds into the
			timeout for that message (i.e., you have 10 seconds left), you extend it by 60 seconds by calling changeMessageVisibility with
			VisibilityTimeoutset to 60 seconds. You have then changed the remaining visibility timeout from 10 seconds to 60 seconds.

			Important: If you attempt to set the VisibilityTimeout to an amount more than the maximum time left, Amazon SQS returns an error.
			It will not automatically recalculate and increase the timeout to the maximum time remaining.

			Important: Unlike with a queue, when you change the visibility timeout for a specific message, that timeout value is applied
			immediately but is not saved in memory for that message. If you don't delete a message after it is received, the visibility timeout
			for the message the next time it is received reverts to the original timeout value, not the value you set with the
			changeMessageVisibility action.

			ReceiptHandle -- The receipt handle associated with the message whose visibility timeout you want to change. This parameter is
				returned by the ReceiveMessage action.
			VisibilityTimeout -- The new value for the message's visibility timeout (in seconds).
				Constraints: Integer from 0 to 43200 (maximum 12 hours)
			Returns True on success
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		p = urlparse(queueUrl)
		return request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'ChangeMessageVisibility', {
			'Version': self.version,
			'ReceiptHandle': ReceiptHandle,
			'VisibilityTimeout': VisibilityTimeout,
		}, response)


	def GetQueueAttributes(self, queueUrl, Attributes=None):
		"""The GetQueueAttributes action returns one or all attributes of a queue.

			Attributes -- If not supplied, than all attributes are returned.
				If supplied it should be an iterable of strings; each string can have one of the following values:
				All - returns all values.
				ApproximateNumberOfMessages - returns the approximate number of visible messages in a queue. For more information, see Resources Required
					to Process Messages in the Amazon SQS Developer Guide.
				ApproximateNumberOfMessagesNotVisible - returns the approximate number of messages that are not timed-out and not deleted. For more
					information, see Resources Required to Process Messages in the Amazon SQS Developer Guide.
				VisibilityTimeout - returns the visibility timeout for the queue. For more information about visibility timeout, see Visibility Timeout
					in the Amazon SQS Developer Guide.
				CreatedTimestamp - returns the time when the queue was created (epoch time in seconds).
				LastModifiedTimestamp - returns the time when the queue was last changed (epoch time in seconds).
				Policy - returns the queue's policy.
				MaximumMessageSize - returns the limit of how many bytes a message can contain before Amazon SQS rejects it.
				MessageRetentionPeriod - returns the number of seconds Amazon SQS retains a message.
				QueueArn - returns the queue's Amazon resource name (ARN).

			Returns a dict of attributes.
			"""

		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				attributes = {}
				for node in root.findall('.//{%s}Attribute' % self.xmlns):
					name = node.find('{%s}Name' % self.xmlns).text
					value = node.find('{%s}Value' % self.xmlns).text
					attributes[name] = value
				return attributes
			raise AWSError(status, reason, data)
		p = urlparse(queueUrl)
		r = request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'GetQueueAttributes', {
			'Version': self.version,
		}, response)
		if Attributes is None:
			r.addParm('AttributeName.1', 'All')
		else:
			for idx, attr in enumerate(Attributes):
				r.addParm('AttributeName.%d' % (idx + 1), attr)
		return r


	def RemovePermission(self, queueUrl, Label):
		"""The removePermission action revokes any permissions in the queue policy that matches the Label parameter. Only the
			owner of the queue can remove permissions.

			Label -- The identfication of the permission you want to remove. This is the label you added in AddPermission.
			Returns True on success
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		p = urlparse(queueUrl)
		return request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'RemovePermission', {
			'Version': self.version,
			'Label': Label,
		}, response)


	def SetQueueAttributes(self, queueUrl, AttributeName, AttributeValue):
		"""The setQueueAttributes action sets one attribute of a queue per request. When you change a queue's attributes,
			the change can take up to 60 seconds to propagate throughout the SQS system.

			AttributeName -- The name of the attribute you want to set. One of:
				VisibilityTimeout - The length of time (in seconds) that a message received from a queue will be invisible to
					other receiving components when they ask to receive messages. For more information about VisibilityTimeout,
					see Visibility Timeout in the Amazon SQS Developer Guide.
				Policy - The formal description of the permissions for a resource. For more information about Policy, see Basic
					Policy Structure in the Amazon SQS Developer Guide.
				MaximumMessageSize - The limit of how many bytes a message can contain before Amazon SQS rejects it.
				MessageRetentionPeriod - The number of seconds Amazon SQS retains a message.
			AttributeValue -- The value of the attribute you want to set. To delete a queue's access control policy,
				set the policy to "". Values supplied depend on AttributeName, as follows:
				VisibilityTimeout - An integer from 0 to 43200 (12 hours).
				Policy - A valid json-encoded policy. For more information about policy structure, see
					Basic Policy Structure in the Amazon SQS Developer Guide.
				MaximumMessageSize - An integer from 1024 bytes (1KB) up to 65536 bytes (64KB). The default
					for this attribute is 8192 (8KB).
				MessageRetentionPeriod - Integer representing seconds, from 3600 (1 hour) to 1209600 (14 days).
					The default for this attribute is 345600 (4 days).
			Returns True on success
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)
		p = urlparse(queueUrl)
		return request.AWSRequest(self._endpoint, p.path, self._key, self._secret, 'SetQueueAttributes', {
			'Version': self.version,
			'Attribute.Name': AttributeName,
			'Attribute.Value': AttributeValue,
		}, response)


if __name__ == '__main__':
	key, secret = getBotoCredentials()
	sqs = SQS('us-west-1', key, secret)
	q = sqs.CreateQueue('test').GET()
	print repr(q)
	print sqs.GetQueueAttributes(q).GET()
	print sqs.GetQueueAttributes(q, ['QueueArn', 'Policy']).GET()
	print sqs.SendMessage(q, 'hello world').GET()
	while True:
		msgs = sqs.ReceiveMessage(q, ['All']).GET()
		print repr(msgs)
		if len(msgs) == 0:
			break
		print sqs.DeleteMessage(q, msgs[0]['ReceiptHandle']).GET()
		print



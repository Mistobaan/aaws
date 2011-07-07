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


class SNS(AWSService):
	endpoints = {
		'us-east-1': 'sns.us-east-1.amazonaws.com',
		'us-west-1': 'sns.us-west-1.amazonaws.com',
		'eu-west-1': 'sns.eu-west-1.amazonaws.com',
		'ap-southeast-1': 'sns.ap-southeast-1.amazonaws.com',
		'ap-northeast-1': 'sns.ap-northeast-1.amazonaws.com',
	}
	xmlns = 'http://sns.amazonaws.com/doc/2010-03-31/'

	def __init__(self, region, key, secret, version=None):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret


	def AddPermission(self, TopicArn, Permissions, Label):
		"""The AddPermission action adds a statement to a topic's access control policy,
			granting access for the specified AWS accounts to the specified actions.

			TopicArn -- The ARN of the topic whose access control policy you wish to modify.
			Permissions -- Either a dict or an iterable of tuples of the form (AWSAccountId, ActionName)
				AWSAccountId - An AWS account ID of the user (principal) who will be
					given access to the specified actions. The user must have an AWS account, but
					does not need to be signed up for this service.
				ActionName - the name of a method (e.g. publish) this accountid may call.
			Label -- A unique identifier for the new policy statement.

			Returns -- True if HTTP request succeeds
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)

		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'AddPermission', {
				'TopicArn': TopicArn,
				'Label': Label,
			}, response)
		if hasattr(Permissions, 'items'):
			Permissions = Permissions.items()
		for idx, (accountid, action) in enumerate(Permissions):
			r.addParm('AWSAccountId.member.%d' % (idx + 1), accountid)
			r.addParm('ActionName.member.%d' % (idx + 1), action)
		return r


	def ConfirmSubscription(self, TopicArn, Token, AuthenticateOnUnsubscribe=None):
		"""The ConfirmSubscription action verifies an endpoint owner's intent to receive messages
			by validating the token sent to the endpoint by an earlier subscribe action. If the
			token is valid, the action creates a new subscription and returns its Amazon Resource
			Name (ARN). This call requires an AWS signature only when the AuthenticateOnUnsubscribe
			flag is set to "true".

			TopicArn -- The ARN of the topic whose access control policy you wish to modify.
				Type: String
				Required: Yes

			Token -- Short-lived token sent to an endpoint during the subscribe action.
				Type: String
				Required: Yes

			AuthenticateOnUnsubscribe -- Indicates that you want to disable unauthenticated
					unsubsciption of the subscription. If parameter is present in the request, the
					request has an AWS signature, and the value of this parameter is true, only the
					topic owner and the subscription owner will be permitted to unsubscribe the
					endpoint, and the unsubscribe action will require AWS authentication.
				Type: String
				Required: No

			Returns -- SubscriptionArn
				The ARN of the created subscription.
				Type: String
			"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				node = root.find('.//{http://sns.amazonaws.com/doc/2010-03-31/}SubscriptionArn')
				if node is not None:
					return node.text
			raise AWSError(status, reason, data)

		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ConfirmSubscription', {
				'TopicArn': TopicArn,
				'Token': Token,
			}, response)
		if AuthenticateOnUnsubscribe is not None:
			r.addParm('AuthenticateOnUnsubscribe', 'Yes')
		return r


	def CreateTopic(self, Name):
		"""The CreateTopic action creates a topic to which notifications can be published.
			Users can create at most 25 topics. This action is idempotent, so if the requester
			already owns a topic with the specified name, that topic's ARN will be returned
			without creating a new topic.

			Name -- The name of the topic you want to create.
				Constraints: Topic names must be made up of only uppercase and lowercase ASCII
				letters, numbers, and hyphens, and must be between 1 and 256 characters long. 
				Type: String
				Required: Yes

			Returns -- TopicArn
				The Amazon Resource Name (ARN) assigned to the created topic.
				Type: String
		"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				node = root.find('.//{%s}TopicArn' % self.xmlns)
				if node is not None:
					return node.text
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'CreateTopic', {
				'Name': Name,
			}, response)


	def DeleteTopic(self, TopicArn):
		"""The DeleteTopic action deletes a topic and all its subscriptions. Deleting a topic
			might prevent some messages previously sent to the topic from being delivered to
			subscribers. This action is idempotent, so deleting a topic that does not exist will
			not result in an error.

			TopicArn -- The ARN of the topic you want to delete.
				Type: String
				Required: Yes

			Returns -- True if HTTP request succeeds (response is irrelevant)
		"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'DeleteTopic', {
				'TopicArn': TopicArn,
			}, response)


	def GetTopicAttributes(self, TopicArn):
		"""The GetTopicAttributes action returns all of the properties of a topic customers
			have created. Topic properties returned might differ based on the authorization of
			the user.

			TopicArn -- The ARN of the topic you want to delete.
				Type: String
				Required: Yes

			Returns -- A dict of the topic's attributes. Attributes in this map include:
				TopicArn -- the topic's ARN
				Owner -- the AWS account ID of the topic's owner
				Policy -- the JSON serialization of the topic's access control policy
				DisplayName -- the human-readable name used in the "From" field for notifications
					to email and email-json endpoints
				Type: dict(String -> String)
			"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				attrib = {}
				for node in root.findall('.//{%s}entry' % self.xmlns):
					name = node.find('{%s}key' % self.xmlns)
					val = node.find('{%s}value' % self.xmlns)
					attrib[name.text] = val.text
				return attrib
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'GetTopicAttributes', {
				'TopicArn': TopicArn,
			}, response)


	def _Response_ListSubscriptions(self, status, reason, data):
		"""Response handler for listSubscriptions, and listSubscriptionsByTopic"""
		def findadd(m, node, attr):
			node = node.find('{%s}%s' % (self.xmlns, attr))
			if node is not None:
				m[attr] = node.text

		# Handle response to ListSubscriptions, and ListSubscriptionsByTopic
		if status == 200:
			root = ET.fromstring(data)
			token = None
			node = root.find('.//{%s}NextToken' % self.xmlns)
			if node is not None:
				token = node.text
			subs = []
			for node in root.findall('.//{%s}member' % self.xmlns):
				sub = {}
				findadd(sub, node, 'TopicArn')
				findadd(sub, node, 'Protocol')
				findadd(sub, node, 'SubscriptionArn')
				findadd(sub, node, 'Owner')
				findadd(sub, node, 'Endpoint')
				subs.append(sub)
			return subs, token
		raise AWSError(status, reason, data)


	def ListSubscriptions(self, NextToken=None):
		"""The ListSubscriptions action returns a list of the requester's subscriptions. Each
			call returns a limited list of subscriptions, up to 100. If there are more
			subscriptions, a NextToken is also returned. Use the NextToken parameter in a new
			ListSubscriptions call to get further results.

			NextToken -- Token returned by the previous listTopics request.
				Type: String
				Required: No

			Returns -- Subscriptions, NextToken
				NextToken -- Token to pass along to the next ListSubscriptions request. This
					element is returned if there are additional topics to retrieve. (Otherwise None)
				Subscriptions -- A list of dicts. Each dict contains the following keys:
					TopicArn, Protocol, SubscriptionArn, Owner, Endpoint
			"""

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ListSubscriptions', {
				'NextToken': NextToken,
			}, self._Response_ListSubscriptions, request.ListFollow)


	def ListSubscriptionsByTopic(self, TopicArn, NextToken=None):
		"""The ListSubscriptionsByTopic action returns a list of the subscriptions to a specific
			topic. Each call returns a limited list of subscriptions, up to 100. If there are more
			subscriptions, a NextToken is also returned. Use the NextToken parameter in a new
			ListSubscriptionsByTopic call to get further results.

			TopicArn -- The ARN of the topic you want to delete.
				Type: String
				Required: Yes

			NextToken -- Token returned by the previous listTopics request.
				Type: String
				Required: No

			Returns -- Subscriptions, NextToken
				NextToken -- Token to pass along to the next ListSubscriptions request. This
					element is returned if there are additional topics to retrieve. (Otherwise None)
				Subscriptions -- A list of dicts. Each dict contains the following keys:
					TopicArn, Protocol, SubscriptionArn, Owner, Endpoint
			"""

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ListSubscriptionsByTopic', {
				'TopicArn': TopicArn,
				'NextToken': NextToken,
			}, self._Response_ListSubscriptions, request.ListFollow)


	def ListTopics(self, NextToken=None):
		"""The ListTopics action returns a list of the requester's topics. Each call returns a
			limited list of topics, up to 100. If there are more topics, a NextToken is also
			returned. Use the NextToken parameter in a new ListTopics call to get further results.

			NextToken -- Token returned by the previous ListTopics request.
				Type: String
				Required: No

			Returns -- Topics, NextToken
				NextToken -- Token to pass along to the next ListTopics request. This element is
					returned if there are additional topics to retrieve. (Otherwise None)
				Topics -- A list of topic ARNs.
			"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				token = None
				node = root.find('.//{%s}NextToken' % self.xmlns)
				if node is not None:
					token = node.text
				topics = []
				for node in root.findall('.//{%s}TopicArn' % self.xmlns):
					topics.append(node.text)
				return topics, token
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'ListTopics', {
				'NextToken': NextToken,
			}, response, request.ListFollow)


	def Publish(self, TopicArn, Message, Subject=None, MessageStructure=None):
		"""The Publish action sends a message to all of a topic's subscribed endpoints. When a
			messageId is returned, the message has been saved and Amazon SNS will attempt to deliver
			it to the topic's subscribers shortly. The format of the outgoing message to each subscribed
			endpoint depends on the notification protocol selected.

			TopicArn -- The topic you want to publish to.
				Type: String
				Required: Yes

			Message -- The message you want to send to the topic.
				Constraints: Messages must be UTF-8 encoded strings at most 8 KB in size
					(8192 bytes, not 8192 characters).
				Type: String
				Required: Yes

			Subject -- Optional parameter to be used as the "Subject" line of when the message is delivered
					to e-mail endpoints. This field will also be included, if present, in the standard JSON
					messages delivered to other endpoints.
				Constraints: Subjects must be ASCII text that begins with a letter, number or punctuation mark;
					must not include line breaks or control characters; and must be less than 100 characters long.
				Type: String
				Required: No

			MessageStructure -- Optional parameter. It will have one valid value: "json". If this option,
					Message is present and set to "json", the value of Message must: be a syntactically valid JSON
					object. It must contain at least a top level JSON key of "default" with a value that is a
					string. For any other top level key that matches one of our transport protocols (e.g. "http"),
					then the corresponding value (if it is a string) will be used for the message published for
					that protocol
				Constraints: Keys in the JSON object that correspond to supported transport protocols must have
					simple JSON string values. The values will be parsed (unescaped) before they are used in
					outgoing messages. Typically, outbound notifications are JSON encoded (meaning, the characters
					will be reescaped for sending). JSON strings are UTF-8. Values have a minimum length of 0 (the
					empty string, "", is allowed). Values have a maximum length bounded by the overall message
					size (so, including multiple protocols may limit message sizes). Non-string values will cause
					the key to be ignored. Keys that do not correspond to supported transport protocols will be
					ignored. Duplicate keys are not allowed. Failure to parse or validate any key or value in the
					message will cause the Publish call to return an error (no partial delivery).
				Type: String
				Required: No

			Returns -- MessageId
				Unique identifier assigned to the published message.
				Type: String
			"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				node = root.find('.//{%s}MessageId' % self.xmlns)
				if node is not None:
					return node.text
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'Publish', {
				'TopicArn': TopicArn,
				'Message': Message,
				'MessageStructure': MessageStructure,
				'Subject': Subject,
			}, response)


	def RemovePermission(self, TopicArn, Label):
		"""The RemovePermission action removes a statement from a topic's access control policy.

			TopicArn -- The ARN of the topic whose access control policy you wish to modify.
				Type: String
				Required: Yes

			Label -- A unique identifier for the new policy statement.
				Type: String
				Required: Yes

			Returns -- True if HTTP request succeeds
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'RemovePermission', {
				'TopicArn': TopicArn,
				'Label': Label,
			}, response)


	def SetTopicAttributes(self, TopicArn, AttributeName, AttributeValue):
		"""The SetTopicAttributes action allows a topic owner to set an attribute of the topic to a
			new value.

			TopicArn -- The ARN of the topic to modify.
				Type: String
				Required: Yes

			AttributeName -- The name of the attribute you want to set. Only a subset of the topic's
					attributes are mutable.
				Valid values: Policy | DisplayName
				Type: String
				Required: Yes

			AttributeValue -- The new value for the attribute.
				Type: String
				Required: Yes

			Returns -- True if HTTP request succeeds
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'SetTopicAttributes', {
				'TopicArn': TopicArn,
				'AttributeName': AttributeName,
				'AttributeValue': AttributeValue,
			}, response)


	def Subscribe(self, TopicArn, Protocol, Endpoint):
		"""The Subscribe action prepares to subscribe an endpoint by sending the endpoint a confirmation
			message. To actually create a subscription, the endpoint owner must call the ConfirmSubscription
			action with the token from the confirmation message. Confirmation tokens are valid for three days.

			TopicArn -- The ARN of topic you want to subscribe to.
				Type: String
				Required: Yes

			Endpoint -- The endpoint that you want to receive notifications. Endpoints vary by protocol:
					For the http protocol, the endpoint is an URL beginning with "http://"
					For the https protocol, the endpoint is a URL beginning with "https://"
					For the email protocol, the endpoint is an e-mail address
					For the email-json protocol, the endpoint is an e-mail address
					For the sqs protocol, the endpoint is the ARN of an Amazon SQS queue
				Type: String
				Required: Yes

			Protocol -- The protocol you want to use. Supported protocols include:
					http -- delivery of JSON-encoded message via HTTP POST
					https -- delivery of JSON-encoded message via HTTPS POST
					email -- delivery of message via SMTP
					email-json -- delivery of JSON-encoded message via SMTP
					sqs -- delivery of JSON-encoded message to an Amazon SQS queue
				Type: String
				Required: Yes

			Returns -- SubscriptionArn
				The ARN of the subscription, if the service was able to create a subscription immediately
				(without requiring endpoint owner confirmation).
				Type: String
			"""

		def response(status, reason, data):
			if status == 200:
				root = ET.fromstring(data)
				node = root.find('.//{%s}SubscriptionArn' % self.xmlns)
				if node is not None:
					return node.text
				return None
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'Subscribe', {
				'TopicArn': TopicArn,
				'Endpoint': Endpoint,
				'Protocol': Protocol,
			}, response)


	def Unsubscribe(self, SubscriptionArn):
		"""The Unsubscribe action deletes a subscription. If the subscription requires authentication
			for deletion, only the owner of the subscription or the its topic's owner can unsubscribe,
			and an AWS signature is required. If the Unsubscribe call does not require authentication
			and the requester is not the subscription owner, a final cancellation message is delivered
			to the endpoint, so that the endpoint owner can easily resubscribe to the topic if the
			Unsubscribe request was unintended.

			SubscriptionArn -- The ARN of the subscription to be deleted.
				Type: String
				Required: Yes

			Returns -- True if HTTP request succeeds
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'Unsubscribe', {
				'SubscriptionArn': SubscriptionArn,
			}, response)


if __name__ == '__main__':
	import proxy

	key, secret = getBotoCredentials()
	sns = proxy.ServiceProxy(SNS('us-west-1', key, secret))
	topics = sns.ListTopics()
	print topics
	subs = sns.ListSubscriptionsByTopic(topics[0])
	print subs
	attrib = sns.GetTopicAttributes(topics[0])
	print repr(attrib)
	subs = sns.ListSubscriptions()
	print subs

	raise SystemExit

	import sys
	if len(sys.argv) >= 2:
		email = sys.argv[1]
		topicArn = sns.CreateTopic('test').GET()
		print 'Created', topicArn
		attrib = sns.GetTopicAttributes(topicArn).GET()
		print 'Attribs', attrib
		if email == 'publish':
			messageId = sns.Publish(topicArn, 'Hello world').GET()
			print 'Published', messageId
		else:
			subArn = sns.Subscribe(topicArn, 'email', email).GET()
			print 'Subscribed', subArn
	else:
		topicArn = sns.CreateTopic('test').GET()
		subs = sns.ListSubscriptionsByTopic(topicArn).GET()
		print 'Subs', subs
		sns.Unsubscribe(subs[0]['SubscriptionArn']).GET()
		subs = sns.ListSubscriptionsByTopic(topics[0]).GET()
		print 'Subs (after Unsubscribe)', subs
		sns.DeleteTopic(topicArn).GET()
		print 'Deleted', topicArn


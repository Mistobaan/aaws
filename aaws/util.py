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
import uuid
import json


def SubscribeQueue(sqs, sns, queueName, topicName):
	queue = sqs.CreateQueue(queueName).GET()
	topic = sns.CreateTopic(topicName).GET()
	attr = sqs.GetQueueAttributes(queue, ['QueueArn', 'Policy']).GET()
	print 'attr', attr
	if attr.get('policy'):
		json.loads(attr['policy'])
		# XXX: check
		pass
	else:
		policy = {
			'Version': '2008-10-17',
			'Id': str(uuid.uuid4()),
			'Statement': [
				{
					'Action': 'sqs:*',
					'Effect': 'Allow',
					'Principal': {'AWS' : '*'},
					'Resource': attr['QueueArn'],
					'Sid': 'allow%s' % topicName,
					'Condition': {'StringLike': {'aws:SourceArn': topic}},
				}
			],
		}
		print repr(policy)
		sqs.SetQueueAttributes(queue, 'Policy', json.dumps(policy)).GET()
	print topic, 'sqs', attr['QueueArn']
	sns.Subscribe(topic, 'sqs', attr['QueueArn']).GET()
	return queue, topic


if __name__ == '__main__':
	import sys
	from sqs import SQS
	from sns import SNS

	key, secret = getBotoCredentials()
	sqs = SQS('us-west-1', key, secret)
	sns = SNS('us-west-1', key, secret)

	if 'cleanq' in sys.argv:
		q = sqs.CreateQueue('testSubscribeQ').GET()
		print sqs.DeleteQueue(q).GET()
	if 'cleantopic' in sys.argv:
		topic = sns.CreateTopic('testSubscribeT').GET()
		print sns.DeleteTopic(topic).GET()
	if 'subscribeq' in sys.argv:
		SubscribeQueue(sqs, sns, 'testSubscribeQ', 'testSubscribeT')


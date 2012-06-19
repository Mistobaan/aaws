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


class CloudWatch(AWSService):
	endpoints = {
		'us-east-1': 'monitoring.us-east-1.amazonaws.com',
		'us-west-1': 'monitoring.us-west-1.amazonaws.com',
		'eu-west-1': 'monitoring.eu-west-1.amazonaws.com',
		'ap-southeast-1': 'monitoring.ap-southeast-1.amazonaws.com',
		'ap-northeast-1': 'monitoring.ap-northeast-1.amazonaws.com',
	}
	version = '2010-08-01'
	xmlns = 'http://monitoring.amazonaws.com/doc/2010-08-01/'

	def __init__(self, region, key, secret, version=None):
		self._region = region
		self._endpoint = self.endpoints[region]
		self._key = key
		self._secret = secret


	def DeleteAlarms(self, AlarmNames):
		"""Deletes all specified alarms. In the event of an error, no alarms are deleted.

			AlarmNames.member.N -- A list of alarms to be deleted.
				Type: List of Strings
				Length constraints: Minimum of 0 item(s) in the list. Maximum of 100 item(s) in the list.
				Required: Yes

			Returns -- True on success
			"""

		def response(status, reason, data):
			if status == 200:
				return True
			raise AWSError(status, reason, data)

		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'DeleteAlarms', {
				'Version': self.version,
			}, response)
		for idx, name in enumerate(AlarmNames):
			r.addParm('AlarmNames.member.%d' % (idx + 1), name)
		return r


	def DescribeAlarmHistory(self, AlarmName=None, StartDate=None, EndDate=None, HistoryItemType=None, MaxRecords=None, NextToken=None):
		"""Retrieves history for the specified alarm. Filter alarms by date range or item type.
			If an alarm name is not specified, Amazon CloudWatch returns histories for all of
			the owner's alarms.

			NOTE: Amazon CloudWatch retains the history of an alarm for two weeks, whether or
			not you delete the alarm.

			AlarmName -- The name of the alarm.
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.
				Required: No

			StartDate -- The starting date to retrieve alarm history.
				Type: DateTime
				Required: No

			EndDate -- The ending date to retrieve alarm history.
				Type: DateTime
				Required: No

			HistoryItemType -- The type of alarm histories to retrieve.
				Type: String
				Valid Values: ConfigurationUpdate | StateUpdate | Action
				Required: No

			MaxRecords -- The maximum number of alarm history records to retrieve.
				Type: Integer
				Required: No

			NextToken -- The token returned by a previous call to indicate that there is more data available.
				Type: String
				Required: No

			Returns -- AlarmHistoryItems, NextToken
				AlarmHistoryItems -- A list of alarm histories.
				NextToken -- A string that marks the start of the next batch of returned results.
			"""

		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				attrib = {}
				for node in root.findall('.//{http://sns.amazonaws.com/doc/2010-03-31/}AlarmHistoryItem'):
					name = node.find('{http://sns.amazonaws.com/doc/2010-03-31/}key')
					val = node.find('{http://sns.amazonaws.com/doc/2010-03-31/}value')
					attrib[name.text] = val.text
				token = None
				node = root.find('.//{%s}NextToken' % self.xmlns)
				if node is not None:
					token = node.text
				return attrib, token
			raise AWSError(status, reason, data)

		return request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'DescribeAlarmHistory', {
				'AlarmName': AlarmName,
				'HistoryItemType': HistoryItemType,
				'MaxRecords': MaxRecords,
				'NextToken': NextToken,
				'StartDate': StartDate,
				'EndDate': EndDate,
				'Version': self.version,
			}, response, request.ListFollow)


	def DescribeAlarms(self, ActionPrefix=None, AlarmNamePrefix=None, AlarmNames=None, MaxRecords=None, StateValue=None, NextToken=None):
		"""Retrieves alarms with the specified names. If no name is specified, all alarms for the user
			are returned. Alarms can be retrieved by using only a prefix for the alarm name, the alarm
			state, or a prefix for any action.

			ActionPrefix -- The action name prefix.
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 1024.
				Required: No

			AlarmNamePrefix -- The alarm name prefix. AlarmNames cannot be specified if this parameter is
					specified.
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.
				Required: No

			AlarmNames -- A list of alarm names to retrieve information for.
				Type: List of Strings
				Length constraints: Minimum of 0 item(s) in the list. Maximum of 100 item(s) in the list.
				Required: No

			MaxRecords -- The maximum number of alarm descriptions to retrieve.
				Type: Integer
				Required: No

			StateValue -- The state value to be used in matching alarms.
				Type: String
				Valid Values: OK | ALARM | INSUFFICIENT_DATA
				Required: No

			NextToken -- The token returned by a previous call to indicate that there is more data available.
				Type: String
				Required: No

			Returns -- MetricAlarms, NextToken
				MetricAlarms -- A list of information for the specified alarms.
				NextToken -- A string that marks the start of the next batch of returned results.
			"""

		def findadd(m, node, attr):
			node = node.find('{%s}%s' % (self.xmlns, attr))
			if node is not None:
				m[attr] = node.text

		def response(status, reason, data):
			if status == 200:
#				print data
				root = ET.fromstring(data)
				alarms = []
				nodeAlarms = root.find('.//{%s}MetricAlarms' % self.xmlns)
				if nodeAlarms is not None:
					for node in nodeAlarms.findall('{%s}member' % self.xmlns):
						alarm = {}
						findadd(alarm, node, 'AlarmDescription')
						findadd(alarm, node, 'StateUpdatedTimestamp')
						findadd(alarm, node, 'StateReasonData')
						findadd(alarm, node, 'AlarmArn')
						findadd(alarm, node, 'AlarmConfigurationUpdatedTimestamp')
						findadd(alarm, node, 'AlarmName')
						findadd(alarm, node, 'StateValue')
						findadd(alarm, node, 'Period')
						findadd(alarm, node, 'ActionsEnabled')
						findadd(alarm, node, 'Namespace')
						findadd(alarm, node, 'EvaluationPeriods')
						findadd(alarm, node, 'Threshold')
						findadd(alarm, node, 'Statistic')
						findadd(alarm, node, 'StateReason')
						findadd(alarm, node, 'ComparisonOperator')
						findadd(alarm, node, 'MetricName')
						alarms.append(alarm)

				# XXX: Dimensions, OKActions, InsufficientDataActions, AlarmActions (all lists)
				token = None
				node = root.find('.//{%s}NextToken' % self.xmlns)
				if node is not None:
					token = node.text
				return alarms, token
			raise AWSError(status, reason, data)

		r = request.AWSRequest(self._endpoint, '/', self._key, self._secret, 'DescribeAlarms', {
				'ActionPrefix': ActionPrefix,
				'AlarmNamePrefix': AlarmNamePrefix,
				'MaxRecords': MaxRecords,
				'StateValue': StateValue,
				'NextToken': NextToken,
				'Version': self.version,
			}, response)
		if AlarmNames is not None:
			for idx, name in enumerate(AlarmNames):
				r.addParm('AlarmNames.member.%d' % (idx + 1), name)
		return r


	def DescribeAlarmsForMetric(self, Namespace, MetricName, Dimensions=None, Unit=None, Statistic=None, Period=None):
		"""Retrieves all alarms for a single metric. Specify a statistic, period, or unit to filter
			the set of alarms further.

			Dimensions -- The list of dimensions associated with the metric.
				Type: List of Strings
				Length constraints: Minimum of 0 item(s) in the list. Maximum of 10 item(s) in the list.
				Required: No

			MetricName -- The name of the metric.
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.
				Required: Yes

			Namespace -- The namespace of the metric.
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.
				Required: Yes

			Period -- The period in seconds over which the statistic is applied.
				Type: Integer
				Required: No

			Statistic -- The statistic for the metric.
				Type: String
				Valid Values: SampleCount | Average | Sum | Minimum | Maximum
				Required: No

			Unit -- The unit for the metric.
				Type: String
				Valid Values: Seconds | Microseconds | Milliseconds | Bytes | Kilobytes | Megabytes
					| Gigabytes | Terabytes | Bits | Kilobits | Megabits | Gigabits | Terabits | Percent
					| Count | Bytes/Second | Kilobytes/Second | Megabytes/Second | Gigabytes/Second
					| Terabytes/Second | Bits/Second | Kilobits/Second | Megabits/Second | Gigabits/Second
					| Terabits/Second | Count/Second | None
				Required: No

			Returns -- A list of information for each alarm with the specified metric.
			"""

	def DisableAlarmActions(self, AlarmNames):
		"""Disables actions for the specified alarms. When an alarm's actions are disabled the alarm's
			state may change, but none of the alarm's actions will execute.

			AlarmNames -- The names of the alarms to disable actions for.
				Type: List of Strings
				Length constraints: Minimum of 0 item(s) in the list. Maximum of 100 item(s) in the list.
				Required: Yes

			Returns -- True on success
			"""

	def EnableAlarmActions(self, AlarmNames):
		"""Enables actions for the specified alarms.

			AlarmNames -- The names of the alarms to enable actions for.
				Type: List of Strings
				Length constraints: Minimum of 0 item(s) in the list. Maximum of 100 item(s) in the list.
				Required: Yes

			Returns -- True on success
			"""

	def GetMetricStatistics(self, Namespace, MetricName, StartTime, EndTime, Period, Statistics, Unit, Dimensions=None):
		"""Gets statistics for the specified metric.

			NOTE: The maximum number of data points returned from a single GetMetricStatistics request is 1,440.
			If a request is made that generates more than 1,440 data points, Amazon CloudWatch returns an error.
			In such a case, alter the request by narrowing the specified time range or increasing the specified
			period. Alternatively, make multiple requests across adjacent time ranges.

			Amazon CloudWatch aggregates data points based on the length of the period that you specify. For
			example, if you request statistics with a one-minute granularity, Amazon CloudWatch aggregates data
			points with time stamps that fall within the same one-minute period. In such a case, the data points
			queried can greatly outnumber the data points returned.

			NOTE: The maximum number of data points that can be queried is 50,850; whereas the maximum number of
			data points returned is 1,440.

			Dimensions -- A list of dimensions describing qualities of the metric.
				Type: List of Strings
				Length constraints: Minimum of 0 item(s) in the list. Maximum of 10 item(s) in the list.
				Required: No

			EndTime -- The time stamp to use for determining the last datapoint to return. The value specified is
					exclusive; results will include datapoints up to the time stamp specified.
				Type: DateTime
				Required: Yes

			MetricName -- The name of the metric.
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.
				Required: Yes

			Namespace -- The namespace of the metric.
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.
				Required: Yes

			Period -- The granularity, in seconds, of the returned datapoints. Period must be at least 60 seconds
					and must be a multiple of 60. The default value is 60.
				Type: Integer
				Required: Yes

			StartTime -- The time stamp to use for determining the first datapoint to return. The value specified
					is inclusive; results include datapoints with the time stamp specified.
				Type: DateTime
				Required: Yes
				Note: The specified start time is rounded down to the nearest value. Datapoints are returned for
				start times up to two weeks in the past. Specified start times that are more than two weeks in the
				past will not return datapoints for metrics that are older than two weeks.

			Statistics -- The metric statistics to return.
				Type: List of Strings
				Length constraints: Minimum of 1 item(s) in the list. Maximum of 5 item(s) in the list.
				Required: Yes

			Unit -- The unit for the metric.
				Type: String
				Valid Values: Seconds | Microseconds | Milliseconds | Bytes | Kilobytes | Megabytes
					| Gigabytes | Terabytes | Bits | Kilobits | Megabits | Gigabits | Terabits | Percent
					| Count | Bytes/Second | Kilobytes/Second | Megabytes/Second | Gigabytes/Second
					| Terabytes/Second | Bits/Second | Kilobits/Second | Megabits/Second | Gigabits/Second
					| Terabits/Second | Count/Second | None
				Required: Yes

			Returns -- Datapoints, Label
			"""


	def ListMetrics(self, Dimensions=None, MetricName=None, Namespace=None, NextToken=None):
		"""Returns a list of valid metrics stored for the AWS account owner. Returned metrics can be
			used with GetMetricStatistics to obtain statistical data for a given metric.

			NOTE: Up to 500 results are returned for any one call. To retrieve further results, use
			returned NextToken values with subsequent ListMetrics operations.

			NOTE: If you create a metric with the PutMetricData action, allow up to fifteen minutes
			for the metric to appear in calls to the ListMetrics action. Statistics about the metric,
			however, are available sooner using GetMetricStatistics.

			Dimensions -- A list of dimensions to filter against.
				Type: DimensionFilter List
				Length constraints: Minimum of 0 item(s) in the list. Maximum of 10 item(s) in the list.
				Required: No

			MetricName -- The name of the metric to filter against.
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.
				Required: No

			Namespace -- The namespace to filter against
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.
				Required: No

			NextToken -- The token returned by a previous call to indicate that there is more data available.
				Type: String
				Required: No

			Returns -- Metrics, NextToken
			"""


	def PutMetricAlarm(self, Namespace, MetricName, AlarmName, ComparisonOperator, EvaluationPeriods, Period, Statistic, Threshold,
			ActionsEnabled=None, AlarmActions=None, AlarmDescription=None, Dimensions=None, InsufficientDataActions=None, OKActions=None, Unit=None):
		"""Creates or updates an alarm and associates it with the specified Amazon CloudWatch metric.
			Optionally, this operation can associate one or more Amazon Simple Notification Service resources
			with the alarm.

			Returns -- True if succeeded
			"""


	def PutMetricData(self, Namespace, MetricData):
		"""Publishes metric data points to Amazon CloudWatch. Amazon Cloudwatch associates the data points
			with the specified metric. If the specified metric does not exist, Amazon CloudWatch creates the
			metric.

			NOTE: If you create a metric with the PutMetricData action, allow up to fifteen minutes for the
			metric to appear in calls to the ListMetrics action.

			The size of a request is limited to 8 KB for HTTP GET requests and 40 KB for HTTP POST requests.

			Important: Although the Value parameter accepts numbers of type Double, Amazon CloudWatch
			truncates values with very large exponents. Values with base-10 exponents greater than 126
			(1 x 10^126) are truncated. Likewise, values with base-10 exponents less than -130 (1 x 10^-130)
			are also truncated.

			Namespace -- The namespace for the metric data
				Type: String
				Required: Yes

			MetricData -- A list of data describing the metric.
				Type: List of MetricDatums

			Returns -- True if succeeded
			"""

	def SetAlarmState(self, AlarmName, StateValue, StateReason, StateReasonData=None):
		"""Temporarily sets the state of an alarm. When the updated StateValue differs from the previous
			value, the action configured for the appropriate state is invoked. This is not a permanent change.
			The next periodic alarm check (in about a minute) will set the alarm to its actual state.

			AlarmName -- The descriptive name for the alarm. This name must be unique within the user's AWS
					account. The maximum length is 255 characters. 
				Type: String
				Length constraints: Minimum value of 1. Maximum value of 255.

			StateValue -- The value of the state
				Type: String
				Valid Values: OK | ALARM | INSUFFICIENT_DATA
				Required: Yes

			StateReason -- The reason that this alarm is set to this specific state (in human-readable text format)
				Type: String
				Required: Yes

			StateReasonData -- The reason that this alarm is set to this specific state
					(in machine-readable JSON format)
				Type: String
				Required: No

			Returns -- True if succeeded
			"""


if __name__ == '__main__':
	import proxy
	key, secret = getBotoCredentials()
	cw = proxy.ServiceProxy(CloudWatch('us-west-1', key, secret))
	alarms = cw.DescribeAlarms()
	print alarms


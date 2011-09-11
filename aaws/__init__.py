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

from proxy import ServiceProxy, ManagerProxy
from request import AWSRequestManager, AWSRequest
from aws import AWSService, AWSError, getBotoCredentials
from sqs import SQS
from sns import SNS
from ec2 import EC2
from s3 import S3
from route53 import Route53
from simpledb import SimpleDB
import util


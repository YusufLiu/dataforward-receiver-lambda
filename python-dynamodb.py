from __future__ import print_function
import boto3
import json
import uuid
from decimal import *

# Monkey patch Decimal's default Context to allow
# inexact and rounded representation of floats
from boto3.dynamodb.types import DYNAMODB_CONTEXT
# Inhibit Inexact Exceptions
DYNAMODB_CONTEXT.traps[Inexact] = 0

# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[Rounded] = 0


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    dynamo = boto3.resource('dynamodb').Table('senseware')



    for record in event['data']:
        # DynamoDb wants decimals, not floats
        record['value'] = Decimal(record['value'])
        record['raw'] = Decimal(record['raw'])

        # Add the streams meta data to every record (optional)
        record['sn'] = event['sn']
        record['site'] = event['site']
        record['location'] = event['location']
        record['mod'] = event['mod']
        record['sid'] = event['sid']
        record['type'] = event['type']
        record['cid'] = event['cid']
        record['name'] = event['name']
        record['unit'] = event['unit']

        # Give this record an ID
        record['partition'] = str(uuid.uuid4())

        # Put it into the database
        dynamo.put_item(
            TableName='senseware',
            Item=record
        )

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
    dynamo = boto3.resource('dynamodb').Table('senseware')

    # When testing lambda the packet is the request body is the event
    # but when testing through the Api Gateway it is wrapped in an additional
    # request dictionary
    try:
        packet = json.loads(event['body'])
    except KeyError:
        packet = event

    for record in packet['data']:
        # DynamoDb wants decimals, not floats
        record['value'] = Decimal(record['value'])
        record['raw'] = Decimal(record['raw'])

        # Add the streams meta data to every record (optional)
        record['sn'] = packet['sn']
        record['site'] = packet['site']
        record['location'] = packet['location']
        record['mod'] = packet['mod']
        record['sid'] = packet['sid']
        record['type'] = packet['type']
        record['cid'] = packet['cid']
        record['name'] = packet['name']
        record['unit'] = packet['unit']

        # Give this record an ID
        record['partition'] = str(uuid.uuid4())

        # Put it into the database
        dynamo.put_item(
            TableName='senseware',
            Item=record
        )

    # Return a Success Message
    return {
      "statusCode": 200,
      "body": ""
    }
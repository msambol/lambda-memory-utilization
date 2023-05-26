import boto3
import json
import os


# boto3 
account = boto3.client('account')
sqs = boto3.client('sqs')

# globals 
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')


def getEnabledRegions():
    response = account.list_regions(
        MaxResults=50,
        RegionOptStatusContains=['ENABLED', 'ENABLED_BY_DEFAULT']
    ).get('Regions')

    return [r.get('RegionName') for r in response]


def handler(event, context):
    print(json.dumps(event))

    regions = event.get('regions', None)
    days = event.get('days', 30)

    # If regions not provided in input, fetch all enabled AWS Regions
    if not regions:
        regions = getEnabledRegions()
    else: 
        regions = regions.split(',')

    for region in regions:
        print(f'Fetching Lambda log groups in {region}..')

        lmbda = boto3.client('lambda', region_name=region)
        functions = lmbda.list_functions().get('Functions')

        for function in functions:
            function_name = function.get('FunctionName')
            log_group_name = function.get('LoggingConfig').get('LogGroup')

            # Used for calculating cost savings, be greedy and take index 0
            architecture = function.get('Architectures')[0]

            body = {
                'function_name': function_name,
                'log_group_name': log_group_name,
                'architecture': architecture,
                'region': region,
                'days': days,
            }

            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(body),
            )

    return True

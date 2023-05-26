import boto3
import botocore
import datetime
import json
import os
import time


# boto3
firehose = boto3.client('firehose')

# globals 
QUERY_STRING = '''
    filter @type = "REPORT"
    | stats max(@memorySize / 1000 / 1000) as provisioned_memory_mb,
    min(@maxMemoryUsed / 1000 / 1000) as min_memory_used_mb,
    avg(@maxMemoryUsed / 1000 / 1000) as avg_memory_used_mb,
    max(@maxMemoryUsed / 1000 / 1000) as max_memory_used_mb,
    provisioned_memory_mb - max_memory_used_mb as over_provisioned_memory_mb,
    avg(@billedDuration) as avg_billed_duration_ms,
    count(@requestId) as invocations
'''
FIREHOSE_STREAM = os.environ.get('FIREHOSE_STREAM')
DATE = datetime.datetime.today().strftime('%Y-%m-%d')


def handler(event, context):
    print(json.dumps(event))

    for record in event.get('Records', []):
        # Set vars from payload
        body = json.loads(record.get('body'))
        function_name = body.get('function_name')
        log_group_name = body.get('log_group_name')
        architecture = body.get('architecture')
        days = body.get('days')
        region = body.get('region')
        
        # boto3 client depends on log group AWS Region
        logs = boto3.client('logs', region_name=region)

        # Results from CloudWatch Logs Insights
        # Response may not include any results, start at None  
        results = None

        # Start and end times for query
        today = datetime.datetime.now()
        today_minus_days = today - datetime.timedelta(days=days)

        try:
            # Submit query to CloudWatch Logs Insights
            query_id = logs.start_query(
                logGroupName=log_group_name,
                queryString=QUERY_STRING,
                startTime=int(today_minus_days.timestamp()),
                endTime=int(today.timestamp()),
            ).get('queryId')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(f'{log_group_name} has no logs, skipping..')
                continue
            else:
                raise e

        # Loop and wait for query results
        while True:
            time.sleep(5)
            print('Sleeping for 5 seconds, waiting for query results..')

            response = logs.get_query_results(queryId=query_id)

            if response.get('status') not in ['Scheduled', 'Running']:
                # Response may not include results if the Lambda hasn't run in the days specified
                if len(response.get('results')):
                    results = response.get('results')[0]
                break
        
        if results:
            data = {}
            data['function_name'] = function_name
            data['log_group_name'] = log_group_name
            data['architecture'] = architecture
            data['metric_collection_date'] = DATE

            if architecture == 'x86_64':
                cost = 0.0000166667 # per GB-second
            elif architecture == 'arm64':
                cost = 0.0000133334 # per GB-second

            for result in results:
                field = result.get('field')
                value = result.get('value')

                # Cast string values to int and float
                if field == 'provisioned_memory_mb':
                    provisioned_memory_mb = int(value)
                elif field == 'min_memory_used_mb':
                    min_memory_used_mb = int(value)
                elif field == 'avg_memory_used_mb':
                    avg_memory_used_mb = float(value)
                elif field == 'max_memory_used_mb':
                    max_memory_used_mb = int(value)
                elif field == 'over_provisioned_memory_mb':
                    over_provisioned_memory_mb = int(value)
                elif field == 'avg_billed_duration_ms':
                    avg_billed_duration_ms = float(value)
                elif field == 'invocations':
                    invocations = int(value)

            # Minimum memory size in Lambda is 128 MB, can't be more efficient than the minimum
            if (provisioned_memory_mb - over_provisioned_memory_mb) < 128:
                over_provisioned_memory_mb = provisioned_memory_mb - 128

            data['provisioned_memory_mb'] = provisioned_memory_mb
            data['min_memory_used_mb'] = min_memory_used_mb
            data['avg_memory_used_mb'] = avg_memory_used_mb
            data['max_memory_used_mb'] = max_memory_used_mb
            data['over_provisioned_memory_mb'] = over_provisioned_memory_mb
            data['avg_billed_duration_ms'] = avg_billed_duration_ms
            data['invocations'] = invocations

            # Calculate savings: over-provisioned (in GB) * duration (in seconds) * cost * # of invocations
            data['potential_savings'] = ((over_provisioned_memory_mb / 1000) * (avg_billed_duration_ms / 1000)) * cost * invocations

            print(json.dumps(data))

            try:
                # Send data to Firehose, which writes to S3
                firehose.put_record(
                    DeliveryStreamName=FIREHOSE_STREAM,
                    Record={'Data': json.dumps(data)}
                )
                print('Sent memory metrics to Firehose')
            except Exception as e:
                print(f'Failed putting record into Firehose: {str(e)}')
                raise e
        else:
            print('No results returned from CloudWatch Logs Insights (try increasing days), skipping..')
        
    return True

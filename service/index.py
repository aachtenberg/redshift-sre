import json
import boto3
import time
import os
import csv
import io

SQL_STATEMENT_TEMPLATE = (
    "select start_time, end_time, user_id, username, database_name, session_id, query_type, query_text, elapsed_time, queue_time, lock_wait_time "
    "from sys_query_history where status = 'running' and elapsed_time >  {elapsed_time} and end_time is null order by start_time DESC;"
)

def handler(event, context):
    client = boto3.client('redshift-data')
    logs_client = boto3.client('logs')
    
    # Create log group and log stream 
    log_group_name = os.environ['LOG_GROUP_NAME']
    log_stream_name = os.environ['LOG_STREAM_NAME']
    
    try:
        logs_client.create_log_group(logGroupName=log_group_name)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass
    
    try:
        logs_client.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass
    
    elapsed_time = event.get('elapsed_time', int(os.environ.get('ELAPSED_TIME', 0))) * 1000  # Convert to microseconds
    sql_statement = SQL_STATEMENT_TEMPLATE.format(elapsed_time=elapsed_time)
    
    response = client.execute_statement(
        WorkgroupName=os.environ['WORKGROUP_NAME'],
        Database=os.environ['DB_NAME'],
        Sql=sql_statement
    )
    
    # Check query status
    start_time = time.time()
    status = client.describe_statement(Id=response['Id'])['Status']
    while status not in ['FINISHED', 'FAILED', 'ABORTED']:
        time.sleep(1)
        status = client.describe_statement(Id=response['Id'])['Status']
        # Add a timeout condition
        if time.time() - start_time > 300:  # 5 minutes timeout
            raise TimeoutError("Query execution timed out")
    
    if status == 'FINISHED':
        result = client.get_statement_result(Id=response['Id'])
        records = result['Records']
    else:
        records = []
    
    # Log the records to CloudWatch
    seq_token = None
    for data in records:
        data = str(data)
        log_event = {
            'logGroupName': log_group_name,
            'logStreamName': log_stream_name,
            'logEvents': [
                {
                    'timestamp': int(round(time.time() * 1000)),
                    'message': f"{data}"
                }
            ],
        }
        if seq_token:
            log_event['sequenceToken'] = seq_token
        response = logs_client.put_log_events(**log_event)
        seq_token = response['nextSequenceToken']
        time.sleep(1)
    
    # Create CloudWatch metric for the number of rows
    cloudwatch_client = boto3.client('cloudwatch')
    cloudwatch_client.put_metric_data(
        Namespace='RedshiftQueryMetrics',
        MetricData=[
            {
                'MetricName': 'RowsExceedingElapsedTimeThreshold',
                'Dimensions': [
                    {
                        'Name': 'LogGroupName',
                        'Value': log_group_name
                    },
                    {
                        'Name': 'LogStreamName',
                        'Value': log_stream_name
                    }
                ],
                'Value': len(records),
                'Unit': 'Count'
            }
        ]
    )
       
    return {
        'statusCode': 200,
        'body': json.dumps(records)
    }
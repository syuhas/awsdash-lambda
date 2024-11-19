import boto3
import json
from loguru import logger

accounts = [
    {
        'account_id': '551796573889',
        'region': 'us-east-1',
        'role_arn': 'arn:aws:iam::551796573889:role/jenkinsAdminXacnt',
        'queue_arn': 'arn:aws:sqs:us-east-1:551796573889:S3Notifications'
    },
    {
        'account_id': '061039789243',
        'region': 'us-east-1',
        'role_arn': 'arn:aws:iam::061039789243:role/jenkinsAdminXacnt',
        'queue_arn': 'arn:aws:sqs:us-east-1:551796573889:S3Notifications'
    }
]

def lambda_handler(event, context):
    logger.info("Event: {}", event)
    logger.info("Context: {}", context)
    for account in accounts:
        for bucket in getBuckets(account):
            if 'aws-cloudtrail-logs' in bucket['Name']:
                print(f"Skipping {bucket['Name']}")
                continue
            else:
                response = checkBucketConfigurationExists(account, bucket['Name'])
                if 'QueueConfigurations' not in response:
                    response = enrollBucketNotifications(account, bucket['Name'])
                else:
                    print(f"Bucket {bucket['Name']} already has notifications enabled")
            


def enrollBucketNotifications(account: dict, bucket: dict):
    session = getAccountSession(account)
    s3 = session.client('s3')
    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={
            'QueueConfigurations': [
                {
                    'QueueArn': account['queue_arn'],
                    'Events': ['s3:ObjectCreated:Put', 's3:ObjectCreated:Post', 's3:ObjectRemoved:Delete', 's3:ObjectRemoved:DeleteMarkerCreated']
                }
            ]
        }
    )

def checkBucketConfigurationExists(account: str, bucket: dict):
    session = getAccountSession(account)
    s3 = session.client('s3')
    response = s3.get_bucket_notification_configuration(Bucket=bucket)
    return response

def getBuckets(account):
    session = getAccountSession(account)
    s3 = session.client('s3')
    response = s3.list_buckets()
    return response['Buckets']


def getAccountSession(account: dict) -> boto3.session.Session:
    session = boto3.Session()
    sts = session.client('sts')
    response = sts.assume_role(
        RoleArn=account['role_arn'],
        RoleSessionName='s3-backfill',
        DurationSeconds=900
    )
    credentials = response['Credentials']
    account_session = boto3.Session(
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=account['region']
    )
    return account_session

import boto3
import json
from loguru import logger
from sqlalchemy import create_engine, select, and_
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DECIMAL, BigInteger
from botocore.exceptions import ClientError
from pathlib import Path

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

Base = declarative_base()
class S3BUCKETS(Base):
    __tablename__ = 's3'
    id = Column(Integer, primary_key=True)
    account_id = Column(String)
    bucket = Column(String)
    totalSizeBytes = Column(BigInteger)
    totalSizeKb = Column(DECIMAL)
    totalSizeMb = Column(DECIMAL)
    totalSizeGb = Column(DECIMAL)
    costPerMonth = Column(DECIMAL)
    objects = relationship('S3BUCKETOBJECTS', backref='s3objects')
    def __repr__(self):
        return f'Bucket {self.bucket}'

class S3BUCKETOBJECTS(Base):
    __tablename__ = 's3objects'
    id = Column(Integer, primary_key=True)
    bucket_id = Column(Integer, ForeignKey('s3.id'))
    bucket = Column(String)
    key = Column(String)
    sizeBytes = Column(BigInteger)
    sizeKb = Column(DECIMAL)
    sizeMb = Column(DECIMAL)
    sizeGb = Column(DECIMAL)
    costPerMonth = Column(DECIMAL)

def lambda_handler(event, context):
    logger.info("Event Recieved: {}", event)
    logger.info("Context: {}", context)
    # filepath = Path(__file__).resolve().parent / 'tests/delete2.json'
    # with open(filepath, 'r') as file:
    #     event = json.load(file)


    try:
        for record in event['Records']:
            eventName = record['body']['detail']['eventName']
            bucket_name = record['body']['detail']['requestParameters']['bucketName']
            account = record['body']['account']

    except KeyError as e:
        logger.error(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error parsing event')
        }

    if eventName == 'CreateBucket':
        logger.info("Bucket created: {}. Adding to database", bucket_name)
        addBucketToDatabase(bucket_name, account)
        logger.info("Bucket {} added to database", bucket_name)
        enrollAllBucketEventNotifications()

    if eventName == 'DeleteBucket':
        logger.info("Bucket deleted: {}. Removing from database", bucket_name)
        deleteBucketsandObjectsFromDatabase(bucket_name)
        logger.info("Bucket {} removed from database", bucket_name)



def addBucketToDatabase(bucket_name: str, account_id: str):
    db = getDatabaseSession()
    bucket = S3BUCKETS(
        bucket=bucket_name, 
        account_id=account_id,
        totalSizeBytes = 0,
        totalSizeKb = 0,
        totalSizeMb = 0,
        totalSizeGb = 0,
        costPerMonth = 0
    )
    db.add(bucket)
    db.commit()
    db.close()

def deleteBucketsandObjectsFromDatabase(bucket_name: str):
    db = getDatabaseSession()

    objects = db.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.bucket == bucket_name).all()
    if objects:
        for obj in objects:
            db.delete(obj)

    bucket = db.query(S3BUCKETS).filter(S3BUCKETS.bucket == bucket_name).first()
    if bucket:
        db.delete(bucket)
        db.commit()

def enrollAllBucketEventNotifications():
    for account in accounts:
        for bucket in getBuckets(account):
            if 'aws-cloudtrail-logs' in bucket['Name']:
                print(f"Skipping {bucket['Name']}")
                continue
            else:
                response = checkBucketConfigurationExists(account, bucket['Name'])
                if 'QueueConfigurations' not in response:
                    logger.info("Enrolling bucket {} in account {}", bucket['Name'], account['account_id'])
                    try:
                        enrollBucketNotifications(account, bucket['Name'])
                        logger.info("Bucket {} enrolled in account {}", bucket['Name'], account['account_id'])
                    except Exception as e:
                        logger.error("Error enrolling bucket {} in account {}: {}", bucket['Name'], account['account_id'], e)
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

def getDatabaseCredentials() -> dict:
    secret_id = "arn:aws:secretsmanager:us-east-1:061039789243:secret:rds!db-555390f8-60f2-4d37-ad75-e63d8f0cbfa9-0s9oyX"
    region = "us-east-1"
    session = boto3.session.Session()
    client = session.client('secretsmanager', region_name=region)

    try:
        secret_response = client.get_secret_value(SecretId=secret_id)
        secret = secret_response['SecretString']
        json_secret = json.loads(secret)
        credentials = {
            'username': json_secret['username'],
            'password': json_secret['password']
        }
        return credentials
    except ClientError as e:
        raise e

def getEngine() -> create_engine:
    credentials = getDatabaseCredentials()
    engine = create_engine(
        f'postgresql://{credentials["username"]}:{credentials["password"]}@resources.czmo2wqo0w7e.us-east-1.rds.amazonaws.com:5432'
    )
    return engine

def getDatabaseSession() -> sessionmaker:
    engine = getEngine()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

if __name__ == '__main__':
    lambda_handler(None, None)
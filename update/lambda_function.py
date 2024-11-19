import boto3
import json
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DECIMAL, BigInteger
from botocore.exceptions import ClientError
from pathlib import Path

accounts = {
    '551796573889': {
        'region': 'us-east-1',
        'role_arn': 'arn:aws:iam::551796573889:role/jenkinsAdminXacnt'
    },
    '061039789243':{
        'region': 'us-east-1',
        'role_arn': 'arn:aws:iam::061039789243:role/jenkinsAdminXacnt'
    }
}

def lambda_handler(event, context):
    try:

        eventName = event['detail']['eventName']

        if eventName == 'PutObject':
            key = event['detail']['requestParameters']['key']
            bucket_name = event['detail']['requestParameters']['bucketName']
            account = accounts[event['account']]
            addObject(event, account, bucket_name, key)
        if eventName == 'DeleteObject':
            key = event['detail']['requestParameters']['key']
            bucket_name = event['detail']['requestParameters']['bucketName']
            account = accounts[event['account']]
            deleteObject(event, account, bucket_name, key)


        
        if event:
            if event['detail']['eventName'] == 'PutObject':

                db_session = getDatabaseSession()

                key = event['detail']['requestParameters']['key']
                bucket_name = event['detail']['requestParameters']['bucketName']
                account = accounts[event['account']]

                print(account)
                bucket_id = db_session.query(S3BUCKETS).filter(S3BUCKETS.bucket == bucket_name).first().id

                obj_data = getObjectData(account, bucket_name, key)

                existing_object = db_session.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.key == key).first()

                print(bucket_name)
                print(key)


                if existing_object:
                    deleteObject(db_session, existing_object)
                    addObject(db_session, bucket_id, obj_data, bucket_name)
                else:
                    addObject(db_session, bucket_id, obj_data, bucket_name)
                db_session.commit()
                db_session.close()
                logger.info(f"Added new object {key} to bucket {bucket_name} in the database.")

    except Exception as e:
        logger.exception(e)
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred')
        }

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


    


def addBucket(session: sessionmaker, bucket: dict, account_id: str):
    bucket_name = bucket['bucket']
    
    s3_bucket = S3BUCKETS(
        account_id=account_id,
        bucket=bucket_name,
        totalSizeBytes=bucket['totalSizeBytes'],
        totalSizeKb=bucket['totalSizeKb'],
        totalSizeMb=bucket['totalSizeMb'],
        totalSizeGb=bucket['totalSizeGb'],
        costPerMonth=bucket['costPerMonth']
    )
    session.add(s3_bucket)
    session.commit()
    logger.info(f"Added new bucket {bucket_name} to the database.")
    session.refresh(s3_bucket)
    return s3_bucket

def addObject(session: sessionmaker, bucket_id: str, obj: dict, bucket_name: str):
    s3_object = S3BUCKETOBJECTS(
        bucket_id=bucket_id,
        bucket=bucket_name,
        key=obj['key'],
        sizeBytes=obj['sizeBytes'],
        sizeKb=obj['sizeKb'],
        sizeMb=obj['sizeMb'],
        sizeGb=obj['sizeGb'],
        costPerMonth=obj['costPerMonth']
    )
    session.add(s3_object)
    logger.info(f"Adding new object {obj['key']} to bucket {bucket_name} in the database.")

def deleteBucket(session: sessionmaker, bucket_data: S3BUCKETS):
    objects = session.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.bucket_id == bucket_data.id).all()
    for obj in objects:
        session.delete(obj)
    bucket = session.query(S3BUCKETS).filter(S3BUCKETS.id == bucket_data.id).first()
    if bucket:
        session.delete(bucket)
        session.commit()
        logger.info(f"Deleted bucket {bucket.bucket} from the database.")
    else:
        logger.info(f"Bucket {bucket.bucket} not found in the database. Could not delete.")

def deleteObject(session: sessionmaker, obj: S3BUCKETOBJECTS):
    obj = session.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.id == obj.id).first()
    if obj:
        session.delete(obj)
        session.commit()
        logger.info(f"Deleted object {obj.key} from the database.")
    else:
        logger.info(f"Object {obj.key} not found in the database. Could not delete.")

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

def getObjectData(account: dict, bucket_name: str, obj_name: str):
    try:
        session = getAccountSession(account)
        s3 = session.client('s3')
        obj = s3.get_object_attributes(Bucket=bucket_name, Key=obj_name, ObjectAttributes=['ObjectSize'])
        size = obj['ObjectSize']
        obj_dict = {
            'key': obj_name,
            'sizeBytes': size,
            'sizeKb': round(size / 1024, 2),
            'sizeMb': round(size / (1024 * 1024), 2),
            'sizeGb': round(size / (1024 * 1024 * 1024), 4)
        }
        obj_dict['costPerMonth'] = obj_dict['sizeGb'] * 0.023
        return obj_dict
    except ClientError as e:
        logger.exception(e)
        return None


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

def getBuckets(account_id: str) -> list:
    session = getDatabaseSession()
    result = session.query(S3BUCKETS).filter(S3BUCKETS.account_id == account_id).all()
    session.close()
    return result

def getObjectsForBucket(bucket_id: int) -> list:
    session = getDatabaseSession()
    result = session.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.bucket_id == bucket_id).all()
    session.close()
    return result



if __name__ == "__main__":
    lambda_handler(None, None)

from typing import List, Dict, Union
import boto3
import json
import boto3.session
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DECIMAL, BigInteger
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
import time

account_lookup = [
    {
        'sdlc': 'prod',
        'account_id': '551796573889',
        'region': 'us-east-1',
        'role_arn': 'arn:aws:iam::551796573889:role/jenkinsAdminXacnt'
    },
    {
        'sdlc': 'dev',
        'account_id': '061039789243',
        'region': 'us-east-1',
        'role_arn': 'arn:aws:iam::061039789243:role/jenkinsAdminXacnt'
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
    logger.info("Starting backfill process...")
    start_time = time.time()

    for account in account_lookup:


        logger.info("Retrieving existing buckets for {}...", account['account_id'])

        session = getAccountSession(account)

        buckets = getBucketsData(session)

        logger.info("Backfilling database for {}...", account['account_id'])

        backfillDatabase(buckets, account['account_id'])
    
    end_time = time.time()

    logger.info(f"Execution time: {end_time - start_time} seconds")


def backfillDatabase(buckets: List[dict], account_id: str) -> dict:
    try:
        current_keys = set()
        session = getDatabaseSession()
        existing_db_buckets = {b.bucket: b.id for b in session.query(S3BUCKETS).filter(S3BUCKETS.account_id == account_id).all()}
        existing_db_objects = {(obj.bucket_id, obj.key): {
                                    "id": obj.id,
                                    "hash": hash((obj.sizeBytes, obj.costPerMonth))
        } for obj in session.query(S3BUCKETOBJECTS).join(S3BUCKETS, S3BUCKETS.id == S3BUCKETOBJECTS.bucket_id).filter(S3BUCKETS.account_id == account_id).all()}

        try:
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(processBucket, session, bucket, existing_db_buckets, existing_db_objects, account_id, current_keys) for bucket in buckets
                ]
                for future in futures:
                    future.result()

            current_buckets = {bucket['bucket'] for bucket in buckets}
             
        except Exception as e:
            logger.exception(e)
            session.rollback()
            return {
                'statusCode': 500,
                'body': json.dumps(f'An error occurred: {e}')
            }
            
        
        objects_to_delete = set(existing_db_objects.keys()) - current_keys
        buckets_to_delete = set(existing_db_buckets.keys()) - current_buckets
        for obj in objects_to_delete:
            logger.info(f'Deleting object from database {obj}')
            deleteObject(session, existing_db_objects[obj])
        for bucket in buckets_to_delete:
            logger.info(f'Deleting bucket from database {bucket}')
            deleteBucket(session, existing_db_buckets[bucket])

    except Exception as e:
        session.rollback()
        return {
            'statusCode': 500,
            'body': json.dumps(f'An error occurred: {e}')
        }
    finally:
        session.commit()
        session.close()



def processBucket(session: sessionmaker, bucket: Dict, existing_db_buckets: Dict, existing_db_objects: Dict, account_id: str, current_keys: set) -> None:
    bucket_id = existing_db_buckets.get(bucket['bucket'])
    try:
        if bucket_id:
            logger.info(f'Bucket {bucket["bucket"]} already exists in the database.')
            if bucketNeedsUpdate(session, bucket_id, bucket, account_id):
                modifyBucket(session, bucket_id, bucket, account_id)
                logger.info(f'Bucket {bucket["bucket"]} updated.')
        else:
            logger.info(f'Adding bucket {bucket["bucket"]} to the database.')
            bucket_id = addBucket(session, bucket, account_id)

        for obj in bucket['objects']:
            object_key = (bucket_id, obj['key'])
            if object_key in existing_db_objects:
                logger.info(f'{obj["key"]} exists in the database.')
                existing_object = existing_db_objects[object_key]
                if objectNeedsUpdate(existing_object, obj):
                    modifyObject(session, existing_object['id'], obj)

            else:
                logger.info(f'Adding object {obj["key"]} to the database.')
                addObject(session, bucket_id, obj)

            current_keys |= ({(bucket_id, obj['key'])})

    except Exception as e:
        logger.exception(e)




#################################### Helper Functions for Buckets ##################################################

def bucketNeedsUpdate(session, bucket_id: int, bucket: Dict, account_id: str) -> bool:
    existing_bucket = session.query(S3BUCKETS).filter(S3BUCKETS.id == bucket_id).one()
    return (
        existing_bucket.account_id != account_id or
        existing_bucket.totalSizeBytes != bucket['totalSizeBytes'] or
        existing_bucket.totalSizeKb != bucket['totalSizeKb'] or
        existing_bucket.totalSizeMb != bucket['totalSizeMb'] or
        existing_bucket.totalSizeGb != bucket['totalSizeGb'] or
        existing_bucket.costPerMonth != bucket['costPerMonth']
    )

def modifyBucket(session, bucket_id: int, bucket: Dict, account_id: str) -> None:
    session.query(S3BUCKETS).filter(S3BUCKETS.id == bucket_id).update({
        "account_id": account_id,
        "totalSizeBytes": bucket["totalSizeBytes"],
        "totalSizeKb": bucket["totalSizeKb"],
        "totalSizeMb": bucket["totalSizeMb"],
        "totalSizeGb": bucket["totalSizeGb"],
        "costPerMonth": bucket["costPerMonth"],
    })


def addBucket(session: sessionmaker, bucket: Dict, account_id: str) -> str:
    new_bucket = S3BUCKETS(
        account_id=account_id,
        bucket=bucket['bucket'],
        totalSizeBytes=bucket['totalSizeBytes'],
        totalSizeKb=bucket['totalSizeKb'],
        totalSizeMb=bucket['totalSizeMb'],
        totalSizeGb=bucket['totalSizeGb'],
        costPerMonth=bucket['costPerMonth']
    )
    session.add(new_bucket)
    return new_bucket.id

def deleteBucket(session: sessionmaker, bucket_id: int) -> None:
    session.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.bucket_id == bucket_id).delete()
    session.query(S3BUCKETS).filter(S3BUCKETS.id == bucket_id).delete()

##################################### Helper Functions for Objects #################################################

def objectNeedsUpdate(existing_object, object: Dict) -> bool:
    return (
        existing_object['hash'] != object['hash']
    )

def modifyObject(session: sessionmaker, object_id: int, object: Dict) -> None:
    session.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.id == object_id).update({
        "sizeBytes": object["sizeBytes"],
        "sizeKb": object["sizeKb"],
        "sizeMb": object["sizeMb"],
        "sizeGb": object["sizeGb"],
        "costPerMonth": object["costPerMonth"]
    })

def addObject(session: sessionmaker, bucket_id: int, obj: Dict) -> None:
    new_object = S3BUCKETOBJECTS(
        bucket_id=bucket_id,
        bucket=obj['bucket'],
        key=obj['key'],
        sizeBytes=obj['sizeBytes'],
        sizeKb=obj['sizeKb'],
        sizeMb=obj['sizeMb'],
        sizeGb=obj['sizeGb'],
        costPerMonth=obj['costPerMonth']
    )
    session.add(new_object)

def deleteObject(session: sessionmaker, object_id: int) -> None:
    session.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.id == object_id).delete()
    session.commit()

##################################### AWS Helper Functions #################################################

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

def getBucketsData(session: boto3.session.Session) -> list:
    logger.info('Retrieving S3 bucket data from AWS...')
    bucket_list = []

    s3 = session.client('s3')
    buckets = s3.list_buckets()

    for bucket in buckets['Buckets']:
        
        bucket_dict = {
            'bucket': bucket['Name'],
            'totalSizeBytes': 0,
            'totalSizeKb': 0,
            'totalSizeMb': 0,
            'totalSizeGb': 0,
            'costPerMonth': 0,
            'objects': []
        }
        
        paginator = s3.get_paginator('list_objects_v2')
    
        object_list = []
    
        total_bucket_cost = 0
    
        object_iterator = paginator.paginate(Bucket=bucket['Name'])
        for page in object_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    object_dict = {
                        'key': obj['Key'],
                        'bucket': bucket['Name'],
                        'sizeBytes': obj['Size'],
                        'sizeKb': round(obj['Size'] / 1024, 2),  # Converts bytes to KB
                        'sizeMb': round(obj['Size'] / (1024 * 1024), 2),  # Converts bytes to MB
                        'sizeGb': round(obj['Size'] / (1024 * 1024 * 1024), 4),  # Converts bytes to GB
                    }
                    object_dict['costPerMonth'] = object_dict['sizeGb'] * 0.023
                    object_dict['hash'] = hash((object_dict['sizeBytes'], object_dict['costPerMonth']))
                    total_bucket_cost = total_bucket_cost + object_dict['costPerMonth']
                    object_list.append(object_dict)

        bucket_dict['totalSizeBytes'] = sum([obj['sizeBytes'] for obj in object_list])
        bucket_dict['totalSizeKb'] = round(sum([obj['sizeKb'] for obj in object_list]), 4)
        bucket_dict['totalSizeMb'] = round(sum([obj['sizeMb'] for obj in object_list]), 4)
        bucket_dict['totalSizeGb'] = round(sum([obj['sizeGb'] for obj in object_list]), 4)
        bucket_dict['costPerMonth'] = round(total_bucket_cost, 6)
        bucket_dict['objects'] = object_list
        bucket_list.append(bucket_dict)

    logger.info('S3 bucket data retrieved.')
    return bucket_list

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

##################################### Database Helper Functions #################################################

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
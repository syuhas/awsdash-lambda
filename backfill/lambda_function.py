import boto3
import json
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, ForeignKey, DECIMAL, BigInteger
from botocore.exceptions import ClientError

accounts = [
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

def lambda_handler(event, context):
    logger.info("Retrieving existing buckets...")
    for account in accounts:
        session = getAccountSession(account)
        buckets = getBucketsData(session)
        response = backfillDatabase(buckets, account['account_id'])
        logger.info(response)

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



def backfillDatabase(buckets: list, account_id: str) -> dict:
    try:
        existing_buckets = {bucket.bucket: bucket for bucket in getBuckets(account_id)}
        session = getDatabaseSession()

        for bucket_data in buckets:
            bucket_name = bucket_data['bucket']
            if bucket_name in existing_buckets:
                s3_bucket = existing_buckets[bucket_name]
                if bucket_data['totalSizeBytes'] != s3_bucket.totalSizeBytes:
                    s3_bucket = modifyBucket(session, s3_bucket.id, bucket_data)
                else:
                    logger.info(f"{account_id}: Bucket {bucket_name} already exists. Skipping adding it to the database...")
            else:
                s3_bucket = addBucket(session, bucket_data, account_id)

            logger.info(f"Retrieving existing objects for bucket {bucket_name}...")
            existing_objects = {obj.key: obj for obj in getObjectsForBucket(s3_bucket.id)}
            for obj in bucket_data['objects']:
                obj_key = obj['key']
                if obj_key in existing_objects:
                    existing_object = existing_objects[obj_key]
                    if obj['sizeBytes'] != existing_object.sizeBytes:
                        modifyObject(session, obj, obj_key)
                        logger.info(f"{account_id}: Object {obj_key} modified in the database.")
                    else:
                        # logger.info(f"{account_id}: Object {obj_key} already exists for bucket {bucket_name}. Skipping...")
                        continue
                else:
                    addObject(session, s3_bucket, obj, bucket_name)

            for obj_key, obj in existing_objects.items():
                if obj_key not in [object['key'] for object in bucket_data['objects']]:
                    logger.info(f"{account_id}: Object {obj_key} not found in AWS. Deleting from database...")
                    deleteObject(session, obj)

        for bucket_name, bucket_data in existing_buckets.items():
            if bucket_name not in [bucket['bucket'] for bucket in buckets]:
                logger.info(f"{account_id}: Bucket {bucket_name} not found in AWS. Deleting from database...")
                deleteBucket(session, bucket_data)

            session.commit()
        session.close()
        return {
            'statusCode': 200,
            'body': json.dumps('Operation Complete')
        }
    except Exception as e:
        logger.exception(e)
        return {
            'statusCode': 500,
            'body': json.dumps('An error occurred')
        }
    


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

def addObject(session: sessionmaker, s3_bucket: dict, obj: dict, bucket_name: str):
    s3_object = S3BUCKETOBJECTS(
        bucket_id=s3_bucket.id,
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

def modifyBucket(session: sessionmaker, bucket_id: int, bucket: dict):
    modified_bucket = session.query(S3BUCKETS).filter(S3BUCKETS.id == bucket_id).first()
    if modified_bucket:
        modified_bucket.totalSizeBytes = bucket['totalSizeBytes']
        modified_bucket.totalSizeKb = bucket['totalSizeKb']
        modified_bucket.totalSizeMb = bucket['totalSizeMb']
        modified_bucket.totalSizeGb = bucket['totalSizeGb']
        modified_bucket.costPerMonth = bucket['costPerMonth']
        logger.info(f"Modified bucket {bucket['bucket']} in the database.")
    else:
        logger.info(f"Bucket {bucket['bucket']} not found in the database. Could not modify.")
    session.refresh(modified_bucket)
    return modified_bucket

def modifyObject(session: sessionmaker, obj: dict, obj_key: str):
    modified_obj = session.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.key == obj_key).first()
    if modified_obj:
        modified_obj.sizeBytes = obj['sizeBytes']
        modified_obj.sizeKb = obj['sizeKb']
        modified_obj.sizeMb = obj['sizeMb']
        modified_obj.sizeGb = obj['sizeGb']
        modified_obj.costPerMonth = obj['costPerMonth']
        logger.info(f"Modified object {obj_key} in the database.")
    else:
        logger.info(f"Object {obj_key} not found in the database. Could not modify.")
    session.commit()

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

def getBucketsData(session: boto3.session.Session):
    logger.info('Retrieving S3 bucket data from AWS...')
    s3 = session.client('s3')
    buckets = s3.list_buckets()
    bucket_list = []
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
        objects = s3.list_objects_v2(Bucket=bucket['Name'])
        object_list = []
        total_bucket_cost = 0
        for obj in objects['Contents']:
            object_dict = {
                'key': obj['Key'],
                'sizeBytes': obj['Size'],
                'sizeKb': round(obj['Size'] / 1024, 2),  # Converts bytes to KB
                'sizeMb': round(obj['Size'] / (1024 * 1024), 2),  # Converts bytes to MB
                'sizeGb': round(obj['Size'] / (1024 * 1024 * 1024), 4),  # Converts bytes to GB
            }
            object_dict['costPerMonth'] = object_dict['sizeGb'] * 0.023
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

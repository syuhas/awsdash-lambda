import boto3
import json
from loguru import logger
from sqlalchemy import create_engine, select, and_
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
    logger.info('Event Recieved: {}', event)
    logger.info("Context: {}", context)

    # filepath = Path(__file__).resolve().parent / 'delete.json'
    # with open(filepath, 'r') as file:
    #     event = json.load(file)

    try:
        if 'Records' in event:
            for record in event['Records']:
                body = json.loads(record['body'])
                if 'Records' in body:
                    for b in body['Records']:
                        eventName = b['eventName']
                        bucket_name = b['s3']['bucket']['name']
                        key = b['s3']['object']['key']

                        if eventName == 'ObjectCreated:Put' or eventName == 'ObjectCreated:Post':
                            addObject(bucket_name, key)            
                        if eventName == 'ObjectRemoved:Delete' or eventName == 'ObjectRemoved:DeleteMarkerCreated':
                            deleteObject(bucket_name, key)
                            
                else:
                    return {
                        'statusCode': 400,
                        'body': json.dumps('Could not process event.')
                    }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps('Could not process event.')
            }
        
    except KeyError as e:
        logger.exception(e)
        return {
            'statusCode': 400,
            'body': json.dumps('Could not process event.')
        }




def addObject(bucket_name: str, key: str):
    try:
        #get the bucket account
        db = getDatabaseSession()
        bucket = db.query(S3BUCKETS).filter(S3BUCKETS.bucket == bucket_name).first()
        account_id = bucket.account_id
        bucket_id = bucket.id

        #asssume the role for that account and get the object size using s3.get_object_attributes
        session = getAccountSession(accounts[account_id])
        s3 = session.client('s3')
        size = s3.get_object_attributes(Bucket=bucket_name, Key=key, ObjectAttributes=['ObjectSize'])['ObjectSize']

        #check that the object is not already in the database

        obj = db.query(S3BUCKETOBJECTS).filter(S3BUCKETOBJECTS.key == key).first()
        if obj:
            deleteObject(bucket_name, key)

        #define the object 
        obj_dict = {
            'key': key,
            'sizeKb': round(size / 1024, 2),
            'sizeMb': round(size / (1024 * 1024), 2),
            'sizeGb': round(size / (1024 * 1024 * 1024), 4)
        }
        obj_dict['costPerMonth'] = obj_dict['sizeGb'] * 0.023

        #create the object
        s3_object = S3BUCKETOBJECTS(
            bucket_id=bucket_id,
            bucket=bucket_name,
            key=key,
            sizeBytes=size,
            sizeKb=obj_dict['sizeKb'],
            sizeMb=obj_dict['sizeMb'],
            sizeGb=obj_dict['costPerMonth'],
            costPerMonth=obj_dict['costPerMonth']
        )

        #add the object to the database
        db.add(s3_object)
        logger.info(f"Adding new object {s3_object.key} to bucket {bucket_name} in the database.")


        updateBucket(bucket_name, s3_object, True)
        
        db.commit()
        db.close()
        
    except Exception as e:
        logger.exception(e)
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }

def updateBucket(bucket_name: str, obj: S3BUCKETOBJECTS, add: bool = True):
    try:
        db = getDatabaseSession()
        bucket = db.query(S3BUCKETS).filter(S3BUCKETS.bucket == bucket_name).first()

        if add:
            newSize = bucket.totalSizeBytes + obj.sizeBytes
        else:
            newSize = bucket.totalSizeBytes - obj.sizeBytes
        
        bucket.totalSizeBytes = newSize
        bucket.totalSizeKb = round(newSize / 1024, 2)
        bucket.totalSizeMb = round(newSize / (1024 * 1024), 2)
        bucket.totalSizeGb = round(newSize / (1024 * 1024 * 1024), 4)
        gb = bucket.totalSizeGb
        bucket.costPerMonth = gb * 0.023

        db.commit()
        db.close()
    except Exception as e:
        logger.exception(e)
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }

def deleteObject(bucket_name: str, key: str):
    try:
        #get the bucket account
        db = getDatabaseSession()
        obj = db.query(S3BUCKETOBJECTS).filter(
            and_(
                S3BUCKETOBJECTS.bucket == bucket_name,
                S3BUCKETOBJECTS.key == key
            )
        ).first()

        updateBucket(bucket_name, obj, False)

        if obj:
            db.delete(obj)
            logger.info(f"Deleted object {obj.key} from the database.")
            db.commit()
            db.close()
        else:
            logger.info(f"Object {key} not found in the database. Could not delete.")

    except Exception as e:
        logger.exception(e)
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }

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
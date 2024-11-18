from sqlalchemy import Column, Integer, String, ForeignKey, BigInteger, DECIMAL
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import boto3
import asyncio
from botocore.exceptions import ClientError
import json

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

credentials = getDatabaseCredentials()

engine = create_async_engine(f'postgresql+asyncpg://{credentials["username"]}:{credentials["password"]}@resources.czmo2wqo0w7e.us-east-1.rds.amazonaws.com:5432')

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

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

asyncio.run(main())

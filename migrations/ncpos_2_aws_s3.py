import boto3
from botocore.client import Config
import os
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')

class MigrationHandler:
    def __init__(self):
        self.setup_clients()
        self.setup_logging()
        self.source_bucket = "YOUR_SOURCE_BUCKET"  # NCP 버킷 이름 지정 필요
        self.dest_bucket = "YOUR_DEST_BUCKET"      # AWS 버킷 이름 지정 필요

    def setup_clients(self):
        """NCP와 AWS 클라이언트 설정"""
        self.ncp_client = boto3.client(
            's3',
            aws_access_key_id="ncp_iam_BPAMKR47Y5KhVyITRlr0",
            aws_secret_access_key="ncp_iam_BPKMKR4qFrz7AucMedAGT9TbBNGjWCbAUx",
            endpoint_url='https://kr.object.ncloudstorage.com',
            config=Config(signature_version='s3v4')
        )
        
        self.aws_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name='ap-northeast-2'
        )

    def setup_logging(self):
        """로깅 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('migration.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def list_objects(self, prefix: str = "") -> list:
        """NCP 버킷의 객체 리스트 조회"""
        try:
            objects = []
            paginator = self.ncp_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.source_bucket, Prefix=prefix):
                if 'Contents' in page:
                    objects.extend(page['Contents'])
            
            return objects
        except Exception as e:
            self.logger.error(f"Error listing objects: {str(e)}")
            raise

    def migrate_object(self, obj: dict, retry_count: int = 3) -> bool:
        """단일 객체 마이그레이션"""
        object_key = obj['Key']
        
        for attempt in range(retry_count):
            try:
                # 객체 다운로드
                response = self.ncp_client.get_object(
                    Bucket=self.source_bucket,
                    Key=object_key
                )
                
                # AWS에 업로드
                self.aws_client.upload_fileobj(
                    response['Body'],
                    self.dest_bucket,
                    object_key
                )
                
                self.logger.info(f"Successfully migrated: {object_key}")
                return True
                
            except Exception as e:
                self.logger.error(f"Error migrating {object_key}: {str(e)}")
                if attempt == retry_count - 1:  # 마지막 시도였다면
                    return False
                
                self.logger.info(f"Retrying... ({attempt + 1}/{retry_count})")
        
        return False

    def run_migration(self, prefix: str = ""):
        """전체 마이그레이션 실행"""
        objects = self.list_objects(prefix)
        total = len(objects)
        success = 0
        
        self.logger.info(f"Starting migration of {total} objects")
        
        for i, obj in enumerate(objects, 1):
            if self.migrate_object(obj):
                success += 1
            
            self.logger.info(f"Progress: {i}/{total} (Success: {success})")
        
        self.logger.info(f"Migration completed. Total: {total}, Success: {success}, Failed: {total-success}")

# 실행 코드
if __name__ == "__main__":
    handler = MigrationHandler()
    handler.run_migration()  # 특정 prefix로 필터링하려면 prefix 인자 전달
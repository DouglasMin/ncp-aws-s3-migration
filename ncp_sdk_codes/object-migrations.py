import boto3
import os
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# NCP 설정
ncp_endpoint = 'https://kr.object.ncloudstorage.com'
ncp_access_key = os.environ.get('NCP_ACCESS_KEY')
ncp_secret_key = os.environ.get('NCP_SECRET_KEY')

# AWS 설정
aws_access_key = os.environ.get('AWS_ACCESS_KEY')
aws_secret_key = os.environ.get('AWS_SECRET_KEY')
aws_region = 'ap-northeast-2'

class StorageMigration:
    def __init__(self):
        # 로깅 설정
        self.setup_logging()
        
        # NCP 클라이언트 설정
        self.ncp_client = boto3.client(
            's3',
            endpoint_url=ncp_endpoint,
            aws_access_key_id=ncp_access_key,
            aws_secret_access_key=ncp_secret_key
        )
        
        # AWS 클라이언트 설정
        self.aws_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        # 버킷 이름 설정 (여기서 소스 포인트, 엔드포인트를 설정하세요!)
        self.ncp_bucket = 'migration-test-2024-aination'
        self.aws_bucket = 'migration-s3-endpoint'  # AWS 버킷 이름 지정 필요
        
    def setup_logging(self):
        """로깅 설정"""
        os.makedirs('logs', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/migration_{timestamp}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def list_objects(self, prefix=''):
        """NCP 버킷의 모든 객체 리스트 조회"""
        objects = []
        paginator = self.ncp_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.ncp_bucket, Prefix=prefix):
            if 'Contents' in page:
                objects.extend(page['Contents'])
        
        return objects

    def migrate_object(self, obj):
        """단일 객체 마이그레이션"""
        try:
            key = obj['Key']
            
            # NCP에서 객체 다운로드
            response = self.ncp_client.get_object(
                Bucket=self.ncp_bucket,
                Key=key
            )
            
            # AWS에 객체 업로드
            self.aws_client.upload_fileobj(
                response['Body'],
                self.aws_bucket,
                key
            )
            
            self.logger.info(f"Successfully migrated: {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error migrating {key}: {str(e)}")
            return False

    def migrate_all(self, max_workers=5):
        """전체 객체 마이그레이션"""
        try:
            # AWS 버킷 존재 확인 또는 생성
            try:
                self.aws_client.head_bucket(Bucket=self.aws_bucket)
            except:
                self.aws_client.create_bucket(
                    Bucket=self.aws_bucket,
                    CreateBucketConfiguration={'LocationConstraint': aws_region}
                )
                self.logger.info(f"Created AWS bucket: {self.aws_bucket}")

            # 마이그레이션할 객체 리스트 조회
            objects = self.list_objects('Migration Test/')
            total_objects = len(objects)
            self.logger.info(f"Found {total_objects} objects to migrate")

            # 병렬 처리로 마이그레이션 수행
            successful = 0
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_key = {executor.submit(self.migrate_object, obj): obj['Key'] 
                               for obj in objects}
                
                # 진행 상황 표시
                with tqdm(total=total_objects, desc="Migrating") as pbar:
                    for future in future_to_key:
                        try:
                            if future.result():
                                successful += 1
                        finally:
                            pbar.update(1)

            # 결과 보고
            self.logger.info(f"\nMigration completed:")
            self.logger.info(f"Total objects: {total_objects}")
            self.logger.info(f"Successfully migrated: {successful}")
            self.logger.info(f"Failed: {total_objects - successful}")

        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")

if __name__ == "__main__":
    migration = StorageMigration()
    migration.migrate_all()
import boto3
from botocore.client import Config
import os
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import time
import concurrent.futures
from itertools import islice

load_dotenv()

# 환경변수에서 키 값들을 가져옴
ncp_access_key = os.getenv('NCP_ACCESS_KEY')
ncp_secret_key = os.getenv('NCP_SECRET_KEY')
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')
# 버킷 이름도 환경변수로 관리
ncp_bucket_name = os.getenv('NCP_BUCKET_NAME')
aws_bucket_name = os.getenv('AWS_BUCKET_NAME')

# NCP 버킷 목록 정의
NCP_BUCKETS = [
    "dentop02"
]

class MigrationHandler:
    def __init__(self, source_bucket, dest_bucket):
        self.setup_clients()
        self.setup_logging()
        self.source_bucket = source_bucket
        self.dest_bucket = dest_bucket
        self.start_time = None
        self.stats = {
            'total': 0,
            'success': 0,
            'skipped': 0,
            'failed': 0,
            'total_bytes': 0,
            'transferred_bytes': 0
        }

    def setup_clients(self):
        """NCP와 AWS 클라이언트 설정"""
        self.ncp_client = boto3.client(
            's3',
            aws_access_key_id=ncp_access_key,
            aws_secret_access_key=ncp_secret_key,
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
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f'logs/migration_{timestamp}.log'
        
        # logs 디렉토리가 없으면 생성
        os.makedirs('logs', exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
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

    def format_size(self, size):
        """바이트 크기를 읽기 쉬운 형식으로 변환"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0

    def format_time(self, seconds):
        """초 단위 시간을 읽기 쉬운 형식으로 변환"""
        return str(timedelta(seconds=int(seconds)))

    def migrate_object(self, obj: dict, retry_count: int = 3) -> bool:
        """단일 객체 마이그레이션 - AWS S3에 없는 경우에만 마이그레이션"""
        object_key = obj['Key']
        
        try:
            # AWS S3에 해당 객체가 있는지 확인
            try:
                self.aws_client.head_object(
                    Bucket=self.dest_bucket,
                    Key=object_key
                )
                # 객체가 이미 존재하면 스킵
                self.logger.info(f"Object already exists in S3, skipping: {object_key}")
                obj['migration_status'] = 'skipped'
                return True
            except:
                # 객체가 없는 경우에만 마이그레이션 진행
                for attempt in range(retry_count):
                    try:
                        # NCP에서 객체 다운로드
                        response = self.ncp_client.get_object(
                            Bucket=self.source_bucket,
                            Key=object_key
                        )
                        
                        # AWS에 업로드 (폴더 구조 유지)
                        self.aws_client.upload_fileobj(
                            response['Body'],
                            self.dest_bucket,
                            object_key  # 원본 경로 그대로 사용하여 폴더 구조 유지
                        )
                        
                        self.logger.info(f"Successfully migrated: {object_key}")
                        return True
                        
                    except Exception as e:
                        self.logger.error(f"Error migrating {object_key}: {str(e)}")
                        if attempt == retry_count - 1:  # 마지막 시도였다면
                            return False
                        
                        self.logger.info(f"Retrying... ({attempt + 1}/{retry_count})")
        
        except Exception as e:
            self.logger.error(f"Unexpected error with {object_key}: {str(e)}")
            return False
        
        return False

    def verify_buckets(self):
        """소스(NCP)와 대상(AWS) 버킷의 존재 여부 확인"""
        try:
            # NCP 버킷 확인
            self.ncp_client.head_bucket(Bucket=self.source_bucket)
            # AWS 버킷 확인
            self.aws_client.head_bucket(Bucket=self.dest_bucket)
            self.logger.info(f"Verified both buckets exist - NCP: {self.source_bucket}, AWS: {self.dest_bucket}")
            return True
        except Exception as e:
            self.logger.error(f"Bucket verification failed: {str(e)}")
            return False

    def compare_objects(self, ncp_obj, aws_obj):
        """두 객체의 메타데이터 비교"""
        if ncp_obj['Size'] != aws_obj['Size']:
            return False
        
        # ETag 비교 (MD5 체크섬)
        ncp_etag = ncp_obj['ETag'].strip('"')
        aws_etag = aws_obj['ETag'].strip('"')
        return ncp_etag == aws_etag

    def get_aws_objects(self, prefix: str = "") -> dict:
        """AWS S3 버킷의 객체 리스��� 조회"""
        try:
            objects = {}
            paginator = self.aws_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.dest_bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects[obj['Key']] = obj
            
            return objects
        except Exception as e:
            self.logger.error(f"Error listing AWS objects: {str(e)}")
            raise

    def analyze_migration_needs(self, prefix: str = ""):
        """마이그레이션 필요성 분석"""
        self.logger.info("Analyzing migration needs...")
        
        # NCP와 AWS의 객체 목록 조회
        ncp_objects = {obj['Key']: obj for obj in self.list_objects(prefix)}
        aws_objects = self.get_aws_objects(prefix)
        
        total_objects = len(ncp_objects)
        existing_objects = 0
        different_objects = 0
        new_objects = 0
        total_size = 0
        
        for key, ncp_obj in ncp_objects.items():
            total_size += ncp_obj['Size']
            
            if key in aws_objects:
                if self.compare_objects(ncp_obj, aws_objects[key]):
                    existing_objects += 1
                else:
                    different_objects += 1
            else:
                new_objects += 1
        
        analysis = {
            'total_objects': total_objects,
            'existing_identical': existing_objects,
            'needs_update': different_objects,
            'new_objects': new_objects,
            'total_size': total_size
        }
        
        self.logger.info(
            f"\nMigration Analysis Results:\n"
            f"Total objects in NCP: {total_objects}\n"
            f"Already identical in AWS: {existing_objects}\n"
            f"Need update (different): {different_objects}\n"
            f"New objects to migrate: {new_objects}\n"
            f"Total size to migrate: {self.format_size(total_size)}\n"
        )
        
        return analysis

    def chunk_list(self, lst, chunk_size):
        """리스트를 청크 단위로 분할"""
        iterator = iter(lst)
        return iter(lambda: list(islice(iterator, chunk_size)), [])

    def migrate_chunk(self, objects):
        """청크 단위 마이그레이션 처리"""
        results = {'success': 0, 'failed': 0, 'skipped': 0, 'transferred_bytes': 0}
        
        for obj in objects:
            try:
                if self.migrate_object(obj):
                    if obj.get('migration_status') == 'skipped':
                        results['skipped'] += 1
                    else:
                        results['success'] += 1
                        results['transferred_bytes'] += obj['Size']
                else:
                    results['failed'] += 1
            except Exception as e:
                self.logger.error(f"Error processing {obj['Key']}: {str(e)}")
                results['failed'] += 1
        
        return results

    def run_migration(self, prefix: str = ""):
        """전체 마이그레이션 실행"""
        self.start_time = time.time()
        objects = self.list_objects(prefix)
        self.stats['total'] = len(objects)
        self.stats['total_bytes'] = sum(obj['Size'] for obj in objects)
        
        self.logger.info(f"Starting migration of {self.stats['total']} objects")
        
        for i, obj in enumerate(objects, 1):
            if self.migrate_object(obj):
                if obj.get('migration_status') == 'skipped':
                    self.stats['skipped'] += 1
                else:
                    self.stats['success'] += 1
                    self.stats['transferred_bytes'] += obj['Size']
            else:
                self.stats['failed'] += 1
            
            # 진행률 계산 및 출력
            progress = (i / self.stats['total']) * 100
            elapsed_time = time.time() - self.start_time
            speed = self.stats['transferred_bytes'] / elapsed_time if elapsed_time > 0 else 0
            
            self.logger.info(
                f"Progress: {progress:.1f}% ({i}/{self.stats['total']}) | "
                f"Speed: {self.format_size(speed)}/s | "
                f"Elapsed: {self.format_time(elapsed_time)}\n"
                f"Success: {self.stats['success']}, "
                f"Skipped: {self.stats['skipped']}, "
                f"Failed: {self.stats['failed']}"
            )
        
        total_time = time.time() - self.start_time
        self.logger.info(
            f"\nMigration completed in {self.format_time(total_time)}\n"
            f"Total objects: {self.stats['total']}\n"
            f"Successfully migrated: {self.stats['success']}\n"
            f"Skipped (already exist): {self.stats['skipped']}\n"
            f"Failed: {self.stats['failed']}\n"
            f"Total size: {self.format_size(self.stats['total_bytes'])}\n"
            f"Transferred size: {self.format_size(self.stats['transferred_bytes'])}\n"
            f"Average speed: {self.format_size(self.stats['transferred_bytes'] / total_time)}/s"
        )

    def print_bucket_structure(self, prefix: str = ""):
        """버킷의 폴더 구조 출력"""
        objects = self.list_objects(prefix)
        
        # 폴더 구조를 저장할 딕셔너리
        structure = {}
        
        for obj in objects:
            path_parts = obj['Key'].split('/')
            current = structure
            
            # 마지막 부분을 제외한 모든 부분을 폴더로 처리
            for part in path_parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            # 마지막 부분을 파일로 처리
            current[path_parts[-1]] = obj['Size']
        
        def print_structure(data, level=0):
            for key, value in sorted(data.items()):
                indent = "  " * level
                if isinstance(value, dict):
                    self.logger.info(f"{indent}📁 {key}/")
                    print_structure(value, level + 1)
                else:
                    self.logger.info(f"{indent}📄 {key} ({self.format_size(value)})")
        
        self.logger.info(f"\nBucket structure for {self.source_bucket}:")
        print_structure(structure)

# 실행 코드
if __name__ == "__main__":
    for bucket in NCP_BUCKETS:
        print(f"\nAnalyzing bucket structure: {bucket}")
        handler = MigrationHandler(
            source_bucket=bucket,
            dest_bucket=bucket
        )
        # 먼저 버킷 구조 출력
        handler.print_bucket_structure()
        
        # 마이그레이션 실행
        handler.run_migration()
        print(f"Completed migration for bucket: {bucket}\n")
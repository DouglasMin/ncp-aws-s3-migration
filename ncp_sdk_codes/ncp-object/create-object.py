import boto3
import random
import string

service_name = 's3'
endpoint_url = 'https://kr.object.ncloudstorage.com'
region_name = 'kr-standard'
access_key = 'ncp_iam_BPAMKR47Y5KhVyITRlr0'
secret_key = 'ncp_iam_BPKMKR4qFrz7AucMedAGT9TbBNGjWCbAUx'

if __name__ == "__main__":
    s3 = boto3.client(
        service_name, 
        endpoint_url=endpoint_url, 
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    bucket_name = 'migration-test-2024-aination'

    try:
        # 1. 먼저 버킷 생성
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created successfully")
        
        # 2. 버킷 목록 확인
        response = s3.list_buckets()
        print("\nAvailable buckets:")
        for bucket in response['Buckets']:
            print(f"- {bucket['Name']}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
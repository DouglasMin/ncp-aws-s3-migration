import boto3
import random
import string

service_name = 's3'
endpoint_url = 'https://kr.object.ncloudstorage.com'
region_name = 'kr-standard'
access_key = 'ncp_iam_BPAMKR47Y5KhVyITRlr0'
secret_key = 'ncp_iam_BPKMKR4qFrz7AucMedAGT9TbBNGjWCbAUx'

def create_random_text(min_length=100, max_length=1000):
    """무작위 텍스트 생성"""
    length = random.randint(min_length, max_length)
    return ''.join(random.choices(string.ascii_letters + string.digits + ' \n', k=length))

def generate_random_name(prefix='folder'):
    """무작위 이름 생성"""
    random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{prefix}_{random_str}"

def create_folder_structure(s3, bucket_name, current_path='Migration Test/', depth=0, max_depth=3):
    """재귀적으로 폴더 구조 생성"""
    if depth >= max_depth:
        return

    # 현재 레벨에서 생성할 폴더와 파일 수 무작위 결정
    num_folders = random.randint(2, 5)
    num_files = random.randint(3, 7)

    # 폴더 생성 및 재귀적으로 하위 구조 생성
    for _ in range(num_folders):
        folder_name = generate_random_name()
        new_path = f"{current_path}{folder_name}/"
        
        # 폴더 생성
        s3.put_object(
            Bucket=bucket_name,
            Key=new_path
        )
        print(f"Created folder: {new_path}")
        
        # 재귀적으로 하위 구조 생성
        create_folder_structure(s3, bucket_name, new_path, depth + 1, max_depth)

    # 파일 생성
    for _ in range(num_files):
        file_name = f"{generate_random_name('file')}.txt"
        file_content = create_random_text()
        
        # 파일 업로드
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{current_path}{file_name}",
            Body=file_content.encode('utf-8')
        )
        print(f"Created file: {current_path}{file_name}")

def list_all_objects(s3, bucket_name, prefix='Migration Test/'):
    """생성된 모든 객체 목록 출력"""
    print("\nListing all objects in bucket:")
    response = s3.list_objects(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"- {obj['Key']} (Size: {obj['Size']} bytes)")

if __name__ == "__main__":
    # NCP Object Storage 클라이언트 생성
    s3 = boto3.client(
        service_name, 
        endpoint_url=endpoint_url, 
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    bucket_name = 'migration-test-2024-aination'

    try:
        # 루트 폴더 생성
        root_folder = 'Migration Test/'
        s3.put_object(
            Bucket=bucket_name,
            Key=root_folder
        )
        print(f"Created root folder: {root_folder}")

        # 폴더 구조 생성 시작
        create_folder_structure(s3, bucket_name)
        
        # 생성된 모든 객체 목록 출력
        list_all_objects(s3, bucket_name)
        
        print("\nTest structure creation completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
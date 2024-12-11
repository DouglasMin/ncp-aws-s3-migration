import boto3
from datetime import datetime
import json
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

service_name = 's3'
endpoint_url = 'https://kr.object.ncloudstorage.com'
region_name = 'kr-standard'
access_key = os.getenv('NCP_ACCESS_KEY')
secret_key = os.getenv('NCP_SECRET_KEY')

def get_structure(s3, bucket_name, prefix=''):
    """재귀적으로 객체 구조를 딕셔너리 형태로 생성"""
    structure = {
        'type': 'folder',
        'name': prefix.rstrip('/').split('/')[-1] if prefix else bucket_name,
        'path': prefix,
        'contents': []
    }

    # 현재 prefix의 모든 객체 조회
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )

    # 하위 폴더 처리
    for common_prefix in response.get('CommonPrefixes', []):
        folder_prefix = common_prefix['Prefix']
        # 재귀적으로 하위 구조 탐색
        sub_structure = get_structure(s3, bucket_name, folder_prefix)
        structure['contents'].append(sub_structure)

    # 파일 처리
    for content in response.get('Contents', []):
        key = content['Key']
        # prefix 자체 건너뛰기
        if key == prefix:
            continue
        # prefix로 시작하는 파일만 처리
        if prefix and not key.startswith(prefix):
            continue
            
        file_name = key.replace(prefix, '').rstrip('/')
        if file_name:  # 빈 문자열이 아닌 경우만 추가
            structure['contents'].append({
                'type': 'file',
                'name': file_name,
                'path': key,
                'size': content['Size'],
                'last_modified': content['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
            })

    return structure

def save_to_log(structure, filename=None):
    """객체 구조를 로그 파일로 저장"""
    if filename is None:
        # logs 디렉토리가 없으면 생성
        os.makedirs('logs', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'logs/storage_structure_{timestamp}.log'

    def write_structure(f, struct, level=0):
        indent = '    ' * level
        if struct['type'] == 'folder':
            f.write(f"{indent}[Folder] {struct['name']}\n")
            for item in struct['contents']:
                write_structure(f, item, level + 1)
        else:
            f.write(f"{indent}[File] {struct['name']} ({struct['size']} bytes) - Last Modified: {struct['last_modified']}\n")

    # 텍스트 형식으로 저장
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"NCP Object Storage Structure Log\n")
        f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Bucket: {structure['name']}\n")
        f.write("-" * 80 + "\n\n")
        write_structure(f, structure)

    # JSON 형식으로도 저장 (나중에 프로그래밍적으로 활용할 수 있도록)
    json_filename = filename.replace('.log', '.json')
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(structure, f, indent=2, ensure_ascii=False)

    return filename, json_filename

if __name__ == "__main__":
    try:
        # NCP Object Storage 클라이언트 생성
        s3 = boto3.client(
            service_name, 
            endpoint_url=endpoint_url, 
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        bucket_name = os.getenv('NCP_BUCKET_NAME')
        root_prefix = ''  # 빈 문자열로 설정하여 최상위 폴더부터 분석

        print("Analyzing storage structure...")
        structure = get_structure(s3, bucket_name, root_prefix)
        
        print("Saving to log files...")
        log_file, json_file = save_to_log(structure)
        
        print(f"\nStructure has been saved to:")
        print(f"- Log file: {log_file}")
        print(f"- JSON file: {json_file}")

    except Exception as e:
        print(f"Error: {str(e)}")
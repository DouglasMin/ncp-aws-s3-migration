## 이 리파지토리는 NCP Object Storage에 있는 객체들을 AWS s3로 Migrate 해주는 코드 입니다.

1. 실행순서
   - git clone 후 프로젝트 폴더로 이동
   - 가상환경 생성 및 활성화
   - requirements.txt로 패키지 설치
   - .env.example을 .env로 복사 후 키 정보 입력
   - object-observe.py로 NCP 버킷 상태 확인
   - object-migrations.py로 마이그레이션 실행

2. 환경 설정   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   cp .env.example .env
   # .env 파일에 NCP, AWS 키 정보 입력   ```

3. 마이그레이션 실행   ```bash
   # NCP 버킷 상태 확인
   python ncp_sdk_codes/object-observe.py
   
   # 마이그레이션 실행
   python ncp_sdk_codes/object-migrations.py   ```

4. 로그 확인
   - logs 폴더에서 마이그레이션 진행 상황 확인 가능
   - 실행 시간별로 로그 파일 생성됨

5. 주의사항
   - 키 정보는 절대 깃허브에 커밋하지 않기
   - 대용량 전송 시 네트워크 비용 발생 가능
   - 마이그레이션 전 데이터 백업 권장
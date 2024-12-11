import boto3
import json
from datetime import datetime
from typing import Dict

class NotificationHandler:
    def __init__(self, aws_access_key: str, aws_secret_key: str, 
                 region: str = 'ap-northeast-2'):
        self.sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
        
        self.sns_client = boto3.client(
            'sns',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
        
    def send_to_sqs(self, queue_url: str, message: Dict):
        """SQS에 메시지 전송"""
        try:
            self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message)
            )
            return True
        except Exception as e:
            print(f"Error sending message to SQS: {str(e)}")
            return False

    def send_to_sns(self, topic_arn: str, subject: str, message: Dict):
        """SNS로 알림 전송"""
        try:
            self.sns_client.publish(
                TopicArn=topic_arn,
                Subject=subject,
                Message=json.dumps(message, indent=2)
            )
            return True
        except Exception as e:
            print(f"Error sending message to SNS: {str(e)}")
            return False

    def send_migration_result(self, queue_url: str, topic_arn: str,
                            status: str, details: Dict):
        """마이그레이션 결과 전송"""
        message = {
            'timestamp': datetime.now().isoformat(),
            'status': status,
            **details
        }
        
        # SQS에 모든 결과 저장
        self.send_to_sqs(queue_url, message)
        
        # 실패한 경우에만 SNS 알림
        if status == 'failed':
            subject = f"Migration Failed: {details.get('object_key', 'Unknown')}"
            self.send_to_sns(topic_arn, subject, message)

    def send_batch_summary(self, topic_arn: str, batch_results: Dict):
        """배치 처리 결과 요약 전송"""
        subject = f"Migration Batch Summary"
        self.send_to_sns(topic_arn, subject, batch_results)
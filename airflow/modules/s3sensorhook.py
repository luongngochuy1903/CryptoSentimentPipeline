from airflow import DAG
from airflow.utils.decorators import apply_defaults
from airflow.sensors.base import BaseSensorOperator
import boto3
from datetime import datetime, timezone

class S3NewFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, aws_access_key, aws_secret_key, endpoint, bucket, prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.boto3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            endpoint_url=endpoint,
        )
    
    def poke(self, context):
        execution_date = context['execution_date'].replace(tzinfo=timezone.utc)
        print(f"From the execution_date: {execution_date}")
        
        response = self.boto3_client.list_objects_v2(
            Bucket = self.bucket,
            Prefix = self.prefix
        )
        for obj in response.get("Contents", []):
            newest = obj["LastModified"]
            if newest > execution_date:
                print(f"Found signal of modifying in bucket {self.bucket}")
                return True
        print("Nothing founded")
        return False
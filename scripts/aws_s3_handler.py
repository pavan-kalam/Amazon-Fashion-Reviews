"""
AWS S3 Handler
Handles uploading and downloading data to/from S3
"""

import boto3
import os
import json
import pandas as pd
from io import StringIO, BytesIO
from typing import Optional, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Handler:
    """Handle S3 operations"""
    
    def __init__(self, bucket_name: Optional[str] = None):
        """
        Initialize S3 client
        
        Args:
            bucket_name: S3 bucket name (can also be set via env var)
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        )
        self.bucket_name = bucket_name or os.getenv('S3_BUCKET_NAME', 'reddit-data-pipeline')
        logger.info(f"S3Handler initialized for bucket: {self.bucket_name}")
    
    def upload_file(self, local_filepath: str, s3_key: str) -> bool:
        """
        Upload a file to S3
        
        Args:
            local_filepath: Path to local file
            s3_key: S3 object key (path in bucket)
        
        Returns:
            True if successful
        """
        try:
            self.s3_client.upload_file(local_filepath, self.bucket_name, s3_key)
            logger.info(f"Uploaded {local_filepath} to s3://{self.bucket_name}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading file to S3: {str(e)}")
            raise
    
    def upload_dataframe(
        self, 
        df: pd.DataFrame, 
        s3_key: str, 
        format: str = 'parquet'
    ) -> bool:
        """
        Upload pandas DataFrame to S3
        
        Args:
            df: DataFrame to upload
            s3_key: S3 object key
            format: File format ('parquet', 'csv', 'json')
        
        Returns:
            True if successful
        """
        try:
            buffer = BytesIO()
            
            if format == 'parquet':
                df.to_parquet(buffer, index=False, engine='pyarrow')
            elif format == 'csv':
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                buffer = BytesIO(buffer.getvalue().encode())
            elif format == 'json':
                buffer = BytesIO()
                df.to_json(buffer, orient='records', lines=True)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            buffer.seek(0)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue()
            )
            
            logger.info(f"Uploaded DataFrame to s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading DataFrame to S3: {str(e)}")
            raise
    
    def upload_json(self, data: dict, s3_key: str) -> bool:
        """
        Upload JSON data to S3
        
        Args:
            data: Dictionary to upload as JSON
            s3_key: S3 object key
        
        Returns:
            True if successful
        """
        try:
            json_str = json.dumps(data, indent=2, default=str)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_str.encode('utf-8'),
                ContentType='application/json'
            )
            logger.info(f"Uploaded JSON to s3://{self.bucket_name}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading JSON to S3: {str(e)}")
            raise
    
    def download_file(self, s3_key: str, local_filepath: str) -> bool:
        """
        Download a file from S3
        
        Args:
            s3_key: S3 object key
            local_filepath: Path to save file locally
        
        Returns:
            True if successful
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_filepath)
            logger.info(f"Downloaded s3://{self.bucket_name}/{s3_key} to {local_filepath}")
            return True
        except Exception as e:
            logger.error(f"Error downloading file from S3: {str(e)}")
            raise
    
    def read_dataframe(self, s3_key: str, format: str = 'parquet') -> pd.DataFrame:
        """
        Read DataFrame from S3
        
        Args:
            s3_key: S3 object key
            format: File format ('parquet', 'csv', 'json')
        
        Returns:
            DataFrame
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            buffer = BytesIO(response['Body'].read())
            
            if format == 'parquet':
                df = pd.read_parquet(buffer)
            elif format == 'csv':
                df = pd.read_csv(buffer)
            elif format == 'json':
                df = pd.read_json(buffer, lines=True)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Read DataFrame from s3://{self.bucket_name}/{s3_key}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading DataFrame from S3: {str(e)}")
            raise
    
    def list_objects(self, prefix: str = '') -> List[str]:
        """
        List objects in S3 bucket with given prefix
        
        Args:
            prefix: Key prefix to filter
        
        Returns:
            List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                keys = [obj['Key'] for obj in response['Contents']]
                logger.info(f"Found {len(keys)} objects with prefix '{prefix}'")
                return keys
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error listing objects from S3: {str(e)}")
            raise
    
    def delete_object(self, s3_key: str) -> bool:
        """
        Delete an object from S3
        
        Args:
            s3_key: S3 object key
        
        Returns:
            True if successful
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Deleted s3://{self.bucket_name}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error deleting object from S3: {str(e)}")
            raise


if __name__ == "__main__":
    # Example usage
    s3_handler = S3Handler()
    
    # Create sample DataFrame
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    
    # Upload DataFrame
    s3_handler.upload_dataframe(df, 'test/data.parquet', format='parquet')
    
    # Read DataFrame
    df_read = s3_handler.read_dataframe('test/data.parquet', format='parquet')
    print(df_read)


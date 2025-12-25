"""
Utility Functions
Common utility functions used across the pipeline
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config(config_path: str = '/opt/airflow/config/config.json') -> Dict[str, Any]:
    """
    Load configuration from JSON file
    
    Args:
        config_path: Path to configuration file
    
    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.warning(f"Config file not found: {config_path}. Using environment variables.")
        return {}


def get_env_var(key: str, default: Optional[str] = None) -> str:
    """
    Get environment variable with optional default
    
    Args:
        key: Environment variable name
        default: Default value if not found
    
    Returns:
        Environment variable value
    """
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Environment variable {key} is required but not set")
    return value


def validate_reddit_credentials() -> bool:
    """
    Validate Reddit API credentials
    
    Returns:
        True if credentials are valid
    """
    required_vars = ['REDDIT_CLIENT_ID', 'REDDIT_CLIENT_SECRET']
    
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Missing required environment variable: {var}")
            return False
    
    return True


def validate_aws_credentials() -> bool:
    """
    Validate AWS credentials
    
    Returns:
        True if credentials are valid
    """
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION']
    
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Missing required environment variable: {var}")
            return False
    
    return True


def validate_redshift_credentials() -> bool:
    """
    Validate Redshift credentials
    
    Returns:
        True if credentials are valid
    """
    required_vars = [
        'REDSHIFT_HOST',
        'REDSHIFT_PORT',
        'REDSHIFT_DATABASE',
        'REDSHIFT_USER',
        'REDSHIFT_PASSWORD'
    ]
    
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Missing required environment variable: {var}")
            return False
    
    return True


def generate_timestamp(format: str = '%Y%m%d_%H%M%S') -> str:
    """
    Generate timestamp string
    
    Args:
        format: Timestamp format string
    
    Returns:
        Formatted timestamp string
    """
    return datetime.now().strftime(format)


def create_s3_path(base_path: str, filename: str, date_partition: bool = True) -> str:
    """
    Create S3 path with optional date partitioning
    
    Args:
        base_path: Base S3 path
        filename: Filename
        date_partition: Whether to include date partition
    
    Returns:
        Full S3 path
    """
    if date_partition:
        date_str = datetime.now().strftime('%Y/%m/%d')
        return f"{base_path}/{date_str}/{filename}"
    else:
        return f"{base_path}/{filename}"


def log_dataframe_info(df: pd.DataFrame, name: str = "DataFrame"):
    """
    Log DataFrame information
    
    Args:
        df: DataFrame to log
        name: Name for logging
    """
    logger.info(f"{name} Info:")
    logger.info(f"  Shape: {df.shape}")
    logger.info(f"  Columns: {list(df.columns)}")
    logger.info(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    if len(df) > 0:
        logger.info(f"  Date range: {df.select_dtypes(include=['datetime64']).min().min()} to {df.select_dtypes(include=['datetime64']).max().max()}")


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if division by zero
    
    Returns:
        Division result or default
    """
    if denominator == 0:
        return default
    return numerator / denominator


def format_bytes(bytes_size: int) -> str:
    """
    Format bytes to human-readable string
    
    Args:
        bytes_size: Size in bytes
    
    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


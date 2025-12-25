"""
JSONL File Processor
Process JSONL files (JSON Lines format) for data pipeline
"""

import json
import pandas as pd
import os
from typing import List, Dict, List, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JSONLProcessor:
    """Process JSONL files for data pipeline"""
    
    def __init__(self):
        """Initialize processor"""
        logger.info("JSONLProcessor initialized")
    
    def read_jsonl(self, filepath: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Read JSONL file and return list of dictionaries
        
        Args:
            filepath: Path to JSONL file
            limit: Maximum number of lines to read (None for all)
        
        Returns:
            List of dictionaries
        """
        data = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if limit and i >= limit:
                        break
                    if line.strip():  # Skip empty lines
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning(f"Skipping invalid JSON on line {i+1}: {e}")
                            continue
            
            logger.info(f"Read {len(data)} records from {filepath}")
            return data
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            raise
        except Exception as e:
            logger.error(f"Error reading JSONL file: {str(e)}")
            raise
    
    def jsonl_to_dataframe(self, filepath: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Convert JSONL file to pandas DataFrame
        
        Args:
            filepath: Path to JSONL file
            limit: Maximum number of lines to read
        
        Returns:
            DataFrame
        """
        data = self.read_jsonl(filepath, limit)
        df = pd.DataFrame(data)
        logger.info(f"Converted to DataFrame with shape: {df.shape}")
        return df
    
    def process_jsonl_file(
        self,
        filepath: str,
        limit: Optional[int] = None,
        output_format: str = 'dataframe'
    ):
        """
        Process JSONL file and return in specified format
        
        Args:
            filepath: Path to JSONL file
            limit: Maximum number of lines to read
            output_format: 'dataframe' or 'list'
        
        Returns:
            DataFrame or list of dictionaries
        """
        if output_format == 'dataframe':
            return self.jsonl_to_dataframe(filepath, limit)
        else:
            return self.read_jsonl(filepath, limit)
    
    def get_file_info(self, filepath: str) -> Dict:
        """
        Get information about JSONL file
        
        Args:
            filepath: Path to JSONL file
        
        Returns:
            Dictionary with file information
        """
        info = {
            'filepath': filepath,
            'exists': os.path.exists(filepath),
            'size_mb': 0,
            'line_count': 0,
            'sample_keys': []
        }
        
        if not info['exists']:
            return info
        
        # Get file size
        info['size_mb'] = round(os.path.getsize(filepath) / (1024 * 1024), 2)
        
        # Count lines and get sample
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                if first_line.strip():
                    sample = json.loads(first_line)
                    info['sample_keys'] = list(sample.keys())
                
                # Count total lines
                f.seek(0)
                info['line_count'] = sum(1 for line in f if line.strip())
                
        except Exception as e:
            logger.warning(f"Could not get file info: {e}")
        
        return info
    
    def preview_jsonl(self, filepath: str, num_lines: int = 5) -> List[Dict]:
        """
        Preview first N lines of JSONL file
        
        Args:
            filepath: Path to JSONL file
            num_lines: Number of lines to preview
        
        Returns:
            List of dictionaries
        """
        return self.read_jsonl(filepath, limit=num_lines)


if __name__ == "__main__":
    # Example usage
    processor = JSONLProcessor()
    
    # Check if file exists
    filepath = '../meta_Amazon_Fashion.jsonl'
    if os.path.exists(filepath):
        # Get file info
        info = processor.get_file_info(filepath)
        print(f"File Info: {info}")
        
        # Preview first few lines
        print("\nPreview (first 3 lines):")
        preview = processor.preview_jsonl(filepath, num_lines=3)
        for i, record in enumerate(preview, 1):
            print(f"\nRecord {i}:")
            print(json.dumps(record, indent=2)[:500])  # Print first 500 chars
        
        # Convert to DataFrame
        print("\n\nConverting to DataFrame (first 100 lines)...")
        df = processor.jsonl_to_dataframe(filepath, limit=100)
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print("\nFirst few rows:")
        print(df.head())
    else:
        print(f"File not found: {filepath}")


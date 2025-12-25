"""
Streaming Upload Script
Uploads JSONL data in batches (e.g., 1000 rows every 20 seconds)
"""

import os
import sys
import json
import time
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Add scripts to path
sys.path.append(os.path.dirname(__file__))

from jsonl_processor import JSONLProcessor
from aws_s3_handler import S3Handler


def upload_in_batches(filepath, batch_size=1000, delay_seconds=20, s3_prefix='raw/data/'):
    """
    Upload JSONL file in batches with delay between uploads
    
    Args:
        filepath: Path to JSONL file
        batch_size: Number of records per batch
        delay_seconds: Seconds to wait between batches
        s3_prefix: S3 prefix for uploaded files
    """
    print("="*60)
    print("Streaming Upload to S3 (Batched with Delay)")
    print("="*60)
    
    if not os.path.exists(filepath):
        print(f"✗ Error: File not found: {filepath}")
        return
    
    processor = JSONLProcessor()
    s3_handler = S3Handler()
    
    # Get file info
    info = processor.get_file_info(filepath)
    total_lines = info['line_count']
    file_size_mb = info['size_mb']
    
    print(f"\nFile: {os.path.basename(filepath)}")
    print(f"Size: {file_size_mb} MB")
    print(f"Total lines: {total_lines:,}")
    print(f"Batch size: {batch_size:,} records")
    print(f"Delay between batches: {delay_seconds} seconds")
    
    num_batches = (total_lines // batch_size) + (1 if total_lines % batch_size > 0 else 0)
    estimated_time = (num_batches - 1) * delay_seconds / 60  # Approximate time in minutes
    
    print(f"Will create: {num_batches} batches")
    print(f"Estimated time: ~{estimated_time:.1f} minutes")
    
    confirm = input(f"\nProceed with streaming upload? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Upload cancelled.")
        return
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.basename(filepath).replace('.jsonl', '')
    
    batch_num = 0
    total_uploaded = 0
    start_time = time.time()
    
    try:
        # Open file and process line by line
        with open(filepath, 'r', encoding='utf-8') as f:
            batch_data = []
            line_num = 0
            
            for line in f:
                if line.strip():
                    try:
                        batch_data.append(json.loads(line))
                        line_num += 1
                    except json.JSONDecodeError:
                        continue
                
                # Process batch when size reached
                if len(batch_data) >= batch_size:
                    batch_num += 1
                    elapsed_time = time.time() - start_time
                    
                    print(f"\n{'='*60}")
                    print(f"Batch {batch_num}/{num_batches}")
                    print(f"{'='*60}")
                    print(f"Processing {len(batch_data)} records...")
                    print(f"Total processed: {line_num:,}/{total_lines:,} lines ({line_num/total_lines*100:.1f}%)")
                    print(f"Elapsed time: {elapsed_time/60:.1f} minutes")
                    
                    # Convert to DataFrame
                    import pandas as pd
                    df = pd.DataFrame(batch_data)
                    
                    # Upload batch
                    s3_key = f'{s3_prefix}{filename}_batch{batch_num:03d}_{timestamp}.parquet'
                    print(f"Uploading to: s3://{s3_handler.bucket_name}/{s3_key}")
                    
                    upload_start = time.time()
                    s3_handler.upload_dataframe(df, s3_key, format='parquet')
                    upload_time = time.time() - upload_start
                    
                    total_uploaded += len(df)
                    print(f"✓ Uploaded batch {batch_num} ({len(df)} records) in {upload_time:.1f} seconds")
                    print(f"  S3 Location: s3://{s3_handler.bucket_name}/{s3_key}")
                    
                    # Clear batch data
                    batch_data = []
                    
                    # Wait before next batch (except for last batch)
                    if line_num < total_lines:
                        print(f"\nWaiting {delay_seconds} seconds before next batch...")
                        for i in range(delay_seconds, 0, -1):
                            print(f"  Next batch in {i} seconds...", end='\r')
                            time.sleep(1)
                        print()  # New line after countdown
            
            # Upload remaining data (final batch)
            if batch_data:
                batch_num += 1
                elapsed_time = time.time() - start_time
                
                print(f"\n{'='*60}")
                print(f"Final Batch {batch_num}/{num_batches}")
                print(f"{'='*60}")
                print(f"Processing {len(batch_data)} records...")
                
                import pandas as pd
                df = pd.DataFrame(batch_data)
                
                s3_key = f'{s3_prefix}{filename}_batch{batch_num:03d}_{timestamp}.parquet'
                print(f"Uploading to: s3://{s3_handler.bucket_name}/{s3_key}")
                
                s3_handler.upload_dataframe(df, s3_key, format='parquet')
                total_uploaded += len(df)
                print(f"✓ Uploaded final batch ({len(df)} records)")
        
        total_time = time.time() - start_time
        
        print("\n" + "="*60)
        print("Streaming Upload Complete!")
        print("="*60)
        print(f"Total batches uploaded: {batch_num}")
        print(f"Total records uploaded: {total_uploaded:,}")
        print(f"Total time: {total_time/60:.1f} minutes")
        print(f"Average time per batch: {total_time/batch_num:.1f} seconds")
        print(f"\nS3 Location: s3://{s3_handler.bucket_name}/{s3_prefix}")
        print(f"File prefix: {filename}_batch*_{timestamp}.parquet")
        print(f"\nAll batches are now in S3!")
        
    except KeyboardInterrupt:
        print(f"\n\n⚠ Upload interrupted by user")
        print(f"Uploaded {batch_num} batches so far ({total_uploaded:,} records)")
        print(f"Remaining data was not uploaded")
        
    except Exception as e:
        print(f"\n✗ Error during upload: {e}")
        import traceback
        traceback.print_exc()
        print(f"\nUploaded {batch_num} batches before error ({total_uploaded:,} records)")


if __name__ == "__main__":
    # Get parameters from command line or use defaults
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        filepath = '../meta_Amazon_Fashion.jsonl'
    
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    delay_seconds = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    
    print(f"\nConfiguration:")
    print(f"  File: {filepath}")
    print(f"  Batch size: {batch_size} records")
    print(f"  Delay: {delay_seconds} seconds between batches")
    print()
    
    upload_in_batches(filepath, batch_size, delay_seconds)


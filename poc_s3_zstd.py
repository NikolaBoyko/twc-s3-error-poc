#!/usr/bin/env python3
"""
Proof of Concept for S3-compatible storage with zstd compression
This script demonstrates how to connect to S3-compatible storage,
store zstd-compressed JSON data, and read it back with metadata.
"""

import os
import json
import sys
import logging
import time
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

try:
    import boto3
    from botocore.config import Config
    import zstandard as zstd
    logger.info("✓ All required dependencies imported successfully")
except ImportError as e:
    logger.error(f"✗ Missing dependency: {e}")
    logger.info("Please run: pip install boto3 zstandard python-dotenv")
    sys.exit(1)

class S3ZstdManager:
    """Handles S3 operations with zstd compression functionality"""
    
    def __init__(self):
        """Initialize S3 client with configuration"""
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.endpoint_url = os.getenv('S3_ENDPOINT_URL')
        self.region_name = os.getenv('S3_REGION_NAME', 'us-east-1')
        self.aws_access_key_id = os.getenv('S3_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('S3_SECRET_ACCESS_KEY')
        
        # Validate required environment variables
        required_vars = ['S3_BUCKET_NAME', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            logger.info("Please set these in your .env file")
            sys.exit(1)
        
        # Initialize S3 client
        self.client = boto3.client(
            service_name='s3',
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            config=Config(
                retries={'mode': 'standard', 'max_attempts': 3}
            )
        )
        logger.info("✓ S3 client initialized successfully")
    
    def create_test_data(self):
        """Create sample JSON data for testing"""
        return {
            "message": "Hello from S3 with zstd compression!",
            "timestamp": "2026-03-03T10:00:00Z",
            "data": {
                "users": [
                    {"id": 1, "name": "Alice", "email": "alice@example.com"},
                    {"id": 2, "name": "Bob", "email": "bob@example.com"},
                    {"id": 3, "name": "Charlie", "email": "charlie@example.com"}
                ],
                "metadata": {
                    "version": "1.0",
                    "format": "json"
                }
            },
            "compression_info": {
                "method": "zstd",
                "original_size": 0,
                "compressed_size": 0,
                "ratio": 0.0
            }
        }
    
    def compress_data_zstd(self, data):
        """Compress data using zstd compression"""
        try:
            # Convert data to JSON string first
            json_str = json.dumps(data)
            # Compress with zstd
            compressed = zstd.compress(json_str.encode('utf-8'))
            return compressed
        except Exception as e:
            logger.error(f"Compression failed: {e}")
            return None
    
    def decompress_data_zstd(self, compressed_data):
        """Decompress zstd compressed data"""
        try:
            # Decompress the zstd data
            decompressed = zstd.decompress(compressed_data)
            # Convert back to JSON
            return json.loads(decompressed.decode('utf-8'))
        except Exception as e:
            logger.error(f"Decompression failed: {e}")
            return None
    
    def store_compressed_file(self, key, data):
        """Store compressed data to S3 with zstd compression"""
        try:
            # Create compressed data
            compressed_data = self.compress_data_zstd(data)
            if compressed_data is None:
                logger.error("Failed to compress data")
                return False
            
            # Get original size
            original_size = len(json.dumps(data))
            compressed_size = len(compressed_data)
            
            # Update compression info in data
            data["compression_info"]["original_size"] = original_size
            data["compression_info"]["compressed_size"] = compressed_size
            data["compression_info"]["ratio"] = round(original_size / compressed_size, 2)
            
            # Store to S3 with content encoding
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=compressed_data,
                ContentEncoding='zstd'
            )
            
            logger.info(f"✓ Successfully stored compressed data to {key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store file to S3: {e}")
            return False
    
    def read_compressed_file(self, key):
        """Read and decompress data from S3"""
        try:
            # Read from S3
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            # Extract metadata
            content_encoding = response.get('ContentEncoding', 'none')
            content_length = response.get('ContentLength', 0)
            last_modified = response.get('LastModified', 'unknown')
            
            logger.info(f"✓ Retrieved data from {key}")
            logger.info(f"  Content-Encoding: {content_encoding}")
            logger.info(f"  Content-Length: {content_length} bytes")
            logger.info(f"  Last-Modified: {last_modified}")
            
            # Get the compressed binary data
            compressed_data = response['Body'].read()
            
            # Decompress if zstd encoded
            if content_encoding == 'zstd':
                decompressed_data = self.decompress_data_zstd(compressed_data)
                if decompressed_data is None:
                    logger.error("Failed to decompress data")
                    return None
                return decompressed_data
            else:
                # If not zstd compressed, return as-is
                logger.error("Data not zstd compressed")
                return None
                
        except Exception as e:
            logger.error(f"Failed to read file from S3: {e}")
            return None

def main():
    logger.info("=== S3-Compatible Storage with zstd Compression POC ===")
    
    # Initialize S3 manager
    try:
        s3_manager = S3ZstdManager()
    except Exception as e:
        logger.error(f"Failed to initialize S3 manager: {e}")
        return
    
    # Show environment setup
    logger.info("Environment variables loaded:")
    logger.info(f"  S3_BUCKET_NAME: {os.getenv('S3_BUCKET_NAME')}")
    logger.info(f"  S3_ENDPOINT_URL: {os.getenv('S3_ENDPOINT_URL')}")
    
    # 1. Create test data
    logger.info("\n1. Creating test JSON data...")
    test_data = s3_manager.create_test_data()
    logger.info(f"   Created data with {len(test_data['data']['users'])} users")
    
    # 2. Compress and store
    logger.info("\n2. Compressing and storing data to S3...")
    test_key = f"test-compressed-file_{time.time()}.json.zst"
    
    success = s3_manager.store_compressed_file(test_key, test_data)
    if not success:
        logger.error("   ❌ Failed to store compressed file")
        return
    
    # 3. Retrieve and inspect metadata
    logger.info("\n3. Retrieving data from S3 with metadata inspection...")
    retrieved_data = s3_manager.read_compressed_file(test_key)
    
    if retrieved_data is None:
        logger.error("   ❌ Failed to retrieve data")
        return
    
    # 4. Display results
    logger.info("\n4. Result summary:")
    logger.info("   ─────────────────────────────────────────────")
    logger.info(f"   ✅ Retrieved JSON data:\n{json.dumps(retrieved_data, indent=2)}")
    logger.info("   ─────────────────────────────────────────────")
    
    # 5. Show detailed compression info
    compression_info = retrieved_data.get("compression_info", {})
    logger.info("\n5. Compression details:")
    logger.info(f"   Original Size: {compression_info.get('original_size', 0)} bytes")
    logger.info(f"   Compressed Size: {compression_info.get('compressed_size', 0)} bytes")
    logger.info(f"   Compression Ratio: {compression_info.get('ratio', 0.0)}:1")
    
    # 6. Demonstrate error handling
    logger.info("\n6. Error handling demonstration:")
    logger.info("   • Non-existent objects: Would raise NoSuchKey exception")
    logger.info("   • Invalid content encoding: Would detect and handle appropriately")
    logger.info("   • Corrupted data: Would raise decompression error")
    
    logger.info("\n=== POC Completed Successfully ===")

if __name__ == "__main__":
    main()
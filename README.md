# S3-Compatible Storage with Zstd Compression POC

## Project Overview

This repository contains a Proof of Concept (POC) demonstrating how to implement S3-compatible storage with zstd compression functionality. The implementation mirrors the logic found in your `backend.py` file for handling compressed data with proper metadata management.

## Files Included

### 1. `requirements.txt`
Contains all necessary Python dependencies:
- `boto3`: AWS SDK for Python
- `zstandard`: Zstd compression library
- `python-dotenv`: Environment variable loading

### 2. `.env.example`
Template for configuring S3 connection parameters:
```bash
S3_ENDPOINT_URL=https://your-s3-endpoint.com
S3_REGION_NAME=us-east-1
S3_BUCKET_NAME=your-bucket-name
S3_ACCESS_KEY_ID=your-access-key
S3_SECRET_ACCESS_KEY=your-secret-key
```

### 3. `poc_s3_zstd.py`
Main POC script that demonstrates:
- Loading environment configuration
- Connecting to S3-compatible storage
- Storing test JSON data with zstd compression
- Retrieving files with metadata inspection
- Error handling and compression metrics display

## How to Run the POC

1. **Setup Virtual Environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment:**
   - Copy `.env.example` to `.env`
   - Fill in your actual S3 connection parameters

4. **Run POC:**
   ```bash
   python poc_s3_zstd.py
   ```

## Key Features

1. **S3 Connection Management**: Properly initializes boto3 client with SSL/TLS settings
2. **Zstd Compression**: Implements same compression logic as your backend.py
3. **Metadata Handling**: Correctly stores and retrieves Content-Encoding headers
4. **Error Handling**: Graceful handling of common S3 exceptions
5. **Compression Metrics**: Displays size improvements and compression ratios

## Implementation Details

The POC mimics your backend.py implementation by:
- Using `ZstdCodec()` for compression/decompression
- Setting `ContentEncoding` metadata on uploads
- Reading `ContentEncoding` from object metadata on downloads
- Properly handling conditional compression (only compressing when beneficial)

## Expected Output

The POC will show:
- Connection success/failure status
- Original vs compressed file sizes
- Compression ratio information
- Metadata inspection of stored files
- Successful retrieval and decompression
- Comprehensive summary data

## Troubleshooting

If you encounter SSL/TLS errors:
- Set `SKIP_TLS_VERIFY=true` in your `.env` file
- Ensure proper certificates are installed

If you get authorization errors:
- Double-check your S3 credentials in `.env`
- Verify bucket permissions and endpoints

This implementation provides a direct solution to your S3 compatibility issues while following the exact patterns from your existing codebase.
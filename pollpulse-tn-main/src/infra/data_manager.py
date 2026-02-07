"""
Data management layer for Supabase Storage and Job Queue.

Implements the DataSystem class that handles:
- Uploading raw JSON data to Supabase Storage
- Creating job queue entries for downstream processing
"""

import json
from typing import Dict, Optional
from datetime import datetime

from .client import get_supabase_client


class DataSystem:
    """
    Manages data operations for the ETL pipeline.
    
    This class provides a unified interface for:
    1. Uploading raw JSON data to Supabase Storage (Data Lake)
    2. Creating job queue entries for asynchronous processing
    
    The decoupled architecture allows the Producer (scraper.py) to push
    data without waiting for the Consumer (processor.py) to process it.
    """
    
    def __init__(self, bucket_name: str = 'raw_data'):
        """
        Initialize the DataSystem.
        
        Args:
            bucket_name: Name of the Supabase Storage bucket to use
        """
        self.client = get_supabase_client()
        self.bucket_name = bucket_name
        
        if self.client is None:
            raise RuntimeError(
                "Supabase client not available. "
                "Ensure SUPABASE_URL and SUPABASE_KEY are set in .env"
            )
        
        # Verify bucket exists (non-blocking check)
        try:
            storage_client = self.client.storage.from_(self.bucket_name)
            # Try to list bucket (will fail if bucket doesn't exist)
            storage_client.list(limit=1)
            print(f"Storage bucket '{bucket_name}' verified")
        except Exception as e:
            error_msg = str(e).lower()
            if 'bucket' in error_msg and ('not found' in error_msg or 'does not exist' in error_msg):
                print(f"WARNING: Storage bucket '{bucket_name}' does not exist!")
                print(f"  Create it in Supabase Dashboard: Storage → New bucket → '{bucket_name}'")
                print(f"  Or it will be created automatically on first upload")
            # Don't fail initialization - let upload attempt handle it
    
    def save_raw_json(
        self,
        data: Dict,
        filename: str,
        video_metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Save raw JSON data to Supabase Storage and create a job queue entry.
        
        This method implements the Producer pattern:
        1. Uploads JSON data to the Storage bucket (Data Lake)
        2. Creates a PENDING job in the job_queue table
        3. Returns the job ID for tracking
        
        Args:
            data: Dictionary containing the data to save (will be JSON serialized)
            filename: Filename to use in the storage bucket
            video_metadata: Optional metadata about the video (stored in job_queue.metadata)
        
        Returns:
            Job ID (UUID string) if successful, None otherwise
        """
        try:
            # Serialize data to JSON bytes
            json_content = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
            print(f"  Preparing to upload {len(json_content)} bytes to storage...")
            
            # Upload to Supabase Storage
            file_path = f"{filename}"
            
            storage_client = self.client.storage.from_(self.bucket_name)
            
            # Step 1: Upload to Storage (Data Lake)
            try:
                print(f"  Uploading to storage bucket '{self.bucket_name}' at path '{file_path}'...")
                # Supabase storage upload: path, file_bytes, file_options (optional)
                storage_client.upload(file_path, json_content, file_options={"content-type": "application/json", "upsert": "true"})
                print(f"  [OK] Storage upload successful")
            except Exception as storage_err:
                storage_msg = str(storage_err)
                error_lower = storage_msg.lower()
                
                # Try without file_options if that fails
                if 'file_options' in error_lower or 'unexpected keyword' in error_lower:
                    print(f"  Warning: file_options not supported, trying basic upload...")
                    try:
                        storage_client.upload(file_path, json_content)
                        print(f"  [OK] Storage upload successful (basic upload)")
                    except Exception as basic_err:
                        print(f"  [FAIL] Storage upload failed: {str(basic_err)[:200]}")
                        return None
                elif 'trailing slash' in error_lower:
                    # Try again without trailing slash issue
                    print(f"  Warning: Trailing slash issue, retrying...")
                    try:
                        storage_client.upload(file_path, json_content)
                        print(f"  [OK] Storage upload successful (retry)")
                    except Exception as retry_err:
                        print(f"  [FAIL] Storage upload failed (retry): {str(retry_err)[:200]}")
                        return None
                elif 'bucket' in error_lower and ('not found' in error_lower or 'does not exist' in error_lower):
                    print(f"  [FAIL] Storage Error: Bucket '{self.bucket_name}' does not exist!")
                    print(f"  Create the bucket in Supabase Dashboard: Storage → New bucket → '{self.bucket_name}'")
                    return None
                elif 'row-level security' in error_lower or 'violates row-level security policy' in error_lower:
                    print(f"  [FAIL] Storage RLS Error: Bucket '{self.bucket_name}' needs storage policies")
                    print("  Run this SQL in Supabase SQL Editor:")
                    print(f"""
CREATE POLICY "Allow public uploads to {self.bucket_name}"
ON storage.objects FOR INSERT
TO anon, authenticated
WITH CHECK (bucket_id = '{self.bucket_name}');

CREATE POLICY "Allow public reads from {self.bucket_name}"
ON storage.objects FOR SELECT
TO anon, authenticated
USING (bucket_id = '{self.bucket_name}');
""")
                    return None
                else:
                    print(f"  [FAIL] Storage upload failed: {storage_msg[:200]}")
                    return None
            
            # Step 2: Create job queue entry
            metadata = video_metadata or {}
            metadata.update({
                'filename': filename,
                'file_path': file_path,
                'uploaded_at': datetime.now().isoformat()
            })
            
            try:
                print(f"  Creating job queue entry...")
                result = self.client.table('job_queue').insert({
                    'status': 'PENDING',
                    'file_path': file_path,
                    'metadata': metadata
                }).execute()
                
                if result.data and len(result.data) > 0:
                    job_id = result.data[0]['id']
                    print(f"  [OK] Job queue entry created: {job_id}")
                    print(f"  [OK] Complete: Data saved to {file_path}, job {job_id} queued")
                    return job_id
                else:
                    print(f"  [WARN] Warning: Job insert returned no data")
                    print(f"  File uploaded to storage but job queue entry failed!")
                    print(f"  Check if job_queue table exists and has correct schema")
                    return None
            except Exception as db_err:
                error_msg = str(db_err)
                error_lower = error_msg.lower()
                
                print(f"  [FAIL] Job queue creation failed!")
                
                if 'relation' in error_lower and 'does not exist' in error_lower:
                    print(f"  ERROR: job_queue table does not exist!")
                    print(f"  Run schema.sql in Supabase SQL Editor to create the table")
                elif 'row-level security' in error_lower or 'violates row-level security policy' in error_lower:
                    print(f"  ERROR: Row Level Security blocking job_queue insert")
                    print(f"  Run: ALTER TABLE job_queue DISABLE ROW LEVEL SECURITY;")
                else:
                    print(f"  ERROR: {error_msg[:200]}")
                
                print(f"  WARNING: File was uploaded to storage but job was NOT queued!")
                print(f"  File path: {file_path}")
                return None
                
        except Exception as e:
            print(f"Error saving raw JSON: {str(e)}")
            return None
    
    def get_file_from_storage(self, file_path: str) -> Optional[Dict]:
        """
        Download and parse a JSON file from Supabase Storage.
        
        Args:
            file_path: Path to the file in the storage bucket
        
        Returns:
            Parsed JSON data as dictionary, None if error
        """
        try:
            response = self.client.storage.from_(self.bucket_name).download(file_path)
            data = json.loads(response.decode('utf-8'))
            return data
        except Exception as e:
            print(f"Error downloading file {file_path}: {str(e)}")
            return None
    
    def update_job_status(self, job_id: str, status: str):
        """
        Update the status of a job in the job_queue table.
        
        Args:
            job_id: UUID of the job to update
            status: New status (PENDING, PROCESSING, DONE, FAILED)
        """
        try:
            self.client.table('job_queue').update({
                'status': status
            }).eq('id', job_id).execute()
        except Exception as e:
            print(f"Error updating job status: {str(e)}")
    
    def verify_setup(self) -> bool:
        """
        Verify that storage bucket and job_queue table are accessible.
        
        Returns:
            True if setup is correct, False otherwise
        """
        issues = []
        
        # Check storage bucket
        try:
            storage_client = self.client.storage.from_(self.bucket_name)
            storage_client.list(limit=1)
            print(f"[OK] Storage bucket '{self.bucket_name}' is accessible")
        except Exception as e:
            error_msg = str(e).lower()
            if 'bucket' in error_msg and ('not found' in error_msg or 'does not exist' in error_msg):
                issues.append(f"Storage bucket '{self.bucket_name}' does not exist")
            else:
                issues.append(f"Storage bucket access error: {str(e)[:100]}")
        
        # Check job_queue table
        try:
            result = self.client.table('job_queue').select('id', count='exact').limit(1).execute()
            print(f"[OK] job_queue table is accessible")
        except Exception as e:
            error_msg = str(e).lower()
            if 'relation' in error_msg and 'does not exist' in error_msg:
                issues.append("job_queue table does not exist - run schema.sql")
            elif 'row-level security' in error_msg:
                issues.append("job_queue table has RLS enabled - disable it or add policies")
            else:
                issues.append(f"job_queue table access error: {str(e)[:100]}")
        
        if issues:
            print(f"\n[WARN] Setup Issues Found:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        
        print(f"[OK] All systems verified")
        return True


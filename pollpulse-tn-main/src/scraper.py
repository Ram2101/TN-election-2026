"""
YouTube Comment Scraper - Producer Component.

This module implements the Producer pattern in the decoupled ETL architecture.
It scrapes comments using yt-dlp and immediately pushes the data to Supabase
via the DataSystem class.

The Producer does NOT wait for processing - it simply:
1. Scrapes comments from YouTube
2. Uploads raw JSON to Supabase Storage (Data Lake)
3. Creates a PENDING job in job_queue table
4. Moves to the next video

The Consumer (processor.py) will independently poll the queue and process jobs.
"""

import json
import os
import time
from typing import List, Dict, Optional
from datetime import datetime
import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound, VideoUnavailable
from dotenv import load_dotenv

from infra.data_manager import DataSystem
from discover import discover_videos
from utils.classifier import classify_alliance, should_process_content

# Load environment variables
load_dotenv()

def get_ytdlp_opts_with_cookies(base_opts: dict) -> dict:
    """
    Add cookie support to yt-dlp options if cookies file exists.
    
    Checks for cookies in this order:
    1. YOUTUBE_COOKIES_PATH environment variable
    2. cookies.txt in project root
    3. cookies.txt in current directory
    
    Args:
        base_opts: Base yt-dlp options dictionary
    
    Returns:
        Updated options dictionary with cookies if available
    """
    cookies_paths = [
        os.getenv('YOUTUBE_COOKIES_PATH'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cookies.txt'),
        'cookies.txt',
    ]
    
    for cookies_path in cookies_paths:
        if cookies_path and os.path.exists(cookies_path):
            print(f"  Using cookies from: {cookies_path}")
            base_opts['cookiefile'] = cookies_path
            return base_opts
    
    # No cookies found - warn but continue (only in non-CI environments)
    if not os.getenv('CI'):
        print("  WARNING: No cookies file found. YouTube may block requests.")
        print("  To fix: Export cookies from your browser and save as 'cookies.txt'")
        print("  See: https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp")
    return base_opts

# Load environment variables
load_dotenv()

# Get max comments per video from environment variable
MAX_COMMENTS_PER_VIDEO = int(os.getenv('MAX_COMMENTS_PER_VIDEO', '50'))


def get_transcript_text(video_id: str) -> Optional[str]:
    """
    Fetch video transcript/subtitles with language fallback.
    
    Prioritizes Tamil ('ta') manual subtitles, falls back to English ('en').
    Returns None gracefully if no transcript is available.
    
    Args:
        video_id: YouTube video ID
    
    Returns:
        Concatenated transcript text as a single string, or None if unavailable
    """
    try:
        # Get list of available transcripts
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        
        # Priority order: Tamil manual > English manual > Tamil auto > English auto
        language_priority = [
            ('ta', True),   # Tamil manual
            ('en', True),   # English manual
            ('ta', False),  # Tamil auto-generated
            ('en', False)   # English auto-generated
        ]
        
        for lang_code, prefer_manual in language_priority:
            try:
                if prefer_manual:
                    # Try manual transcript first
                    transcript = transcript_list.find_manually_created_transcript([lang_code])
                else:
                    # Try auto-generated transcript
                    transcript = transcript_list.find_generated_transcript([lang_code])
                
                # Fetch and concatenate transcript
                transcript_data = transcript.fetch()
                transcript_text = ' '.join([entry['text'] for entry in transcript_data])
                return transcript_text
                
            except (NoTranscriptFound, TranscriptsDisabled):
                # Try next language/type
                continue
        
        # No transcript found in any language/type
        return None
    
    except (VideoUnavailable, TranscriptsDisabled, NoTranscriptFound):
        # Video unavailable or transcripts disabled - return None gracefully
        return None
    except Exception:
        # Any other error - return None gracefully (don't crash)
        return None


def scrape_comments_from_video(video_url: str, max_comments: int = 50) -> tuple:
    """
    Extract comments and description from a YouTube video using yt-dlp.
    
    Args:
        video_url: YouTube video URL
        max_comments: Maximum number of comments to extract
    
    Returns:
        Tuple of (list of comment dictionaries, video description)
    """
    comments = []
    description = ""
    
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'getcomments': True,
        }
        
        # Add cookies if available
        ydl_opts = get_ytdlp_opts_with_cookies(ydl_opts)
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            
            # Extract description
            description = info.get('description', '') or ''
            
            # Extract comments from info dict
            comment_data = info.get('comments', [])
            
            for comment in comment_data[:max_comments]:
                comments.append({
                    'text': comment.get('text', ''),
                    'author': comment.get('author', 'Unknown'),
                    'likes': comment.get('like_count', 0),
                    'timestamp': comment.get('timestamp', ''),
                    'time_text': comment.get('time_text', '')
                })
    
    except Exception as e:
        print(f"Error extracting comments: {str(e)[:150]}")
    
    return comments, description


def scrape_comments(video_list: Optional[List[Dict]] = None):
    """
    Main scraping function - Producer component.
    
    Scrapes comments for each video and immediately pushes data to Supabase
    via DataSystem. Does not wait for downstream processing.
    
    Args:
        video_list: Optional list of video dictionaries. If None, will call
                   discover_videos() to get videos programmatically.
    """
    # Get video list - either from parameter or discover programmatically
    if video_list is None:
        print("No video list provided, running discovery...")
        video_list = discover_videos()
    
    if not video_list:
        print("No videos found to process.")
        return
    
    # Initialize DataSystem for Supabase operations
    try:
        data_system = DataSystem(bucket_name='raw_data')
        print("\nVerifying Supabase setup...")
        if not data_system.verify_setup():
            print("\n[WARN] WARNING: Setup verification failed. Jobs may not be created.")
            print("Fix the issues above before continuing.\n")
    except RuntimeError as e:
        print(f"Error initializing DataSystem: {e}")
        print("Cannot proceed without Supabase connection.")
        return
    
    print("=" * 60)
    print("Starting Comment Scraper (Producer)")
    print(f"Total videos to process: {len(video_list)}")
    print("=" * 60)
    print()
    
    jobs_created = 0
    videos_skipped = 0
    
    for i, video in enumerate(video_list, 1):
        video_url = video.get('url', '')
        video_id = video.get('id', '')
        video_title = video.get('title', 'Unknown')
        
        if not video_url:
            print(f"[{i}/{len(video_list)}] Skipping: No URL found")
            continue
        
        print(f"[{i}/{len(video_list)}] Processing: {video_title[:50]}...")
        print(f"  Video ID: {video_id}")
        
        try:
            # Fetch transcript (many videos don't have transcripts - this is expected)
            transcript_text = get_transcript_text(video_id)
            if transcript_text:
                print(f"  Transcript found ({len(transcript_text)} characters)")
            else:
                print(f"  No transcript available (skipping)")
            
            # Scrape comments and description
            comments, description = scrape_comments_from_video(video_url, MAX_COMMENTS_PER_VIDEO)
            
            if not comments:
                print(f"    No comments extracted - skipping job creation")
                videos_skipped += 1
                continue
            
            # Structure the data with Weighted Hybrid model
            # YouTube comments are user_comments (weight 1.0) vs authoritative_content (weight 3.0)
            structured_data = {
                "meta": {
                    "id": video_id,
                    "title": video_title,
                    "description": description,
                    "url": video_url,
                    "alliance": video.get('alliance', 'Unknown'),  # Initial alliance from discovery
                    "search_query": video.get('search_query', ''),
                    "channel": video.get('channel', 'Unknown'),
                    "scraped_at": datetime.now().isoformat()
                },
                "transcript": transcript_text or "",
                "authoritative_content": [],  # Empty for YouTube sources (noisy signal)
                "user_comments": comments  # Low weight (1.0) - user sentiment
            }
            
            # Classify alliance with full content (more accurate than discovery phase)
            detected_alliance = classify_alliance(structured_data, producer_alliance=video.get('alliance'))
            
            # Skip if no alliance detected (saves storage and processing)
            if detected_alliance == "Unknown":
                print(f"    Skipping: No political alliance detected in content")
                videos_skipped += 1
                continue
            
            # Update alliance in structured data with detected alliance
            structured_data["meta"]["alliance"] = detected_alliance
            print(f"    Alliance detected: {detected_alliance}")
            
            # Prepare metadata for job queue
            video_metadata = {
                "video_id": video_id,
                "video_title": video_title,
                "video_url": video_url,
                "alliance": video.get('alliance', 'Unknown'),
                "search_query": video.get('search_query', ''),
                "channel": video.get('channel', 'Unknown'),
                "comment_count": len(comments),
                "has_transcript": bool(transcript_text),
                "transcript_length": len(transcript_text) if transcript_text else 0
            }
            
            # Save to Supabase via DataSystem (Producer pattern)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"comments/{video_id}_{timestamp}.json"
            
            job_id = data_system.save_raw_json(
                data=structured_data,
                filename=filename,
                video_metadata=video_metadata
            )
            
            if job_id:
                jobs_created += 1
                print(f"  Extracted {len(comments)} user comments" + (f" and transcript ({len(transcript_text)} chars)" if transcript_text else " (no transcript)"))
                print(f"  Job {job_id} created in queue")
            else:
                videos_skipped += 1
                print(f"  Extracted {len(comments)} user comments" + (f" and transcript" if transcript_text else "") + " (FAILED to save - check logs above)")
        
        except Exception as e:
            print(f"  Error processing video: {str(e)[:100]}")
            continue
        
        # Rate limiting
        if i < len(video_list):
            time.sleep(2)
    
    print()
    print("=" * 60)
    print("Scraping Complete!")
    print("=" * 60)
    print(f"Summary:")
    print(f"  Videos processed: {len(video_list)}")
    print(f"  Jobs created: {jobs_created}")
    print(f"  Videos skipped: {videos_skipped} (no comments or save failed)")
    
    # Verify jobs were actually created in database
    if jobs_created > 0:
        try:
            from infra.client import get_supabase_client
            client = get_supabase_client()
            if client:
                result = client.table('job_queue').select('id', count='exact').eq('status', 'PENDING').execute()
                pending_count = result.count or 0
                print(f"\nVerification:")
                print(f"  PENDING jobs in queue: {pending_count}")
                if pending_count < jobs_created:
                    print(f"  [WARN] Expected {jobs_created} jobs but found {pending_count} in queue")
        except Exception as e:
            print(f"  [WARN] Could not verify jobs in database: {str(e)[:100]}")
    
    if jobs_created == 0:
        print(f"\n[WARN] WARNING: No jobs were created!")
        print(f"  - Check if videos have comments")
        print(f"  - Check Supabase connection and storage permissions")
        print(f"  - Check job_queue table exists (run schema.sql)")
        print(f"  - Check storage bucket 'raw_data' exists")
    print("=" * 60)


if __name__ == "__main__":
    # When run standalone, automatically discovers videos
    scrape_comments()

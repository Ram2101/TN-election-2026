"""
YouTube Video Discovery Engine.

Discovers YouTube videos based on keywords defined in config/alliances.json.
Uses yt-dlp to find recent videos per political alliance category.
Returns video list programmatically for use by scraper.py.
"""

import json
import os
import datetime
import yt_dlp
from typing import List, Dict, Optional
from dotenv import load_dotenv

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


CONFIG_PATH = os.path.join("config", "alliances.json")


def load_keywords():
    """
    Load keywords from alliances.json configuration file.
    
    The JSON structure is: {"keywords": {"DMK_Front": [...], "ADMK_Front": [...]}}
    
    Returns:
        List of tuples: (alliance_name, query_string)
    """
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Access the 'keywords' key to get the actual alliance dictionary
    keywords_data = data.get('keywords', data)
    
    queries = []
    for alliance, query_list in keywords_data.items():
        for query in query_list:
            queries.append((alliance, query))
    
    return queries


def search_youtube_videos(query: str, max_results: int = 5) -> List[Dict]:
    """
    Search YouTube for videos matching the query using yt-dlp.
    
    Args:
        query: Search query string
        max_results: Maximum number of videos to return (default: 5)
    
    Returns:
        List of video dictionaries with id, title, channel, url
    """
    videos = []
    
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'default_search': 'ytsearch',
            'max_downloads': max_results,
        }
        
        # Add cookies if available
        ydl_opts = get_ytdlp_opts_with_cookies(ydl_opts)
        
        search_query = f"ytsearch{max_results}:{query}"
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(search_query, download=False)
            
            if 'entries' in info:
                for entry in info['entries']:
                    if entry:
                        videos.append({
                            'id': entry.get('id', ''),
                            'title': entry.get('title', 'Unknown'),
                            'channel': entry.get('channel', 'Unknown'),
                            'url': entry.get('url', f"https://www.youtube.com/watch?v={entry.get('id', '')}")
                        })
    
    except Exception as e:
        print(f"Error searching for '{query}': {str(e)[:100]}")
    
    return videos


def discover_videos(max_videos_per_query: Optional[int] = None) -> List[Dict]:
    """
    Main discovery function that searches for videos based on alliance keywords.
    
    Searches YouTube for videos per keyword category (configurable via env var),
    deduplicates by video ID, and returns the video list.
    
    Args:
        max_videos_per_query: Maximum videos to fetch per query. If None, reads
                             from MAX_VIDEOS_PER_QUERY env var (default: 5)
    
    Returns:
        List of video dictionaries with keys: id, url, title, channel, 
        alliance, search_query, status
    """
    # Get max videos per query from parameter or environment variable
    if max_videos_per_query is None:
        max_videos_per_query = int(os.getenv('MAX_VIDEOS_PER_QUERY', '5'))
    
    queries = load_keywords()
    video_list = []
    seen_ids = set()
    
    print(f"Starting Discovery for {datetime.date.today()}")
    print(f"Total queries to process: {len(queries)}")
    print(f"Max videos per query: {max_videos_per_query}\n")
    
    for alliance, query in queries:
        print(f"Searching [{alliance}]: {query}...")
        
        try:
            videos = search_youtube_videos(query, max_results=max_videos_per_query)
            
            for video in videos:
                vid_id = video['id']
                
                if vid_id and vid_id not in seen_ids:
                    print(f"  Found: {video['title'][:60]}...")
                    video_list.append({
                        "id": vid_id,
                        "url": video['url'],
                        "title": video['title'],
                        "channel": video['channel'],
                        "alliance": alliance,
                        "search_query": query,
                        "status": "pending"
                    })
                    seen_ids.add(vid_id)
                elif vid_id in seen_ids:
                    print(f"  Duplicate: {video['title'][:60]}...")
        
        except Exception as e:
            print(f"  Error processing query '{query}': {str(e)[:100]}")
            continue
    
    print(f"\nDiscovery Complete!")
    print(f"Total unique videos found: {len(video_list)}")
    
    return video_list


if __name__ == "__main__":
    # When run standalone, can optionally save for debugging
    videos = discover_videos()
    print(f"\nDiscovered {len(videos)} videos")

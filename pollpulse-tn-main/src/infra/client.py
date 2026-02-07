"""
Supabase client singleton for the ETL pipeline.

Provides a centralized way to access the Supabase client instance
throughout the application.
"""

import os
from typing import Optional
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Global client instance
_client: Optional[Client] = None


def get_supabase_client() -> Optional[Client]:
    """
    Get or create the Supabase client singleton.
    
    Returns:
        Supabase Client instance if credentials are available, None otherwise.
    """
    global _client
    
    if _client is not None:
        return _client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if supabase_url:
        supabase_url = supabase_url.strip().rstrip('/') + '/'
    if supabase_key:
        supabase_key = supabase_key.strip()
    
    if not supabase_url or not supabase_key:
        print("Error: SUPABASE_URL or SUPABASE_KEY not found in environment")
        print("Please check your .env file and ensure both variables are set")
        return None
    
    # Validate URL format
    if not supabase_url.startswith('https://'):
        print(f"Error: SUPABASE_URL must start with 'https://'. Got: {supabase_url[:20]}...")
        return None
    
    # Validate key format (JWT tokens start with 'eyJ')
    if not supabase_key.startswith('eyJ'):
        print("Warning: SUPABASE_KEY should start with 'eyJ' (JWT token format)")
        print("Make sure you're using the 'anon' key from Supabase Dashboard")
    
    try:
        _client = create_client(supabase_url, supabase_key)
        return _client
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        print("\nTroubleshooting:")
        print("  1. Verify SUPABASE_URL is correct (Settings → API → Project URL)")
        print("  2. Verify SUPABASE_KEY is the 'anon public' key (Settings → API → anon public)")
        print("  3. Ensure .env file is in the project root directory")
        print("  4. Check for extra quotes or spaces in .env file values")
        return None


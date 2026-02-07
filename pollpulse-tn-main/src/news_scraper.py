"""
Tamil News Portal Scraper - High Confidence Location Data Source.

This module scrapes district-specific news from Daily Thanthi to provide
100% accurate location sentiment data. Unlike YouTube scraping where location
is inferred, news portals have explicit district information in URLs.

The scraper:
1. Fetches headlines from district-specific news pages
2. Maps headlines to the 'comments' field for existing NLP pipeline compatibility
3. Includes location_override field for guaranteed location accuracy
4. Uploads data to Supabase Storage via DataSystem
5. SECURITY: Only scrapes from whitelisted domains to prevent spam/malicious content
"""

import json
import os
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

from infra.data_manager import DataSystem


# ============================================================
# DOMAIN WHITELIST - Security Feature
# ============================================================
# Only these domains are allowed to be scraped.
# This prevents:
# 1. Accidental scraping of non-news sites
# 2. Injection of malicious URLs
# 3. Spam content entering the pipeline

ALLOWED_DOMAINS = {
    # Primary source - Tamil news
    "dailythanthi.com",
    "www.dailythanthi.com",
    
    # Other reputable Tamil news sources (for future expansion)
    "dinamalar.com",
    "www.dinamalar.com",
    "dinamani.com",
    "www.dinamani.com",
    "dinakaran.com",
    "www.dinakaran.com",
    "vikatan.com",
    "www.vikatan.com",
    "thehindu.com",
    "www.thehindu.com",
    "newindianexpress.com",
    "www.newindianexpress.com",
    "deccanchronicle.com",
    "www.deccanchronicle.com",
    "timesofindia.indiatimes.com",
    
    # Tamil Nadu government sources
    "tn.gov.in",
    "www.tn.gov.in",
}


def is_domain_allowed(url: str) -> Tuple[bool, str]:
    """
    Check if URL domain is in the whitelist.
    
    Args:
        url: URL to validate
        
    Returns:
        Tuple of (is_allowed, reason)
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        if not domain:
            return False, "Invalid URL: no domain found"
        
        # Check exact match
        if domain in ALLOWED_DOMAINS:
            return True, f"Domain '{domain}' is whitelisted"
        
        # Check if it's a subdomain of an allowed domain
        for allowed in ALLOWED_DOMAINS:
            if domain.endswith('.' + allowed) or domain == allowed:
                return True, f"Domain '{domain}' is a subdomain of whitelisted '{allowed}'"
        
        return False, f"Domain '{domain}' is NOT in whitelist. Allowed: {sorted(ALLOWED_DOMAINS)[:5]}..."
    
    except Exception as e:
        return False, f"URL parsing error: {e}"


# Daily Thanthi district URLs configuration
# All 38 districts of Tamil Nadu
DISTRICT_URLS = {
    # Northern Districts
    "Chennai": "https://www.dailythanthi.com/Districts/Chennai",
    "Chengalpattu": "https://www.dailythanthi.com/Districts/Chengalpattu",
    "Kancheepuram": "https://www.dailythanthi.com/Districts/Kancheepuram",
    "Thiruvallur": "https://www.dailythanthi.com/Districts/Thiruvallur",
    "Vellore": "https://www.dailythanthi.com/Districts/Vellore",
    "Ranipet": "https://www.dailythanthi.com/Districts/Ranipet",
    "Tirupathur": "https://www.dailythanthi.com/Districts/Tirupattur",
    "Tiruvannamalai": "https://www.dailythanthi.com/Districts/Tiruvannamalai",
    "Villupuram": "https://www.dailythanthi.com/Districts/Villupuram",
    "Kallakurichi": "https://www.dailythanthi.com/Districts/Kallakurichi",
    
    # Western Districts
    "Salem": "https://www.dailythanthi.com/Districts/Salem",
    "Namakkal": "https://www.dailythanthi.com/Districts/Namakkal",
    "Erode": "https://www.dailythanthi.com/Districts/Erode",
    "Tiruppur": "https://www.dailythanthi.com/Districts/Tirupur",
    "Coimbatore": "https://www.dailythanthi.com/Districts/Coimbatore",
    "Nilgiris": "https://www.dailythanthi.com/Districts/Nilgiris",
    "Dharmapuri": "https://www.dailythanthi.com/Districts/Dharmapuri",
    "Krishnagiri": "https://www.dailythanthi.com/Districts/Krishnagiri",
    
    # Central Districts
    "Tiruchirappalli": "https://www.dailythanthi.com/Districts/Trichy",
    "Perambalur": "https://www.dailythanthi.com/Districts/Perambalur",
    "Ariyalur": "https://www.dailythanthi.com/Districts/Ariyalur",
    "Cuddalore": "https://www.dailythanthi.com/Districts/Cuddalore",
    "Nagapattinam": "https://www.dailythanthi.com/Districts/Nagapattinam",
    "Mayiladuthurai": "https://www.dailythanthi.com/Districts/Mayiladuthurai",
    "Tiruvarur": "https://www.dailythanthi.com/Districts/Tiruvarur",
    "Thanjavur": "https://www.dailythanthi.com/Districts/Thanjavur",
    "Pudukkottai": "https://www.dailythanthi.com/Districts/Pudukkottai",
    "Karur": "https://www.dailythanthi.com/Districts/Karur",
    
    # Southern Districts
    "Madurai": "https://www.dailythanthi.com/Districts/Madurai",
    "Theni": "https://www.dailythanthi.com/Districts/Theni",
    "Dindigul": "https://www.dailythanthi.com/Districts/Dindigul",
    "Sivaganga": "https://www.dailythanthi.com/Districts/Sivaganga",
    "Ramanathapuram": "https://www.dailythanthi.com/Districts/Ramanathapuram",
    "Virudhunagar": "https://www.dailythanthi.com/Districts/Virudhunagar",
    "Thoothukudi": "https://www.dailythanthi.com/Districts/Thoothukudi",
    "Tirunelveli": "https://www.dailythanthi.com/Districts/Tirunelveli",
    "Tenkasi": "https://www.dailythanthi.com/Districts/Tenkasi",
    "Kanyakumari": "https://www.dailythanthi.com/Districts/Kanyakumari",
}

# User-Agent header to avoid blocking
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Maximum headlines to scrape per district
MAX_HEADLINES_PER_DISTRICT = int(os.getenv('MAX_HEADLINES_PER_DISTRICT', '20'))


def fetch_page(url: str, timeout: int = 10, skip_whitelist: bool = False) -> Optional[str]:
    """
    Fetch HTML content from a URL.
    
    Args:
        url: URL to fetch
        timeout: Request timeout in seconds
        skip_whitelist: If True, skip domain whitelist check (use with caution)
    
    Returns:
        HTML content as string, or None if error
    """
    # Domain whitelist check (security feature)
    if not skip_whitelist:
        is_allowed, reason = is_domain_allowed(url)
        if not is_allowed:
            print(f"  BLOCKED: {reason}")
            return None
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'  # Ensure proper encoding for Tamil text
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching {url}: {str(e)[:100]}")
        return None


def extract_headlines(html: str, district: str) -> List[str]:
    """
    Extract headlines from Daily Thanthi HTML page.
    
    This function tries multiple CSS selectors to find headlines, as website
    structure may vary. Common selectors for news sites:
    - h2, h3 tags (common headline tags)
    - div.ListingNews_content (specific to Daily Thanthi structure)
    - article tags
    - div.news-item or similar
    
    Args:
        html: HTML content of the page
        district: District name (for logging)
    
    Returns:
        List of headline text strings
    """
    headlines = []
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Strategy 1: Try specific Daily Thanthi selectors
        # Common patterns for news listing pages
        selectors = [
            'div.ListingNews_content h3 a',  # Headlines in listing div
            'div.ListingNews_content h2 a',
            'div.news-list h3 a',
            'article h3 a',
            'div.news-item h3 a',
            'h3 a',  # Fallback: any h3 with link
            'h2 a',  # Fallback: any h2 with link
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                print(f"  Found {len(elements)} headlines using selector: {selector}")
                for elem in elements[:MAX_HEADLINES_PER_DISTRICT]:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 10:  # Filter out very short text
                        headlines.append(text)
                break  # Use first selector that finds elements
        
        # Strategy 2: If no headlines found, try extracting from common news structures
        if not headlines:
            # Look for any divs with class containing "news", "article", "headline"
            news_divs = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['news', 'article', 'headline', 'story']
            ))
            
            for div in news_divs[:MAX_HEADLINES_PER_DISTRICT]:
                # Try to find headline text within the div
                headline_elem = div.find(['h2', 'h3', 'h4', 'a'])
                if headline_elem:
                    text = headline_elem.get_text(strip=True)
                    if text and len(text) > 10:
                        headlines.append(text)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_headlines = []
        for headline in headlines:
            headline_lower = headline.lower()
            if headline_lower not in seen:
                seen.add(headline_lower)
                unique_headlines.append(headline)
        
        return unique_headlines[:MAX_HEADLINES_PER_DISTRICT]
    
    except Exception as e:
        print(f"  Error parsing HTML for {district}: {str(e)[:100]}")
        return []


def scrape_district_news(district: str, url: str) -> Optional[Dict]:
    """
    Scrape news headlines for a specific district.
    
    Args:
        district: District name
        url: URL of the district news page
    
    Returns:
        Dictionary with scraped data, or None if error
    """
    print(f"Scraping {district}...")
    print(f"  URL: {url}")
    
    # Fetch page
    html = fetch_page(url)
    if not html:
        print(f"  Failed to fetch page")
        return None
    
    # Extract headlines
    headlines = extract_headlines(html, district)
    
    if not headlines:
        print(f"  No headlines found")
        return None
    
    print(f"  Extracted {len(headlines)} headlines")
    
    # Structure data payload with Weighted Hybrid model
    # News headlines are authoritative_content (weight 3.0) vs user_comments (weight 1.0)
    structured_data = {
        "meta": {
            "source": "DailyThanthi",
            "type": "news_headline",
            "url": url,
            "district": district,
            "scraped_at": datetime.now().isoformat(),
            "headline_count": len(headlines)
        },
        "location_override": district,  # 100% confidence location from URL (ground truth)
        "authoritative_content": headlines,  # High weight (3.0) - authoritative signal
        "user_comments": []  # Empty for news sources
    }
    
    return structured_data


def scrape_news_portals(district_urls: Optional[Dict[str, str]] = None, validate_only: bool = False):
    """
    Main scraping function for news portals.
    
    Scrapes headlines from each district page and uploads to Supabase
    via DataSystem. Follows the same Producer pattern as scraper.py.
    
    SECURITY: All URLs are validated against the domain whitelist before scraping.
    
    Args:
        district_urls: Optional dictionary mapping district names to URLs.
                      If None, uses default DISTRICT_URLS.
        validate_only: If True, only validate URLs without scraping (for testing)
    """
    if district_urls is None:
        district_urls = DISTRICT_URLS
    
    if not district_urls:
        print("No district URLs provided")
        return
    
    # ============================================================
    # SECURITY: Validate all URLs against whitelist before starting
    # ============================================================
    print("Validating URLs against domain whitelist...")
    blocked_urls = []
    valid_urls = {}
    
    for district, url in district_urls.items():
        is_allowed, reason = is_domain_allowed(url)
        if is_allowed:
            valid_urls[district] = url
        else:
            blocked_urls.append((district, url, reason))
            print(f"  BLOCKED: {district} - {reason}")
    
    if blocked_urls:
        print(f"\nWARNING: {len(blocked_urls)} URLs blocked by whitelist:")
        for district, url, reason in blocked_urls:
            print(f"  - {district}: {url}")
        print()
    
    if not valid_urls:
        print("ERROR: No valid URLs to scrape after whitelist validation")
        return
    
    print(f"Validated {len(valid_urls)} URLs (blocked {len(blocked_urls)})")
    
    if validate_only:
        print("\nValidation mode - skipping actual scraping")
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
    
    print()
    print("=" * 60)
    print("Starting News Portal Scraper (High Confidence Location)")
    print(f"Total districts to process: {len(valid_urls)}")
    print(f"Whitelisted domains: {len(ALLOWED_DOMAINS)}")
    print("=" * 60)
    print()
    
    # Use only valid URLs for scraping
    district_urls = valid_urls
    
    jobs_created = 0
    districts_skipped = 0
    
    for i, (district, url) in enumerate(district_urls.items(), 1):
        print(f"[{i}/{len(district_urls)}] Processing: {district}")
        
        try:
            # Scrape district news
            data = scrape_district_news(district, url)
            
            if not data:
                print(f"  Skipping {district} (no data extracted)")
                districts_skipped += 1
                continue
            
            # Prepare metadata for job queue
            news_metadata = {
                "source": "DailyThanthi",
                "type": "news_headline",
                "district": district,
                "url": url,
                "headline_count": len(data.get('authoritative_content', [])),
                "location_override": district
            }
            
            # Save to Supabase via DataSystem (Producer pattern)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"news/{district.lower()}_{timestamp}.json"
            
            job_id = data_system.save_raw_json(
                data=data,
                filename=filename,
                video_metadata=news_metadata
            )
            
            if job_id:
                jobs_created += 1
                print(f"  Uploaded {len(data.get('authoritative_content', []))} headlines")
                print(f"  Job {job_id} created in queue")
            else:
                districts_skipped += 1
                print(f"  FAILED to upload data - check logs above")
        
        except Exception as e:
            print(f"  Error processing {district}: {str(e)[:100]}")
            continue
        
        # Rate limiting between districts
        if i < len(district_urls):
            time.sleep(2)
    
    print()
    print("=" * 60)
    print("News Scraping Complete!")
    print("=" * 60)
    print(f"Summary:")
    print(f"  Districts processed: {len(district_urls)}")
    print(f"  Jobs created: {jobs_created}")
    print(f"  Districts skipped: {districts_skipped} (no data or save failed)")
    
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
        print(f"  - Check if headlines are being extracted")
        print(f"  - Check Supabase connection and storage permissions")
        print(f"  - Check job_queue table exists (run schema.sql)")
        print(f"  - Check storage bucket 'raw_data' exists")
    print("=" * 60)


if __name__ == "__main__":
    # When run standalone, scrapes all configured districts
    scrape_news_portals()


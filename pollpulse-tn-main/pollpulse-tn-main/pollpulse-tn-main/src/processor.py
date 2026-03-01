"""
Sentiment Analysis Processor - Consumer Component.

This module implements the Consumer pattern in the decoupled ETL architecture.
It polls the job_queue table for PENDING jobs, downloads the raw JSON data
from Supabase Storage, runs sentiment analysis using HuggingFace, and updates
the job status to DONE.

Production Features:
- Semantic deduplication (skip already processed content)
- Dead Letter Queue (failed jobs stored for inspection)
- Metrics logging (processing latency, error rates)
- Data lineage (track source_ids in predictions)
- Outlier cap (prevent single video from dominating)
"""

import json
import os
import time
import hashlib
import traceback
import math
import re
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from transformers import pipeline, AutoModelForSequenceClassification, XLMRobertaTokenizer

from infra.client import get_supabase_client
from infra.data_manager import DataSystem
from utils.classifier import classify_alliance


# Configuration
MODEL_VERSION = os.getenv('MODEL_VERSION', 'xlm-roberta-sentiment-v1')
MAX_INFLUENCE_PER_SOURCE = float(os.getenv('MAX_INFLUENCE_PER_SOURCE', '0.05'))
ENABLE_DEDUPLICATION = os.getenv('ENABLE_DEDUPLICATION', 'true').lower() == 'true'
ENABLE_DLQ = os.getenv('ENABLE_DLQ', 'true').lower() == 'true'
ENABLE_METRICS = os.getenv('ENABLE_METRICS', 'true').lower() == 'true'

FRESHNESS_HALF_LIFE_DAYS = float(os.getenv('FRESHNESS_HALF_LIFE_DAYS', '14'))  # Half-life for decay
ENABLE_ENGAGEMENT_WEIGHTING = os.getenv('ENABLE_ENGAGEMENT_WEIGHTING', 'true').lower() == 'true'
ENABLE_PROBABILITY_SCORING = os.getenv('ENABLE_PROBABILITY_SCORING', 'true').lower() == 'true'
ENABLE_OUTLIER_CAP = os.getenv('ENABLE_OUTLIER_CAP', 'true').lower() == 'true'


# Load gazetteer (districts data)
GAZETTEER_PATH = os.path.join("config", "districts.json")
GAZETTEER = {}

# Load alliances configuration
ALLIANCES_PATH = os.path.join("config", "alliances.json")
ALLIANCES = {}

# Load entity map for politician routing
ENTITY_MAP_PATH = os.path.join("config", "entity_map.json")
ENTITY_MAP = {}


def load_gazetteer() -> Dict:
    """
    Load the districts gazetteer from config/districts.json.
    
    Returns:
        Dictionary mapping district names to their keywords and constituencies
    """
    global GAZETTEER
    if GAZETTEER:
        return GAZETTEER
    
    try:
        with open(GAZETTEER_PATH, 'r', encoding='utf-8') as f:
            GAZETTEER = json.load(f)
        print(f"Loaded gazetteer with {len(GAZETTEER)} districts")
        return GAZETTEER
    except Exception as e:
        print(f"Error loading gazetteer: {e}")
        return {}


def load_alliances() -> Dict:
    """
    Load the alliances configuration from config/alliances.json.
    
    Returns:
        Dictionary with 'keywords' mapping alliance names to their keywords
    """
    global ALLIANCES
    if ALLIANCES:
        return ALLIANCES
    
    try:
        with open(ALLIANCES_PATH, 'r', encoding='utf-8') as f:
            ALLIANCES = json.load(f)
        keywords = ALLIANCES.get('keywords', {})
        print(f"Loaded alliances: {list(keywords.keys())}")
        return ALLIANCES
    except Exception as e:
        print(f"Error loading alliances: {e}")
        return {}


def load_entity_map() -> Dict:
    """
    Load the entity map for politician-to-constituency routing.
    
    Returns:
        Dictionary with politicians, alias_index, and constituency_politicians
    """
    global ENTITY_MAP
    if ENTITY_MAP:
        return ENTITY_MAP
    
    try:
        with open(ENTITY_MAP_PATH, 'r', encoding='utf-8') as f:
            ENTITY_MAP = json.load(f)
        print(f"Loaded entity map: {len(ENTITY_MAP.get('politicians', {}))} politicians")
        return ENTITY_MAP
    except FileNotFoundError:
        print("Entity map not found. Run src/discover_entities.py to generate it.")
        return {}
    except Exception as e:
        print(f"Error loading entity map: {e}")
        return {}


def detect_politicians(data: Dict) -> List[Dict]:
    """
    Detect politician mentions in content and return their constituencies.
    
    This enables routing sentiment to specific constituencies when a 
    politician is mentioned (e.g., "Stalin speech" -> KOLATHUR).
    
    Args:
        data: Full JSON payload containing meta, transcript, comments
        
    Returns:
        List of matched politicians with their constituencies:
        [{"name": "M. K. Stalin", "constituency": "KOLATHUR", "party": "DMK"}]
    """
    entity_map = load_entity_map()
    if not entity_map:
        return []
    
    alias_index = entity_map.get('alias_index', {})
    politicians = entity_map.get('politicians', {})
    
    # Combine all text sources
    meta = data.get('meta', {})
    title = (meta.get('title', '') or '').lower()
    description = (meta.get('description', '') or '')[:500].lower()
    transcript = (data.get('transcript', '') or '')[:2000].lower()
    
    # Build comment text
    comments = data.get('user_comments', []) or data.get('comments', [])
    comment_text = ""
    for comment in comments[:50]:  # Limit to first 50 comments
        if isinstance(comment, dict):
            comment_text += " " + (comment.get('text', '') or '').lower()
        elif isinstance(comment, str):
            comment_text += " " + comment.lower()
    
    combined_text = f"{title} {description} {transcript} {comment_text}"
    
    matched = []
    matched_names = set()  # Avoid duplicates
    
    for alias, normalized_name in alias_index.items():
        # Check if alias appears in text (word boundary aware)
        # Use simple "in" check for speed, could use regex for precision
        if alias in combined_text and normalized_name not in matched_names:
            politician_data = politicians.get(normalized_name, {})
            constituency = politician_data.get('constituency')
            
            if constituency:  # Only add if has constituency mapping
                matched.append({
                    "name": politician_data.get('canonical_name', alias),
                    "constituency": constituency,
                    "party": politician_data.get('party', ''),
                    "matched_alias": alias
                })
                matched_names.add(normalized_name)
    
    if matched:
        print(f"  Politicians detected: {[m['name'] for m in matched]}")
    
    return matched



def get_content_id(data: Dict) -> str:
    """Extract or generate content ID for deduplication."""
    meta = data.get('meta', {})
    
    # For YouTube: use video_id
    if meta.get('id'):
        return meta['id']
    
    # For news: hash the URL
    if meta.get('url'):
        return hashlib.md5(meta['url'].encode()).hexdigest()[:16]
    
    # Fallback: hash the whole meta
    return hashlib.md5(json.dumps(meta, sort_keys=True).encode()).hexdigest()[:16]


def get_content_type(data: Dict) -> str:
    """Determine content type from payload."""
    meta = data.get('meta', {})
    source = meta.get('source', '')
    
    if source in ['DailyThanthi', 'news']:
        return 'news'
    return 'youtube'


def is_duplicate_content(client, content_id: str, alliance: str) -> bool:
    """Check if content was already processed (semantic deduplication)."""
    if not ENABLE_DEDUPLICATION:
        return False
    
    try:
        result = client.table('processed_content').select('id').eq(
            'content_id', content_id
        ).execute()
        
        return len(result.data) > 0
    except Exception as e:
        print(f"  Warning: Deduplication check failed: {e}")
        return False  # Fail open - process anyway


def mark_content_processed(
    client,
    content_id: str,
    content_type: str,
    alliance: str,
    file_path: str,
    sentiment_score: float
) -> bool:
    """Mark content as processed for deduplication."""
    if not ENABLE_DEDUPLICATION:
        return True
    
    try:
        client.table('processed_content').insert({
            'content_id': content_id,
            'content_type': content_type,
            'alliance': alliance,
            'file_path': file_path,
            'sentiment_score': sentiment_score,
            'processed_at': datetime.now(timezone.utc).isoformat()
        }).execute()
        return True
    except Exception as e:
        # May fail on duplicate - that's OK
        if 'duplicate' not in str(e).lower():
            print(f"  Warning: Failed to mark content processed: {e}")
        return False


def add_to_dlq(
    client,
    job_id: str,
    file_path: str,
    error_message: str,
    error_type: str,
    payload: Optional[Dict] = None
) -> bool:
    """Add failed job to Dead Letter Queue for inspection."""
    if not ENABLE_DLQ:
        return True
    
    try:
        client.table('dead_letter_queue').insert({
            'original_job_id': job_id,
            'file_path': file_path,
            'error_message': error_message[:1000],  # Truncate
            'error_type': error_type,
            'payload': payload,
            'failed_at': datetime.now(timezone.utc).isoformat(),
            'retry_count': 0
        }).execute()
        print(f"  Added to DLQ: {error_type}")
        return True
    except Exception as e:
        print(f"  Warning: Failed to add to DLQ: {e}")
        return False


def log_metric(client, name: str, value: float, dimensions: Optional[Dict] = None) -> bool:
    """Log a metric to the pipeline_metrics table."""
    if not ENABLE_METRICS:
        return True
    
    try:
        client.table('pipeline_metrics').insert({
            'metric_name': name,
            'metric_value': value,
            'dimensions': dimensions or {},
            'recorded_at': datetime.now(timezone.utc).isoformat()
        }).execute()
        return True
    except Exception as e:
        # Don't fail on metrics errors
        return False


def apply_influence_cap(current_score: float, new_delta: float) -> float:
    """Cap the influence of a single source to prevent outliers."""
    if not ENABLE_OUTLIER_CAP:
        return current_score + new_delta
    capped_delta = max(-MAX_INFLUENCE_PER_SOURCE, 
                       min(MAX_INFLUENCE_PER_SOURCE, new_delta))
    return current_score + capped_delta


def calculate_freshness_decay(last_updated: datetime) -> float:
    """
    Calculate time-weighted decay factor based on how stale the prediction is.
    
    Uses exponential decay with configurable half-life.
    Formula: decay = exp(-ln(2) * days_old / half_life)
    
    Args:
        last_updated: When the prediction was last updated
        
    Returns:
        Decay factor between 0.0 and 1.0 (1.0 = fresh, approaches 0 = stale)
    """
    if last_updated is None:
        return 1.0
    
    now = datetime.now(timezone.utc)
    
    # Handle timezone-naive datetime
    if last_updated.tzinfo is None:
        last_updated = last_updated.replace(tzinfo=timezone.utc)
    
    days_old = (now - last_updated).total_seconds() / 86400  # Convert to days
    
    if days_old <= 0:
        return 1.0
    
    # Exponential decay: decay_factor = exp(-ln(2) * days / half_life)
    decay_factor = math.exp(-0.693 * days_old / FRESHNESS_HALF_LIFE_DAYS)
    
    return max(0.01, decay_factor)  # Minimum 1% to prevent complete zeroing


def get_engagement_weight(likes: int) -> float:
    """
    Calculate engagement weight using logarithmic scaling.
    
    A comment with 1000 likes should count more than one with 1 like,
    but not 1000x more (diminishing returns).
    
    Formula: weight = 1 + log10(1 + likes)
    
    Args:
        likes: Number of likes on the comment
        
    Returns:
        Weight multiplier (1.0 for 0 likes, ~4.0 for 1000 likes)
    """
    if not ENABLE_ENGAGEMENT_WEIGHTING or likes <= 0:
        return 1.0
    
    return 1.0 + math.log10(1 + likes)


def apply_freshness_to_predictions(client) -> int:
    """
    Apply freshness decay to all predictions in the database.
    
    This should be run periodically (e.g., daily) to decay stale predictions.
    
    Args:
        client: Supabase client
        
    Returns:
        Number of predictions updated
    """
    try:
        # Get all predictions with their timestamps
        result = client.table('constituency_predictions').select(
            'id, constituency_name, alliance, sentiment_score, confidence_weight, last_updated'
        ).execute()
        
        if not result.data:
            return 0
        
        updated_count = 0
        
        for prediction in result.data:
            last_updated_str = prediction.get('last_updated')
            if not last_updated_str:
                continue
            
            # Parse timestamp
            try:
                last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
            except:
                continue
            
            # Calculate decay
            decay = calculate_freshness_decay(last_updated)
            current_confidence = prediction.get('confidence_weight', 0.5)
            
            # Apply decay to confidence (not sentiment score)
            new_confidence = current_confidence * decay
            
            # Only update if significantly changed (>1% difference)
            if abs(new_confidence - current_confidence) > 0.01:
                client.table('constituency_predictions').update({
                    'confidence_weight': round(new_confidence, 4)
                }).eq('id', prediction['id']).execute()
                updated_count += 1
        
        return updated_count
    
    except Exception as e:
        print(f"Error applying freshness decay: {e}")
        return 0


def detect_alliance(data: Dict) -> str:
    """
    Detect which political alliance the content belongs to.
    
    Uses shared classification utility for consistency across pipeline.
    
    Args:
        data: Full JSON payload
    
    Returns:
        Alliance name (e.g., "DMK_Front") or "Unknown"
    """
    # Check if alliance is already set by producer
    meta = data.get('meta', {})
    producer_alliance = meta.get('alliance', '')
    
    # Use shared classifier (will use producer_alliance if valid)
    return classify_alliance(data, producer_alliance=producer_alliance if producer_alliance else None)


def get_all_constituencies() -> List[str]:
    """
    Get all 234 constituencies from the gazetteer.
    
    Returns:
        List of all constituency names
    """
    gazetteer = load_gazetteer()
    constituencies = []
    for district_data in gazetteer.values():
        constituencies.extend(district_data.get('constituencies', []))
    return constituencies


def get_constituencies_for_districts(districts: List[str]) -> List[str]:
    """
    Get constituencies for specific districts.
    
    Args:
        districts: List of district names
    
    Returns:
        List of constituency names for those districts
    """
    gazetteer = load_gazetteer()
    constituencies = []
    
    for district in districts:
        if district in gazetteer:
            constituencies.extend(gazetteer[district].get('constituencies', []))
    
    return constituencies


def calculate_sentiment_score(sentiment_results: Dict) -> float:
    """
    Calculate a single sentiment score from weighted results.
    
    Score ranges from -1.0 (very negative) to +1.0 (very positive).
    
    Formula: (positive - negative) / total
    
    Args:
        sentiment_results: Output from compute_weighted_sentiment()
    
    Returns:
        Sentiment score between -1.0 and +1.0
    """
    weighted_positive = sentiment_results.get('weighted_positive', 0)
    weighted_negative = sentiment_results.get('weighted_negative', 0)
    total_weighted = sentiment_results.get('total_weighted', 0)
    
    if total_weighted == 0:
        return 0.0
    
    # Score: (positive - negative) / total
    # Ranges from -1.0 (all negative) to +1.0 (all positive)
    score = (weighted_positive - weighted_negative) / total_weighted
    return round(score, 4)


def upsert_constituency_prediction(
    client,
    constituency_name: str,
    alliance_name: str,
    current_score: float,
    is_state_wide: bool = False,
    source_id: Optional[str] = None,
    model_version: Optional[str] = None,
    avg_confidence: float = 0.5
) -> bool:
    """
    Upsert a constituency prediction with enhancements.
    
    Sprint 2 Features:
    - Outlier cap: Limits influence of any single source
    - Confidence weighting: Uses model confidence in weight calculation
    
    Formula: New_Score = Old_Score + capped_delta
    Where capped_delta = cap(current_score * new_factor - old_score * (1-decay_factor))
    
    Args:
        client: Supabase client
        constituency_name: Name of the constituency
        alliance_name: Name of the political alliance
        current_score: Current sentiment score (-1.0 to +1.0)
        is_state_wide: If True, apply lower weight (0.05 instead of 0.1)
        source_id: Content ID for data lineage tracking
        model_version: Version of the ML model used
        avg_confidence: Average model confidence (0.0-1.0)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Set decay factor based on whether this is local or state-wide
        if is_state_wide:
            new_factor = 0.05   # New score weight (lower for state-wide)
        else:
            new_factor = 0.1   # New score weight
        
        # Check if row exists
        result = client.table('constituency_predictions').select(
            'sentiment_score, confidence_weight, source_ids, source_count'
        ).eq('constituency_name', constituency_name).eq('alliance', alliance_name).execute()
        
        if result.data and len(result.data) > 0:
            # Row exists - update with moving average + outlier cap
            old_score = result.data[0].get('sentiment_score', 0.0)
            old_confidence = result.data[0].get('confidence_weight', 0.5)
            old_sources = result.data[0].get('source_ids', []) or []
            source_count = result.data[0].get('source_count', 0) or 0
            
            # Calculate delta (what this source would contribute)
            raw_delta = (current_score - old_score) * new_factor
            
            # Sprint 2: Apply outlier cap to prevent single source dominance
            new_score = apply_influence_cap(old_score, raw_delta)
            new_score = round(max(-1.0, min(1.0, new_score)), 4)  # Clamp to [-1, 1]
            
            # Update confidence weight (blend old and new, weighted by avg_confidence)
            new_confidence = (old_confidence * 0.8) + (avg_confidence * 0.2)
            new_confidence = round(new_confidence, 4)
            
            # Update source_ids (keep last 100)
            if source_id and source_id not in old_sources:
                old_sources.append(source_id)
                if len(old_sources) > 100:
                    old_sources = old_sources[-100:]
                source_count += 1
            
            update_data = {
                'sentiment_score': new_score,
                'confidence_weight': new_confidence,
                'source_count': source_count,
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            
            if source_id:
                update_data['source_ids'] = old_sources
            if model_version:
                update_data['model_version'] = model_version
            
            client.table('constituency_predictions').update(update_data).eq(
                'constituency_name', constituency_name
            ).eq('alliance', alliance_name).execute()
        else:
            # Row doesn't exist - insert new (cap for initial score too)
            initial_score = apply_influence_cap(0.0, current_score * new_factor)
            initial_score = round(max(-1.0, min(1.0, initial_score)), 4)
            
            insert_data = {
                'constituency_name': constituency_name,
                'alliance': alliance_name,
                'district': 'Unknown',  # Will be updated by location detection
                'sentiment_score': initial_score,
                'confidence_weight': avg_confidence,
                'source_count': 1 if source_id else 0
            }
            
            if source_id:
                insert_data['source_ids'] = [source_id]
            if model_version:
                insert_data['model_version'] = model_version
            
            client.table('constituency_predictions').insert(insert_data).execute()
        
        return True
    except Exception as e:
        print(f"Error upserting prediction for {constituency_name}/{alliance_name}: {e}")
        return False


def persist_predictions(
    client,
    detected_locations: List[str],
    alliance_name: str,
    sentiment_score: float,
    source_id: Optional[str] = None,
    model_version: Optional[str] = None,
    avg_confidence: float = 0.5
) -> int:
    """
    Persist sentiment predictions to the database with Sprint 2 enhancements.
    
    Handles both local (specific districts) and state-wide updates.
    
    Args:
        client: Supabase client
        detected_locations: List of detected districts or ["State_Wide"]
        alliance_name: Political alliance name
        sentiment_score: Sentiment score (-1.0 to +1.0)
        source_id: Content ID for data lineage tracking
        model_version: Version of the ML model used
        avg_confidence: Average model confidence (Sprint 2)
    
    Returns:
        Number of constituencies updated
    """
    # Handle "Unknown" alliance
    # For news articles: Skip if truly neutral (no political content detected)
    # For YouTube: Skip (should have alliance from discovery phase)
    if alliance_name == "Unknown":
        print("  Skipping persistence: Unknown alliance (no political content detected)")
        print("  Note: Generic news articles without alliance-specific content are excluded")
        return 0
    
    updated_count = 0
    
    if "State_Wide" in detected_locations:
        # State-wide: Update ALL constituencies with lower weight
        print(f"  Persisting to ALL constituencies (State_Wide, weight: 0.05)")
        constituencies = get_all_constituencies()
        
        for constituency in constituencies:
            if upsert_constituency_prediction(
                client, constituency, alliance_name, sentiment_score, 
                is_state_wide=True, source_id=source_id, model_version=model_version,
                avg_confidence=avg_confidence
            ):
                updated_count += 1
    else:
        # Local: Update only constituencies in detected districts
        constituencies = get_constituencies_for_districts(detected_locations)
        print(f"  Persisting to {len(constituencies)} constituencies in {detected_locations}")
        
        for constituency in constituencies:
            if upsert_constituency_prediction(
                client, constituency, alliance_name, sentiment_score, 
                is_state_wide=False, source_id=source_id, model_version=model_version,
                avg_confidence=avg_confidence
            ):
                updated_count += 1
    
    return updated_count


def detect_location(data: Dict) -> List[str]:
    """
    Detect location(s) from video data using Metadata-First strategy.
    
    Priority order:
    0. location_override (from news scraper - 100% confidence)
    1. Metadata (title + description) - High Confidence
    2. Transcript (if available)
    3. Comments (require 3+ mentions per district) - Crowdsourcing
    
    Args:
        data: Full JSON payload containing meta, transcript, and comments
    
    Returns:
        List of district names, or ["State_Wide"] if no match found
    """
    # Priority 0: Check for location_override (from news scraper - highest confidence)
    location_override = data.get('location_override')
    if location_override:
        print(f"  Location from override (news scraper): {location_override}")
        return [location_override] if isinstance(location_override, str) else location_override
    
    gazetteer = load_gazetteer()
    if not gazetteer:
        return ["State_Wide"]
    
    detected_districts = set()
    
    # Priority 1: Metadata (title + description)
    meta = data.get('meta', {})
    title = meta.get('title', '') or ''
    description = meta.get('description', '') or ''
    metadata_text = f"{title} {description}".lower()
    
    for district_name, district_data in gazetteer.items():
        keywords = district_data.get('keywords', [])
        for keyword in keywords:
            if keyword.lower() in metadata_text:
                detected_districts.add(district_name)
                break  # Found match, move to next district
    
    if detected_districts:
        print(f"  Location detected from metadata: {sorted(detected_districts)}")
        return sorted(list(detected_districts))
    
    # Priority 2: Transcript
    transcript = data.get('transcript', '') or ''
    if transcript:
        transcript_lower = transcript.lower()
        for district_name, district_data in gazetteer.items():
            keywords = district_data.get('keywords', [])
            for keyword in keywords:
                if keyword.lower() in transcript_lower:
                    detected_districts.add(district_name)
                    break
    
    if detected_districts:
        print(f"  Location detected from transcript: {sorted(detected_districts)}")
        return sorted(list(detected_districts))
    
    # Priority 3: Comments (require 3+ mentions per district)
    # Check both new field (user_comments) and old field (comments) for backward compatibility
    comments = data.get('user_comments', []) or data.get('comments', [])
    if comments:
        district_mentions = {}
        
        for comment in comments:
            # Handle both formats: dict (YouTube) and string (news scraper)
            if isinstance(comment, dict):
                comment_text = (comment.get('text', '') or '').lower()
            elif isinstance(comment, str):
                comment_text = comment.lower()
            else:
                continue
            
            if not comment_text:
                continue
            
            for district_name, district_data in gazetteer.items():
                keywords = district_data.get('keywords', [])
                for keyword in keywords:
                    if keyword.lower() in comment_text:
                        district_mentions[district_name] = district_mentions.get(district_name, 0) + 1
                        break  # Count once per comment per district
        
        # Only include districts mentioned in 3+ different comments
        for district_name, mention_count in district_mentions.items():
            if mention_count >= 3:
                detected_districts.add(district_name)
    
    if detected_districts:
        print(f"  Location detected from comments (3+ mentions): {sorted(detected_districts)}")
        return sorted(list(detected_districts))
    
    # Fallback: State-wide
    print(f"  No location detected, defaulting to State_Wide")
    return ["State_Wide"]


def load_sentiment_model():
    """
    Initialize the HuggingFace sentiment analysis pipeline.
    
    Returns:
        Pipeline object for sentiment analysis
    """
    try:
        print("Loading sentiment model (this may take a moment on first run)...")
        print("Model: cardiffnlp/twitter-xlm-roberta-base-sentiment")
        
        # Set cache directory if not already set
        if not os.getenv('HF_HOME') and not os.getenv('TRANSFORMERS_CACHE'):
            cache_dir = os.path.join(os.path.expanduser('~'), '.cache', 'huggingface')
            os.makedirs(cache_dir, exist_ok=True)
        
        model_name = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
        
        # Use slow tokenizer class directly to avoid fast tokenizer conversion bug
        print("Loading tokenizer...")
        tokenizer = XLMRobertaTokenizer.from_pretrained(model_name)
        
        # Load model
        print("Loading model...")
        model_obj = AutoModelForSequenceClassification.from_pretrained(model_name)
        
        # Create pipeline with explicit tokenizer
        print("Creating pipeline...")
        model = pipeline(
            "sentiment-analysis",
            model=model_obj,
            tokenizer=tokenizer,
            return_all_scores=True,
            device=-1  # Use CPU (set to 0 for GPU if available)
        )
        print("Model loaded successfully!")
        return model
    except Exception as e:
        import traceback
        print(f"Error loading sentiment model: {e}")
        print(f"Error type: {type(e).__name__}")
        print("Full traceback:")
        traceback.print_exc()
        print("\nTroubleshooting:")
        print("1. Ensure protobuf is installed: pip install protobuf")
        print("2. Check internet connection (model needs to download on first run)")
        print("3. Try clearing cache: rm -rf ~/.cache/huggingface")
        return None


def analyze_sentiment(texts: list, model) -> Dict:
    """
    Run sentiment analysis on a list of texts with Sprint 2 enhancements.
    
    Features:
    - Engagement weighting: Comments with more likes count more
    - Probability scoring: Uses model confidence, not just top label
    
    Args:
        texts: List of text items (can be dict with 'text'/'likes' keys or plain strings)
        model: HuggingFace pipeline model
    
    Returns:
        Dictionary with sentiment statistics (weighted counts and probabilities)
    """
    if not model:
        return {"error": "Model not available"}
    
    sentiments = {
        "positive": 0.0,
        "negative": 0.0,
        "neutral": 0.0,
        "total": len(texts),
        # Sprint 2: Probability-based scores
        "positive_prob": 0.0,
        "negative_prob": 0.0,
        "neutral_prob": 0.0,
        "total_weight": 0.0,
        "avg_confidence": 0.0
    }
    
    try:
        # Handle both formats: dict (YouTube) and string (news scraper)
        text_list = []
        likes_list = []  # Sprint 2: Track likes for engagement weighting
        
        for item in texts:
            if isinstance(item, dict):
                text = item.get('text', '')
                likes = item.get('likes', 0) or item.get('like_count', 0) or 0
            elif isinstance(item, str):
                text = item
                likes = 0
            else:
                continue
            
            if text:
                text_list.append(text)
                likes_list.append(likes)
        
        if not text_list:
            return sentiments
        
        sentiments["total"] = len(text_list)
        total_confidence = 0.0
        
        # Run inference in batches to avoid memory issues
        batch_size = 32
        for i in range(0, len(text_list), batch_size):
            batch = text_list[i:i + batch_size]
            batch_likes = likes_list[i:i + batch_size]
            
            # Request all scores (not just top-1) for probability scoring
            results = model(batch, top_k=None) if ENABLE_PROBABILITY_SCORING else model(batch)
            
            for idx, result in enumerate(results):
                # Get engagement weight for this item
                engagement_weight = get_engagement_weight(batch_likes[idx])
                sentiments["total_weight"] += engagement_weight
                
                if ENABLE_PROBABILITY_SCORING and isinstance(result, list):
                    # Sprint 2: Use full probability distribution
                    for score_item in result:
                        label = score_item['label'].lower()
                        prob = score_item['score']
                        weighted_prob = prob * engagement_weight
                        
                        if 'positive' in label:
                            sentiments["positive_prob"] += weighted_prob
                            sentiments["positive"] += weighted_prob
                        elif 'negative' in label:
                            sentiments["negative_prob"] += weighted_prob
                            sentiments["negative"] += weighted_prob
                        else:
                            sentiments["neutral_prob"] += weighted_prob
                            sentiments["neutral"] += weighted_prob
                    
                    # Track confidence (highest probability)
                    if result:
                        top_prob = max(r['score'] for r in result)
                        total_confidence += top_prob
                else:
                    # Legacy: Binary classification (top label only)
                    if isinstance(result, list) and len(result) > 0:
                        top_result = max(result, key=lambda x: x['score'])
                        label = top_result['label'].lower()
                        confidence = top_result['score']
                        total_confidence += confidence
                        
                        if 'positive' in label:
                            sentiments["positive"] += engagement_weight
                        elif 'negative' in label:
                            sentiments["negative"] += engagement_weight
                        else:
                            sentiments["neutral"] += engagement_weight
        
        # Calculate average confidence
        if sentiments["total"] > 0:
            sentiments["avg_confidence"] = round(total_confidence / sentiments["total"], 4)
    
    except Exception as e:
        print(f"Error during sentiment analysis: {e}")
    
    return sentiments


def compute_weighted_sentiment(
    authoritative_content: list,
    user_comments: list,
    model,
    authoritative_weight: float = 3.0,
    user_weight: float = 1.0
) -> Dict:
    """
    Compute weighted sentiment using the "Weighted Hybrid" model.
    
    Applies different weights to authoritative content (news) vs user comments:
    - Authoritative content (news headlines): weight 3.0 (authoritative signal)
    - User comments (social media): weight 1.0 (noisy signal)
    
    Formula: Score = (User_Sentiment * 1.0) + (Authoritative_Sentiment * 3.0)
    
    This mimics "Market Sentiment" vs "Retail Noise" in trading algorithms.
    
    Args:
        authoritative_content: List of authoritative texts (news headlines)
        user_comments: List of user comments (social media)
        model: Sentiment analysis model
        authoritative_weight: Weight for authoritative content (default: 3.0)
        user_weight: Weight for user comments (default: 1.0)
    
    Returns:
        Dictionary with weighted sentiment scores and breakdown
    """
    # Analyze authoritative content
    auth_sentiment = analyze_sentiment(authoritative_content, model)
    
    # Analyze user comments
    user_sentiment = analyze_sentiment(user_comments, model)
    
    # Calculate weighted scores
    weighted_positive = (
        (user_sentiment.get("positive", 0) * user_weight) +
        (auth_sentiment.get("positive", 0) * authoritative_weight)
    )
    
    weighted_negative = (
        (user_sentiment.get("negative", 0) * user_weight) +
        (auth_sentiment.get("negative", 0) * authoritative_weight)
    )
    
    weighted_neutral = (
        (user_sentiment.get("neutral", 0) * user_weight) +
        (auth_sentiment.get("neutral", 0) * authoritative_weight)
    )
    
    total_weighted = weighted_positive + weighted_negative + weighted_neutral
    
    # Calculate percentages
    if total_weighted > 0:
        positive_pct = (weighted_positive / total_weighted) * 100
        negative_pct = (weighted_negative / total_weighted) * 100
        neutral_pct = (weighted_neutral / total_weighted) * 100
    else:
        positive_pct = negative_pct = neutral_pct = 0.0
    
    # Sprint 2: Calculate combined average confidence
    auth_conf = auth_sentiment.get('avg_confidence', 0.5)
    user_conf = user_sentiment.get('avg_confidence', 0.5)
    auth_count = auth_sentiment.get('total', 0)
    user_count = user_sentiment.get('total', 0)
    
    if auth_count + user_count > 0:
        # Weighted average confidence based on sample sizes
        combined_avg_confidence = (
            (auth_conf * auth_count + user_conf * user_count) / (auth_count + user_count)
        )
    else:
        combined_avg_confidence = 0.5
    
    return {
        # Weighted counts
        "weighted_positive": weighted_positive,
        "weighted_negative": weighted_negative,
        "weighted_neutral": weighted_neutral,
        "total_weighted": total_weighted,
        
        # Percentages
        "positive_percentage": positive_pct,
        "negative_percentage": negative_pct,
        "neutral_percentage": neutral_pct,
        
        # Sprint 2: Average model confidence
        "avg_confidence": round(combined_avg_confidence, 4),
        
        # Raw counts (for reference)
        "authoritative": {
            "positive": auth_sentiment.get("positive", 0),
            "negative": auth_sentiment.get("negative", 0),
            "neutral": auth_sentiment.get("neutral", 0),
            "total": auth_sentiment.get("total", 0),
            "avg_confidence": auth_conf
        },
        "user_comments": {
            "positive": user_sentiment.get("positive", 0),
            "negative": user_sentiment.get("negative", 0),
            "neutral": user_sentiment.get("neutral", 0),
            "total": user_sentiment.get("total", 0),
            "avg_confidence": user_conf
        },
        
        # Weights used
        "weights": {
            "authoritative": authoritative_weight,
            "user": user_weight
        }
    }


def process_job(job_id: str, data_system: DataSystem, model) -> bool:
    """
    Process a single job from the queue with production hardening.
    
    Features:
    - Semantic deduplication (skip if already processed)
    - Dead Letter Queue (store failed jobs for inspection)
    - Metrics logging (track processing performance)
    - Outlier cap (prevent single source dominance)
    - Model versioning (track which model processed)
    
    Args:
        job_id: UUID of the job to process
        data_system: DataSystem instance for file operations
        model: Sentiment analysis model
    
    Returns:
        True if successful, False otherwise
    """
    start_time = time.time()
    file_path = None
    data = None
    client = None
    
    try:
        # Update status to PROCESSING
        data_system.update_job_status(job_id, 'PROCESSING')
        print(f"Processing job {job_id}...")
        
        # Get job details to find file_path
        client = get_supabase_client()
        if not client:
            print("Error: Supabase client not available")
            return False
        
        result = client.table('job_queue').select('file_path, metadata').eq('id', job_id).execute()
        
        if not result.data or len(result.data) == 0:
            print(f"Job {job_id} not found")
            return False
        
        job_data = result.data[0]
        file_path = job_data.get('file_path')
        
        if not file_path:
            print(f"No file_path found for job {job_id}")
            data_system.update_job_status(job_id, 'FAILED')
            return False
        
        # Download file from Storage
        print(f"Downloading {file_path}...")
        data = data_system.get_file_from_storage(file_path)
        
        if not data:
            print(f"Failed to download {file_path}")
            add_to_dlq(client, job_id, file_path, "Failed to download file", "NETWORK")
            data_system.update_job_status(job_id, 'FAILED')
            return False
        
        # Get content ID and type for deduplication
        content_id = get_content_id(data)
        content_type = get_content_type(data)
        alliance_name = detect_alliance(data)
        
        # Debug logging for alliance detection
        if alliance_name == "Unknown":
            meta = data.get('meta', {})
            title = meta.get('title', '')[:100] if meta.get('title') else ''
            headlines = data.get('authoritative_content', [])
            print(f"  Alliance detection: Unknown")
            print(f"    Title: {title}")
            if headlines:
                print(f"    Headlines sample: {str(headlines[:2])[:150]}")
        else:
            print(f"  Alliance detected: {alliance_name}")
        
        # Check for duplicate (semantic deduplication)
        if is_duplicate_content(client, content_id, alliance_name):
            print(f"  [WARN] Duplicate content detected ({content_id}), skipping")
            data_system.update_job_status(job_id, 'DONE')
            log_metric(client, 'duplicate_skipped', 1, {'content_type': content_type})
            return True
        
        # Extract data components (Weighted Hybrid model)
        authoritative_content = data.get('authoritative_content', [])  # News headlines (weight 3.0)
        user_comments = data.get('user_comments', [])  # Social media comments (weight 1.0)
        
        # Backward compatibility: check old 'comments' field
        if not user_comments and data.get('comments'):
            user_comments = data.get('comments', [])
        
        if not authoritative_content and not user_comments:
            print("No content found in data (neither authoritative_content nor user_comments)")
            data_system.update_job_status(job_id, 'DONE')
            return True
        
        # Get quality signals if available
        quality_signals = data.get('quality_signals', {})
        confidence_multiplier = quality_signals.get('confidence_multiplier', 0.5)
        
        # Detect location using Metadata-First strategy
        print("Detecting location...")
        detected_locations = detect_location(data)
        print(f"  Detected locations: {detected_locations}")
        
        # Detect politician mentions for constituency-level routing
        print("Detecting politicians...")
        detected_politicians = detect_politicians(data)
        politician_constituencies = [p['constituency'] for p in detected_politicians if p.get('constituency')]
        if politician_constituencies:
            print(f"  Politician-based constituencies: {politician_constituencies}")
        
        # Run weighted sentiment analysis (Weighted Hybrid model)
        print(f"Running weighted sentiment analysis...")
        print(f"  Authoritative content: {len(authoritative_content)} items (weight: 3.0)")
        print(f"  User comments: {len(user_comments)} items (weight: 1.0)")
        
        sentiment_results = compute_weighted_sentiment(
            authoritative_content=authoritative_content,
            user_comments=user_comments,
            model=model,
            authoritative_weight=3.0,
            user_weight=1.0
        )
        
        # Print results (Weighted Hybrid model output)
        print("Weighted Sentiment Analysis Results:")
        print(f"  Weighted Positive: {sentiment_results.get('weighted_positive', 0):.2f} ({sentiment_results.get('positive_percentage', 0):.1f}%)")
        print(f"  Weighted Negative: {sentiment_results.get('weighted_negative', 0):.2f} ({sentiment_results.get('negative_percentage', 0):.1f}%)")
        print(f"  Weighted Neutral: {sentiment_results.get('weighted_neutral', 0):.2f} ({sentiment_results.get('neutral_percentage', 0):.1f}%)")
        print(f"  Total Weighted Score: {sentiment_results.get('total_weighted', 0):.2f}")
        print(f"  Locations: {detected_locations}")
        
        # Breakdown by source
        print("\n  Breakdown by Source:")
        print(f"    Authoritative (raw): {sentiment_results.get('authoritative', {})}")
        print(f"    User Comments (raw): {sentiment_results.get('user_comments', {})}")
        
        print(f"\n  Alliance: {alliance_name}")
        print(f"  Confidence Multiplier: {confidence_multiplier:.2f}")
        
        # Calculate sentiment score (-1.0 to +1.0)
        sentiment_score = calculate_sentiment_score(sentiment_results)
        print(f"  Sentiment Score: {sentiment_score}")
        
        # Get average confidence from sentiment results
        avg_confidence = sentiment_results.get('avg_confidence', 0.5)
        print(f"  Avg Model Confidence: {avg_confidence:.3f}")
        
        # Persist predictions to database (with source tracking + Sprint 2 confidence)
        print("\nPersisting predictions...")
        updated_count = persist_predictions(
            client=client,
            detected_locations=detected_locations,
            alliance_name=alliance_name,
            sentiment_score=sentiment_score,
            source_id=content_id,
            model_version=MODEL_VERSION,
            avg_confidence=avg_confidence
        )
        print(f"  Updated {updated_count} constituency predictions (district-based)")
        
        # Persist politician-specific constituency predictions (higher precision)
        if politician_constituencies:
            print(f"  Routing to politician constituencies: {politician_constituencies}")
            for constituency in politician_constituencies:
                # Update specific constituency with boosted confidence (politician mention = high precision)
                boosted_confidence = min(avg_confidence * 1.2, 1.0)  # 20% confidence boost
                upsert_constituency_prediction(
                    client=client,
                    constituency_name=constituency,
                    district="POLITICIAN_ROUTED",  # Special marker for debugging
                    alliance=alliance_name,
                    new_score=sentiment_score,
                    source_id=content_id,
                    model_version=MODEL_VERSION,
                    avg_confidence=boosted_confidence
                )
            print(f"  Updated {len(politician_constituencies)} politician-routed constituencies")
        
        # Mark content as processed (for deduplication)
        mark_content_processed(
            client=client,
            content_id=content_id,
            content_type=content_type,
            alliance=alliance_name,
            file_path=file_path,
            sentiment_score=sentiment_score
        )
        
        # Update job status to DONE
        data_system.update_job_status(job_id, 'DONE')
        
        # Log metrics
        processing_time = (time.time() - start_time) * 1000  # ms
        log_metric(client, 'processing_latency_ms', processing_time, {
            'content_type': content_type,
            'alliance': alliance_name
        })
        log_metric(client, 'sentiment_score', sentiment_score, {
            'content_type': content_type,
            'alliance': alliance_name,
            'content_id': content_id
        })
        
        print(f"Job {job_id} completed successfully ({processing_time:.0f}ms)")
        
        return True
    
    except json.JSONDecodeError as e:
        error_msg = f"JSON parse error: {str(e)}"
        print(f"Error processing job {job_id}: {error_msg}")
        if client and file_path:
            add_to_dlq(client, job_id, file_path, error_msg, "JSON_PARSE", data)
        try:
            data_system.update_job_status(job_id, 'FAILED')
        except:
            pass
        return False
    
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(f"Error processing job {job_id}: {error_msg}")
        print(traceback.format_exc())
        
        # Determine error type
        error_type = "UNKNOWN"
        if "model" in str(e).lower() or "inference" in str(e).lower():
            error_type = "ML_INFERENCE"
        elif "database" in str(e).lower() or "supabase" in str(e).lower():
            error_type = "DB_ERROR"
        elif "network" in str(e).lower() or "connection" in str(e).lower():
            error_type = "NETWORK"
        
        # Add to DLQ
        if client and file_path:
            add_to_dlq(client, job_id, file_path, error_msg, error_type, data)
        
        try:
            data_system.update_job_status(job_id, 'FAILED')
        except:
            pass
        
        return False


def poll_and_process(poll_interval: int = 5, timeout_minutes: int = 3):
    """
    Main consumer loop that polls the job queue and processes jobs.
    
    Args:
        poll_interval: Seconds to wait between polls when no jobs are found
        timeout_minutes: Exit if no jobs found for this many minutes (default: 3)
    """
    print("=" * 60)
    print("Starting Sentiment Analysis Processor (Consumer)")
    print("=" * 60)
    print()
    
    # Initialize components
    try:
        data_system = DataSystem(bucket_name='raw_data')
    except RuntimeError as e:
        print(f"Error initializing DataSystem: {e}")
        return
    
    # Load configuration
    load_gazetteer()
    load_alliances()
    load_entity_map()  # For politician-to-constituency routing
    
    model = load_sentiment_model()
    if not model:
        print("Error: Could not load sentiment model")
        return
    
    print("Model loaded successfully")
    print(f"Polling job queue for PENDING jobs...")
    print(f"Timeout: Will exit if no jobs found for {timeout_minutes} minutes")
    print()
    
    client = get_supabase_client()
    if not client:
        print("Error: Supabase client not available")
        return
    
    # Track last job time for timeout
    last_job_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    # Main polling loop
    while True:
        try:
            # Check if timeout exceeded
            time_since_last_job = time.time() - last_job_time
            if time_since_last_job >= timeout_seconds:
                print(f"\n{'=' * 60}")
                print(f"Timeout: No jobs found for {timeout_minutes} minutes")
                print(f"Exiting processor gracefully...")
                print(f"{'=' * 60}")
                break
            
            # Query for PENDING jobs
            result = client.table('job_queue').select('id').eq('status', 'PENDING').limit(1).execute()
            
            if result.data and len(result.data) > 0:
                job_id = result.data[0]['id']
                process_job(job_id, data_system, model)
                last_job_time = time.time()  # Reset timeout timer
                print()
            else:
                time_remaining = timeout_seconds - time_since_last_job
                minutes_remaining = int(time_remaining / 60)
                print(f"No pending jobs found. Waiting {poll_interval} seconds... (Timeout in {minutes_remaining}m)")
                time.sleep(poll_interval)
        
        except KeyboardInterrupt:
            print("\nShutting down consumer...")
            break
        except Exception as e:
            print(f"Error in polling loop: {str(e)}")
            time.sleep(poll_interval)


if __name__ == "__main__":
    # Allow timeout to be configured via environment variable
    timeout_minutes = int(os.getenv('PROCESSOR_TIMEOUT_MINUTES', '3'))
    poll_interval = int(os.getenv('PROCESSOR_POLL_INTERVAL', '5'))
    poll_and_process(poll_interval=poll_interval, timeout_minutes=timeout_minutes)


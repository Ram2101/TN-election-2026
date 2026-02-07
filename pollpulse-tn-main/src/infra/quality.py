"""
Content Quality Pipeline for PollPulse TN.

Philosophy: "Filter garbage at the source, not downstream"
- Every low-quality item rejected saves ML inference cost
- Improves prediction accuracy with cleaner signal
- Zero additional cost (just smarter filtering)
"""

import re
import math
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any


# Video quality filter configuration
VIDEO_QUALITY_FILTERS = {
    "min_views": 1000,
    "max_age_days": 30,
    "min_duration_seconds": 60,
    "max_duration_seconds": 7200,
    "min_channel_subscribers": 500,
    "blocked_keywords": [
        "whatsapp status", "ringtone", "shorts", "compilation",
        "old speech", "throwback", "2021", "2020", "2019", "2018",
        "best moments", "tik tok", "tiktok", "reels", "memes",
        "comedy", "funny", "prank", "reaction video"
    ]
}

# Political entities for TN 2026
POLITICAL_ENTITIES = {
    'parties': [
        'dmk', 'admk', 'aiadmk', 'tvk', 'ntk', 'bjp', 'congress', 
        'vck', 'mdmk', 'pmk', 'dmdk', 'tmm', 'mnm'
    ],
    'politicians': [
        'stalin', 'udhayanidhi', 'kanimozhi', 'duraimurugan',
        'edappadi', 'eps', 'ops', 'palaniswami', 'panneerselvam',
        'vijay', 'thalapathy', 'tvk vijay',
        'seeman', 'naam tamilar',
        'annamalai', 'tamilisai',
        'thirumavalavan', 'thiruma',
        'kamal haasan', 'vaiko', 'premalatha'
    ],
    'election': [
        'election', 'vote', 'voting', 'campaign', 'rally', 'manifesto', 
        'seat', 'constituency', 'mla', 'minister', 'chief minister',
        'assembly', 'polling', 'ballot', 'candidate'
    ],
    'locations': [
        'tamil nadu', 'tamilnadu', 'chennai', 'madurai', 'coimbatore', 
        'trichy', 'salem', 'tirunelveli', 'erode', 'vellore'
    ]
}

# Spam patterns
SPAM_PATTERNS = [
    r'(.)\1{4,}',
    r'^[A-Z\s!?]{20,}$',
    r'https?://[^\s]+',
    r'@\w+\s*@\w+\s*@\w+',
    r'(subscribe|channel|check out|link in bio)',
    r'[🔥💯👍🙏❤️😍🎉]{3,}',
    r'(earn|win|prize|free|click here)',
    r'^[\d\s\.\,]+$',
]

SPAM_REGEX = re.compile('|'.join(SPAM_PATTERNS), re.IGNORECASE)

MIN_COMMENT_QUALITY = 0.4
MIN_RELEVANCE_SCORE = 0.3


def passes_video_quality_filter(video: Dict) -> Tuple[bool, str]:
    """Check if video passes quality filters BEFORE scraping."""
    title = (video.get('title', '') or '').lower()
    description = (video.get('description', '') or '').lower()
    
    for blocked in VIDEO_QUALITY_FILTERS['blocked_keywords']:
        if blocked.lower() in title:
            return False, f"Blocked keyword: '{blocked}'"
        if blocked.lower() in description[:500]:
            return False, f"Blocked keyword in description: '{blocked}'"
    
    views = video.get('view_count', 0) or 0
    if views > 0 and views < VIDEO_QUALITY_FILTERS['min_views']:
        return False, f"Low views: {views}"
    
    duration = video.get('duration', 0) or 0
    if duration > 0:
        if duration < VIDEO_QUALITY_FILTERS['min_duration_seconds']:
            return False, f"Too short: {duration}s"
        if duration > VIDEO_QUALITY_FILTERS['max_duration_seconds']:
            return False, f"Too long: {duration}s"
    
    return True, "PASSED"


def get_video_quality_score(video: Dict) -> float:
    """Calculate a quality score for the video (0.0 to 1.0)."""
    score = 0.5
    
    views = video.get('view_count', 0) or 0
    if views >= 1000000:
        score += 0.2
    elif views >= 100000:
        score += 0.15
    elif views >= 10000:
        score += 0.1
    elif views >= 1000:
        score += 0.05
    
    subscribers = video.get('channel_follower_count', 0) or 0
    if subscribers >= 1000000:
        score += 0.15
    elif subscribers >= 100000:
        score += 0.1
    elif subscribers >= 10000:
        score += 0.05
    
    if video.get('channel_is_verified', False):
        score += 0.1
    
    return min(score, 1.0)


def score_comment_quality(comment: Dict) -> Tuple[float, str]:
    """Score comment quality from 0.0 to 1.5."""
    text = (comment.get('text', '') or '').strip()
    
    if len(text) < 10:
        return 0.0, "Too short"
    
    if SPAM_REGEX.search(text):
        return 0.0, "Spam pattern detected"
    
    alphanumeric = sum(c.isalnum() or c.isspace() for c in text)
    if len(text) > 0 and alphanumeric / len(text) < 0.5:
        return 0.0, "Too many special characters"
    
    score = 1.0
    
    if len(text) > 200:
        score *= 1.3
    elif len(text) > 100:
        score *= 1.2
    elif len(text) > 50:
        score *= 1.1
    elif len(text) < 30:
        score *= 0.7
    
    likes = comment.get('likes', 0) or comment.get('like_count', 0) or 0
    if likes >= 1000:
        score *= 1.5
    elif likes >= 100:
        score *= 1.3
    elif likes >= 10:
        score *= 1.2
    
    text_lower = text.lower()
    political_terms = POLITICAL_ENTITIES['parties'] + POLITICAL_ENTITIES['politicians'] + POLITICAL_ENTITIES['election']
    relevance_hits = sum(1 for term in political_terms if term in text_lower)
    
    if relevance_hits >= 3:
        score *= 1.4
    elif relevance_hits >= 2:
        score *= 1.3
    elif relevance_hits >= 1:
        score *= 1.1
    
    return min(score, 1.5), "OK"


def filter_quality_comments(comments: List[Dict]) -> Tuple[List[Dict], int, float]:
    """Filter comments by quality score."""
    quality_comments = []
    rejected_count = 0
    total_score = 0.0
    
    for comment in comments:
        score, reason = score_comment_quality(comment)
        
        if score >= MIN_COMMENT_QUALITY:
            comment['quality_score'] = round(score, 3)
            quality_comments.append(comment)
            total_score += score
        else:
            rejected_count += 1
    
    avg_quality = total_score / len(quality_comments) if quality_comments else 0.0
    
    return quality_comments, rejected_count, round(avg_quality, 3)


def get_engagement_weight(likes: int) -> float:
    """Calculate engagement weight using logarithmic scale."""
    return 1 + math.log10(1 + likes)


def calculate_political_relevance(data: Dict) -> Tuple[float, Dict[str, List[str]]]:
    """Score political relevance of content (0.0 to 1.0)."""
    meta = data.get('meta', {})
    text_sources = [
        meta.get('title', '') or '',
        (meta.get('description', '') or '')[:1000],
    ]
    
    transcript = data.get('transcript', '') or ''
    if transcript:
        text_sources.append(transcript[:2000])
    
    combined_text = ' '.join(text_sources).lower()
    
    matches: Dict[str, List[str]] = {
        'parties': [],
        'politicians': [],
        'election': [],
        'locations': []
    }
    
    for category, terms in POLITICAL_ENTITIES.items():
        for term in terms:
            if term in combined_text and term not in matches[category]:
                matches[category].append(term)
    
    score = (
        len(matches['parties']) * 0.3 +
        len(matches['politicians']) * 0.4 +
        len(matches['election']) * 0.2 +
        len(matches['locations']) * 0.1
    )
    
    normalized_score = min(score / 2.0, 1.0)
    
    return round(normalized_score, 3), matches


def should_process_content(data: Dict) -> Tuple[bool, str, float]:
    """Final gate: should this content enter the ML pipeline?"""
    score, matches = calculate_political_relevance(data)
    
    if score < MIN_RELEVANCE_SCORE:
        matched_terms = sum(len(v) for v in matches.values())
        return False, f"Low relevance ({score:.2f}): only {matched_terms} political terms found", score
    
    matched_summary = []
    for category, terms in matches.items():
        if terms:
            matched_summary.append(f"{category}: {terms[:3]}")
    
    return True, f"Relevant ({score:.2f}): {', '.join(matched_summary)}", score


def calculate_source_confidence(
    has_transcript: bool,
    video_views: int,
    relevance_score: float,
    avg_comment_quality: float,
    channel_verified: bool = False
) -> float:
    """Calculate overall source confidence multiplier (0.3 to 1.0)."""
    confidence = 0.4
    
    if has_transcript:
        confidence += 0.2
    
    if video_views >= 1000000:
        confidence += 0.15
    elif video_views >= 100000:
        confidence += 0.12
    elif video_views >= 10000:
        confidence += 0.08
    elif video_views >= 1000:
        confidence += 0.05
    
    confidence += relevance_score * 0.1
    
    if avg_comment_quality >= 1.0:
        confidence += 0.1
    elif avg_comment_quality >= 0.7:
        confidence += 0.05
    
    if channel_verified:
        confidence += 0.05
    
    return min(round(confidence, 3), 1.0)


def build_quality_signals(
    video_data: Dict,
    transcript_text: Optional[str],
    quality_comments: List[Dict],
    rejected_count: int,
    avg_comment_quality: float,
    relevance_score: float
) -> Dict[str, Any]:
    """Build quality_signals dict for payload."""
    views = video_data.get('view_count', 0) or 0
    duration = video_data.get('duration', 0) or 0
    subscribers = video_data.get('channel_follower_count', 0) or 0
    verified = video_data.get('channel_is_verified', False)
    
    age_days = 0
    upload_date = video_data.get('upload_date', '')
    if upload_date and len(upload_date) == 8:
        try:
            upload_dt = datetime.strptime(upload_date, '%Y%m%d')
            age_days = (datetime.now() - upload_dt).days
        except ValueError:
            pass
    
    confidence = calculate_source_confidence(
        has_transcript=bool(transcript_text),
        video_views=views,
        relevance_score=relevance_score,
        avg_comment_quality=avg_comment_quality,
        channel_verified=verified
    )
    
    return {
        "has_transcript": bool(transcript_text),
        "transcript_length": len(transcript_text) if transcript_text else 0,
        "video_views": views,
        "video_age_days": age_days,
        "video_duration_seconds": duration,
        "channel_subscribers": subscribers,
        "channel_verified": verified,
        "relevance_score": relevance_score,
        "avg_comment_quality": avg_comment_quality,
        "comments_kept": len(quality_comments),
        "comments_rejected": rejected_count,
        "confidence_multiplier": confidence
    }


def generate_content_id(source_type: str, identifier: str) -> str:
    """Generate a unique content ID for deduplication."""
    if source_type == 'youtube':
        return identifier
    else:
        return hashlib.md5(identifier.encode()).hexdigest()[:16]

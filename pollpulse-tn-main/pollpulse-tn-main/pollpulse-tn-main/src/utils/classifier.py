"""
Alliance Classification Utility

Production-grade classification using Zero-Shot Learning.
Uses pre-trained transformer models to classify content into political alliances without training data.

Approach:
1. Zero-Shot Classification: Use multilingual NLI models to classify into custom categories
2. Entity Extraction: Extract political entities as supporting signals (fallback)
3. Confidence Thresholding: Only classify if confidence exceeds threshold

Benefits:
- No training data required (zero-shot)
- Handles Tamil-English mixed content
- Understands context and semantics
- Production-ready and maintainable
- Can be fine-tuned later if labeled data becomes available
"""

import json
import os
import re
from typing import Dict, Optional, List, Tuple
from pathlib import Path
import warnings

# Suppress transformers warnings
warnings.filterwarnings('ignore', category=UserWarning)

# Try to import transformers for zero-shot classification
try:
    from transformers import pipeline
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
    print("Warning: transformers not available. Using keyword-based classification.")

CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

# Global model cache
_zero_shot_classifier = None

# Alliance labels for zero-shot classification (descriptive labels work better)
ALLIANCE_LABELS = [
    "DMK Front alliance led by MK Stalin, includes Congress, VCK, CPI, CPM, MDMK",
    "ADMK Front alliance led by Edappadi Palaniswami, includes BJP, PMK, DMDK",
    "TVK Front alliance led by Thalapathy Vijay, Tamizhaga Vetri Kazhagam",
    "NTK Naam Tamilar Katchi led by Seeman"
]

# Mapping from label text to alliance name
LABEL_TO_ALLIANCE = {
    "DMK Front": "DMK_Front",
    "ADMK Front": "ADMK_Front",
    "TVK Front": "TVK_Front",
    "NTK": "NTK"
}


def get_zero_shot_classifier():
    """Lazy load zero-shot classification model."""
    global _zero_shot_classifier
    if _zero_shot_classifier is None and TRANSFORMERS_AVAILABLE:
        try:
            # Use multilingual zero-shot model that works with Tamil-English mixed content
            # MoritzLaurer models are specifically designed for zero-shot classification
            _zero_shot_classifier = pipeline(
                "zero-shot-classification",
                model="MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7",
                device=-1  # Use CPU (set to 0 for GPU if available)
            )
        except Exception as e:
            print(f"Warning: Could not load multilingual zero-shot classifier: {e}")
            # Fallback to English-only model
            try:
                _zero_shot_classifier = pipeline(
                    "zero-shot-classification",
                    model="facebook/bart-large-mnli",
                    device=-1
                )
            except Exception as e2:
                print(f"Warning: Could not load fallback classifier: {e2}")
                return None
    return _zero_shot_classifier


def extract_political_entities(text: str) -> Dict[str, List[str]]:
    """
    Extract political entities from text using pattern matching (fallback method).
    
    Returns:
        Dict with keys: 'parties', 'leaders'
    """
    entities = {
        'parties': [],
        'leaders': []
    }
    
    # Party acronyms and names
    party_patterns = {
        'DMK': r'\bdmk\b',
        'ADMK': r'\badmk\b|\baiadmk\b',
        'BJP': r'\bbjp\b',
        'TVK': r'\btvk\b',
        'NTK': r'\bntk\b',
        'PMK': r'\bpmk\b',
        'VCK': r'\bvck\b',
        'MDMK': r'\bmdmk\b',
        'CPI': r'\bcpi\b',
        'CPM': r'\bcpm\b',
        'Congress': r'\bcongress\b|\binc\b',
    }
    
    # Leader names
    leader_patterns = {
        'Stalin': r'\bstalin\b',
        'Udhayanidhi': r'\budhayanidhi\b',
        'EPS': r'\beps\b|\bedappadi\b',
        'OPS': r'\bops\b',
        'Vijay': r'\bvijay\b|\bthalapathy\b',
        'Seeman': r'\bseeman\b',
        'Thirumavalavan': r'\bthirumavalavan\b',
        'Vaiko': r'\bvaiko\b',
        'Annamalai': r'\bannamalai\b',
    }
    
    text_lower = text.lower()
    
    for party, pattern in party_patterns.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            entities['parties'].append(party)
    
    for leader, pattern in leader_patterns.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            entities['leaders'].append(leader)
    
    return entities


def classify_with_entities(text: str) -> Optional[str]:
    """
    Fallback classification using entity extraction.
    Used when zero-shot classifier is not available.
    """
    entities = extract_political_entities(text)
    
    # Entity-to-alliance mapping
    entity_alliance_map = {
        'DMK': 'DMK_Front',
        'Congress': 'DMK_Front',
        'VCK': 'DMK_Front',
        'MDMK': 'DMK_Front',
        'CPI': 'DMK_Front',
        'CPM': 'DMK_Front',
        'ADMK': 'ADMK_Front',
        'BJP': 'ADMK_Front',
        'PMK': 'ADMK_Front',
        'TVK': 'TVK_Front',
        'NTK': 'NTK',
        'Stalin': 'DMK_Front',
        'Udhayanidhi': 'DMK_Front',
        'Thirumavalavan': 'DMK_Front',
        'Vaiko': 'DMK_Front',
        'EPS': 'ADMK_Front',
        'OPS': 'ADMK_Front',
        'Vijay': 'TVK_Front',
        'Seeman': 'NTK',
        'Annamalai': 'ADMK_Front',
    }
    
    alliance_counts = {}
    for party in entities['parties']:
        alliance = entity_alliance_map.get(party)
        if alliance:
            alliance_counts[alliance] = alliance_counts.get(alliance, 0) + 1
    
    for leader in entities['leaders']:
        alliance = entity_alliance_map.get(leader)
        if alliance:
            alliance_counts[alliance] = alliance_counts.get(alliance, 0) + 2  # Leaders weighted higher
    
    if alliance_counts:
        return max(alliance_counts.items(), key=lambda x: x[1])[0]
    
    return None


def load_alliances() -> Dict:
    """Load alliance keywords from config file."""
    config_path = CONFIG_DIR / "alliances.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Alliance config not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def classify_alliance(data: Dict, producer_alliance: Optional[str] = None) -> str:
    """
    Classify content into a political alliance using zero-shot classification.
    
    This is the production-grade approach that uses pre-trained models
    to understand context and semantics without requiring training data.
    
    Args:
        data: Structured data payload with meta, authoritative_content, user_comments
        producer_alliance: Optional alliance already detected by producer
    
    Returns:
        Alliance name (e.g., "DMK_Front") or "Unknown"
    """
    # Load alliances config
    alliances = load_alliances()
    keywords_map = alliances.get('keywords', {})
    valid_alliances = set(keywords_map.keys())
    
    # Priority 1: Use producer alliance if valid
    if producer_alliance and producer_alliance != 'Unknown' and producer_alliance in valid_alliances:
        return producer_alliance
    
    # Priority 2: Extract text from all content sources
    meta = data.get('meta', {})
    title = meta.get('title', '') or ''
    description = meta.get('description', '') or ''
    
    # News headlines (authoritative_content)
    headlines = []
    authoritative_content = data.get('authoritative_content', [])
    if authoritative_content:
        if isinstance(authoritative_content, list):
            headlines = [str(item) for item in authoritative_content]
        else:
            headlines = [str(authoritative_content)]
    
    # User comments (limited to avoid noise)
    comments = []
    user_comments = data.get('user_comments', [])
    if user_comments:
        if isinstance(user_comments, list):
            for comment in user_comments[:10]:  # Limit to top 10 comments
                if isinstance(comment, dict):
                    comments.append(comment.get('text', ''))
                else:
                    comments.append(str(comment))
        else:
            comments = [str(user_comments)]
    
    # Combine text with priority weighting
    # Title and headlines are most important
    primary_text = f"{title} {' '.join(headlines)}"
    secondary_text = description
    tertiary_text = ' '.join(comments)
    
    # Use primary text for classification (most reliable signal)
    text_to_classify = primary_text.strip()
    if not text_to_classify:
        text_to_classify = secondary_text.strip()
    if not text_to_classify:
        text_to_classify = tertiary_text.strip()
    
    if not text_to_classify or len(text_to_classify) < 10:
        return "Unknown"
    
    # Try zero-shot classification first
    classifier = get_zero_shot_classifier()
    if classifier:
        try:
            # Zero-shot classification
            result = classifier(text_to_classify, ALLIANCE_LABELS)
            
            # Extract top prediction
            if result['labels'] and result['scores']:
                top_label = result['labels'][0]
                top_score = result['scores'][0]
                
                # Map label to alliance name
                for label_key, alliance_name in LABEL_TO_ALLIANCE.items():
                    if label_key in top_label:
                        # Require minimum confidence (0.3 for zero-shot)
                        if top_score >= 0.3:
                            return alliance_name
                        break
            
            # If zero-shot didn't find a match, fall back to entity extraction
            entity_result = classify_with_entities(text_to_classify)
            if entity_result:
                return entity_result
                
        except Exception as e:
            print(f"Error in zero-shot classification: {e}")
            # Fall back to entity extraction
            entity_result = classify_with_entities(text_to_classify)
            if entity_result:
                return entity_result
    else:
        # No classifier available, use entity extraction
        entity_result = classify_with_entities(text_to_classify)
        if entity_result:
            return entity_result
    
    return "Unknown"


def should_process_content(data: Dict, min_alliance_confidence: int = 1) -> tuple[bool, str]:
    """
    Determine if content should be processed based on alliance classification.
    
    Args:
        data: Structured data payload
        min_alliance_confidence: Minimum score threshold for processing
    
    Returns:
        Tuple of (should_process: bool, alliance: str)
    """
    alliance = classify_alliance(data)
    
    # Skip "Unknown" alliance content (generic/non-political)
    if alliance == "Unknown":
        return False, "Unknown"
    
    return True, alliance

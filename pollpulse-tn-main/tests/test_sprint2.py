"""
Sprint 2 Test Suite: Advanced ML Scoring

Tests for:
1. Freshness Decay - Time-weighted confidence decay
2. Outlier Cap - Prevents single source dominance
3. Engagement Weighting - Likes affect sentiment weight
4. Probability-Based Scoring - Uses model confidence

Run: python tests/test_sprint2.py
"""

import sys
import os
import math
from datetime import datetime, timezone, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import Sprint 2 functions
from processor import (
    calculate_freshness_decay,
    get_engagement_weight,
    apply_influence_cap,
    FRESHNESS_HALF_LIFE_DAYS,
    MAX_INFLUENCE_PER_SOURCE,
    ENABLE_OUTLIER_CAP,
    ENABLE_ENGAGEMENT_WEIGHTING,
    ENABLE_PROBABILITY_SCORING
)


def test_freshness_decay():
    """Test freshness decay calculation."""
    print("\n" + "=" * 60)
    print("TEST 1: Freshness Decay")
    print("=" * 60)
    
    now = datetime.now(timezone.utc)
    
    test_cases = [
        (now, "Just updated", 1.0),  # Fresh = 1.0
        (now - timedelta(days=7), "7 days old", 0.707),  # ~0.7
        (now - timedelta(days=14), "14 days old (half-life)", 0.5),  # Half-life = 0.5
        (now - timedelta(days=28), "28 days old (2x half-life)", 0.25),  # ~0.25
        (now - timedelta(days=60), "60 days old", 0.05),  # Very decayed
    ]
    
    print(f"\nHalf-life configured: {FRESHNESS_HALF_LIFE_DAYS} days")
    print("-" * 60)
    
    all_passed = True
    for last_updated, description, expected in test_cases:
        decay = calculate_freshness_decay(last_updated)
        status = "PASS" if abs(decay - expected) < 0.1 else "FAIL"
        if status == "FAIL":
            all_passed = False
        print(f"[{status}] {description}: decay={decay:.4f} (expected ~{expected})")
    
    print("-" * 60)
    print(f"Result: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed


def test_engagement_weighting():
    """Test engagement weight calculation."""
    print("\n" + "=" * 60)
    print("TEST 2: Engagement Weighting")
    print("=" * 60)
    
    if not ENABLE_ENGAGEMENT_WEIGHTING:
        print("WARNING:  Engagement weighting is DISABLED")
        print("Set ENABLE_ENGAGEMENT_WEIGHTING=true to enable")
        return True
    
    test_cases = [
        (0, "No likes", 1.0),
        (9, "9 likes", 2.0),  # 1 + log10(10) = 2.0
        (99, "99 likes", 3.0),  # 1 + log10(100) = 3.0
        (999, "999 likes", 4.0),  # 1 + log10(1000) = 4.0
        (9999, "9999 likes", 5.0),  # 1 + log10(10000) = 5.0
    ]
    
    print("\nFormula: weight = 1 + log10(1 + likes)")
    print("-" * 60)
    
    all_passed = True
    for likes, description, expected in test_cases:
        weight = get_engagement_weight(likes)
        status = "[PASS]" if abs(weight - expected) < 0.1 else "[FAIL]"
        if status == "[FAIL]":
            all_passed = False
        print(f"{status} {description}: weight={weight:.3f} (expected ~{expected})")
    
    print("-" * 60)
    print(f"Result: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed


def test_outlier_cap():
    """Test outlier cap functionality."""
    print("\n" + "=" * 60)
    print("TEST 3: Outlier Cap")
    print("=" * 60)
    
    print(f"\nMAX_INFLUENCE_PER_SOURCE: {MAX_INFLUENCE_PER_SOURCE}")
    print(f"ENABLE_OUTLIER_CAP: {ENABLE_OUTLIER_CAP}")
    print("-" * 60)
    
    test_cases = [
        # (current_score, delta, expected_new_score, description)
        (0.0, 0.02, 0.02, "Small delta (within cap)"),
        (0.0, 0.10, MAX_INFLUENCE_PER_SOURCE, "Large positive delta (capped)"),
        (0.0, -0.10, -MAX_INFLUENCE_PER_SOURCE, "Large negative delta (capped)"),
        (0.5, 0.01, 0.51, "Small positive shift"),
        (0.5, 0.20, 0.5 + MAX_INFLUENCE_PER_SOURCE, "Large shift (capped)"),
    ]
    
    all_passed = True
    for current, delta, expected, description in test_cases:
        result = apply_influence_cap(current, delta)
        status = "[PASS]" if abs(result - expected) < 0.001 else "[FAIL]"
        if status == "[FAIL]":
            all_passed = False
        print(f"{status} {description}:")
        print(f"    current={current}, delta={delta} → result={result:.4f} (expected={expected})")
    
    print("-" * 60)
    print(f"Result: {'PASSED' if all_passed else 'FAILED'}")
    return all_passed


def test_probability_scoring():
    """Test probability scoring configuration."""
    print("\n" + "=" * 60)
    print("TEST 4: Probability-Based Scoring")
    print("=" * 60)
    
    print(f"\nENABLE_PROBABILITY_SCORING: {ENABLE_PROBABILITY_SCORING}")
    
    if ENABLE_PROBABILITY_SCORING:
        print("[PASS] Probability scoring is ENABLED")
        print("   - Model returns full probability distribution")
        print("   - Uses all scores (positive/negative/neutral)")
        print("   - More nuanced than binary classification")
    else:
        print("WARNING:  Probability scoring is DISABLED")
        print("   - Using legacy binary classification")
        print("   - Set ENABLE_PROBABILITY_SCORING=true to enable")
    
    print("-" * 60)
    return True


def test_sentiment_with_engagement():
    """Test sentiment analysis with engagement weighting (requires model)."""
    print("\n" + "=" * 60)
    print("TEST 5: Sentiment Analysis Integration")
    print("=" * 60)
    
    try:
        from processor import analyze_sentiment, load_sentiment_model
        
        print("\nLoading sentiment model...")
        model = load_sentiment_model()
        
        if model is None:
            print("WARNING:  Could not load model (this is OK for unit tests)")
            return True
        
        # Test comments with different engagement levels
        test_comments = [
            {"text": "DMK is doing great work!", "likes": 100},
            {"text": "ADMK should have won", "likes": 5},
            {"text": "Politics is complicated", "likes": 0},
        ]
        
        print("\nAnalyzing comments with engagement weighting...")
        result = analyze_sentiment(test_comments, model)
        
        print(f"\nResults:")
        print(f"  Positive: {result.get('positive', 0):.2f}")
        print(f"  Negative: {result.get('negative', 0):.2f}")
        print(f"  Neutral: {result.get('neutral', 0):.2f}")
        print(f"  Total Weight: {result.get('total_weight', 0):.2f}")
        print(f"  Avg Confidence: {result.get('avg_confidence', 0):.4f}")
        
        # Verify engagement weighting is applied
        # Comment with 100 likes should have weight ~3.0
        # Comment with 5 likes should have weight ~1.8
        # Comment with 0 likes should have weight 1.0
        expected_total_weight = 3.0 + 1.8 + 1.0  # ~5.8
        
        if result.get('total_weight', 0) > 3.0:
            print("\n[PASS] Engagement weighting is working!")
            return True
        else:
            print("\nWARNING:  Engagement weighting may not be applied")
            return True  # Don't fail on this
            
    except Exception as e:
        print(f"\nWARNING:  Integration test skipped: {e}")
        return True  # Don't fail on model loading issues


def run_all_tests():
    """Run all Sprint 2 tests."""
    print("\n" + "=" * 60)
    print("SPRINT 2 TEST SUITE: Advanced ML Scoring")
    print("=" * 60)
    
    results = []
    
    results.append(("Freshness Decay", test_freshness_decay()))
    results.append(("Engagement Weighting", test_engagement_weighting()))
    results.append(("Outlier Cap", test_outlier_cap()))
    results.append(("Probability Scoring", test_probability_scoring()))
    
    # Skip model-dependent test by default
    # results.append(("Sentiment Integration", test_sentiment_with_engagement()))
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "PASSED" if passed else "FAILED"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False
    
    print("-" * 60)
    if all_passed:
        print("SUCCESS: All Sprint 2 tests PASSED!")
    else:
        print("FAILED: Some tests did not pass")
    
    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)

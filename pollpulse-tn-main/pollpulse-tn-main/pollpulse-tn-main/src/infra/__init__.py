"""
Infrastructure layer for PollPulse TN ETL pipeline.

Provides:
- Supabase client and data management utilities
- Production resilience patterns (Circuit Breaker, Rate Limiter)
- Content quality filtering and scoring
"""

from .client import get_supabase_client
from .data_manager import DataSystem
from .resilience import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    AdaptiveRateLimiter,
    BackpressureMonitor,
    RetryWithBackoff,
    get_circuit_breaker,
    get_rate_limiter
)
from .quality import (
    passes_video_quality_filter,
    get_video_quality_score,
    score_comment_quality,
    filter_quality_comments,
    calculate_political_relevance,
    should_process_content,
    calculate_source_confidence,
    build_quality_signals,
    get_engagement_weight
)

__all__ = [
    # Client
    'get_supabase_client', 
    'DataSystem',
    # Resilience
    'CircuitBreaker',
    'CircuitBreakerOpenError',
    'AdaptiveRateLimiter',
    'BackpressureMonitor',
    'RetryWithBackoff',
    'get_circuit_breaker',
    'get_rate_limiter',
    # Quality
    'passes_video_quality_filter',
    'get_video_quality_score',
    'score_comment_quality',
    'filter_quality_comments',
    'calculate_political_relevance',
    'should_process_content',
    'calculate_source_confidence',
    'build_quality_signals',
    'get_engagement_weight'
]


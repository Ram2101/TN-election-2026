"""
Production Resilience Utilities for PollPulse TN.

This module provides production-grade resilience patterns:
- CircuitBreaker: Prevents cascading failures
- AdaptiveRateLimiter: Handles external API rate limits
- BackpressureMonitor: Detects queue overflow

These patterns ensure the pipeline degrades gracefully under failure conditions.
"""

import time
from typing import Optional, Callable, Any
from functools import wraps


class CircuitBreaker:
    """
    Circuit Breaker pattern to prevent cascading failures.
    
    States:
    - CLOSED: Normal operation, requests flow through
    - OPEN: Too many failures, requests are blocked
    - HALF_OPEN: Testing if service recovered
    """
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 300):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time: Optional[float] = None
        self.state = "CLOSED"
        self.success_count_in_half_open = 0
        self.half_open_success_threshold = 2
    
    def can_execute(self) -> bool:
        """Check if a request can be executed."""
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if self.last_failure_time and (time.time() - self.last_failure_time > self.reset_timeout):
                self.state = "HALF_OPEN"
                self.success_count_in_half_open = 0
                print(f"Circuit breaker: OPEN -> HALF_OPEN (testing recovery)")
                return True
            return False
        else:
            return True
    
    def record_success(self):
        """Record a successful operation."""
        if self.state == "HALF_OPEN":
            self.success_count_in_half_open += 1
            if self.success_count_in_half_open >= self.half_open_success_threshold:
                self.state = "CLOSED"
                self.failures = 0
                print(f"Circuit breaker: HALF_OPEN -> CLOSED (recovered)")
        else:
            self.failures = 0
    
    def record_failure(self):
        """Record a failed operation."""
        self.failures += 1
        self.last_failure_time = time.time()
        
        if self.state == "HALF_OPEN":
            self.state = "OPEN"
            print(f"Circuit breaker: HALF_OPEN -> OPEN (recovery failed)")
        elif self.failures >= self.failure_threshold:
            self.state = "OPEN"
            print(f"Circuit breaker: CLOSED -> OPEN (threshold reached: {self.failures} failures)")
    
    def get_state(self) -> str:
        return self.state
    
    def reset(self):
        self.failures = 0
        self.state = "CLOSED"
        self.last_failure_time = None


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open and blocking requests."""
    pass


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts delay based on response codes.
    """
    
    def __init__(self, base_delay: float = 2.0, max_delay: float = 60.0):
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.max_delay = max_delay
        self.consecutive_successes = 0
        self.success_threshold_for_decrease = 5
    
    def handle_response(self, status_code: int):
        """Adjust delay based on response status code."""
        if status_code == 429:
            self.current_delay = min(self.current_delay * 2, self.max_delay)
            self.consecutive_successes = 0
            print(f"Rate limited (429)! Delay increased to {self.current_delay:.1f}s")
        elif status_code == 503:
            self.current_delay = min(self.current_delay * 1.5, self.max_delay)
            self.consecutive_successes = 0
        elif 200 <= status_code < 300:
            self.consecutive_successes += 1
            if self.consecutive_successes >= self.success_threshold_for_decrease:
                self.current_delay = max(self.base_delay, self.current_delay * 0.9)
                self.consecutive_successes = 0
    
    def wait(self):
        """Wait for the current delay period."""
        time.sleep(self.current_delay)
    
    def get_delay(self) -> float:
        return self.current_delay
    
    def reset(self):
        self.current_delay = self.base_delay
        self.consecutive_successes = 0


class BackpressureMonitor:
    """
    Monitor queue depth and detect backpressure conditions.
    """
    
    def __init__(self, client, max_queue_depth: int = 500):
        self.client = client
        self.max_queue_depth = max_queue_depth
        self._last_check_time: Optional[float] = None
        self._last_depth: int = 0
        self._cache_ttl = 30
    
    def get_queue_depth(self) -> int:
        """Get current number of PENDING jobs in queue."""
        try:
            result = self.client.table('job_queue').select(
                'id', count='exact'
            ).eq('status', 'PENDING').execute()
            
            self._last_depth = result.count or 0
            self._last_check_time = time.time()
            return self._last_depth
        except Exception as e:
            print(f"Error checking queue depth: {e}")
            return self._last_depth
    
    def is_overloaded(self, force_check: bool = False) -> bool:
        """Check if system is experiencing backpressure."""
        if not force_check and self._last_check_time:
            if time.time() - self._last_check_time < self._cache_ttl:
                return self._last_depth > self.max_queue_depth
        
        depth = self.get_queue_depth()
        is_overloaded = depth > self.max_queue_depth
        
        if is_overloaded:
            print(f"BACKPRESSURE: Queue depth {depth} > {self.max_queue_depth}")
        
        return is_overloaded
    
    def get_status(self) -> dict:
        depth = self.get_queue_depth()
        return {
            "queue_depth": depth,
            "max_depth": self.max_queue_depth,
            "utilization_pct": (depth / self.max_queue_depth) * 100,
            "is_overloaded": depth > self.max_queue_depth
        }


class RetryWithBackoff:
    """Retry helper with exponential backoff."""
    
    def __init__(
        self, 
        max_retries: int = 3, 
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        exponential_base: float = 2.0
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retries."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = min(
                        self.base_delay * (self.exponential_base ** attempt),
                        self.max_delay
                    )
                    print(f"Retry {attempt + 1}/{self.max_retries} after {delay:.1f}s: {e}")
                    time.sleep(delay)
        
        raise last_exception


# Singleton instances
_default_circuit_breaker: Optional[CircuitBreaker] = None
_default_rate_limiter: Optional[AdaptiveRateLimiter] = None


def get_circuit_breaker() -> CircuitBreaker:
    """Get or create default circuit breaker instance."""
    global _default_circuit_breaker
    if _default_circuit_breaker is None:
        _default_circuit_breaker = CircuitBreaker(failure_threshold=5, reset_timeout=300)
    return _default_circuit_breaker


def get_rate_limiter() -> AdaptiveRateLimiter:
    """Get or create default rate limiter instance."""
    global _default_rate_limiter
    if _default_rate_limiter is None:
        _default_rate_limiter = AdaptiveRateLimiter(base_delay=2.0, max_delay=60.0)
    return _default_rate_limiter

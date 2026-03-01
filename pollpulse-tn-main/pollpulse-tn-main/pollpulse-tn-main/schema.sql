-- ============================================
-- PollPulse TN - Production Database Schema
-- ============================================
-- Run this in Supabase SQL Editor to set up
-- all required tables for the prediction engine.
-- ============================================

-- ============================================
-- CORE PREDICTION TABLE (Enhanced)
-- ============================================
-- Stores sentiment predictions per constituency per alliance
-- Uses moving average for score updates

CREATE TABLE IF NOT EXISTS constituency_predictions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    constituency_name TEXT NOT NULL,
    district TEXT NOT NULL,
    alliance TEXT NOT NULL,
    sentiment_score FLOAT DEFAULT 0.0,
    confidence_weight FLOAT DEFAULT 0.5,
    model_version TEXT DEFAULT 'xlm-roberta-sentiment-v1',
    source_ids TEXT[] DEFAULT '{}',           -- Data lineage tracking
    source_count INT DEFAULT 0,               -- Number of sources contributing
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(constituency_name, alliance)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_predictions_district ON constituency_predictions(district);
CREATE INDEX IF NOT EXISTS idx_predictions_alliance ON constituency_predictions(alliance);
CREATE INDEX IF NOT EXISTS idx_predictions_updated ON constituency_predictions(last_updated);

-- ============================================
-- SEMANTIC DEDUPLICATION (Enhanced Idempotency)
-- ============================================
-- Tracks processed content by content_id (video_id or URL hash)
-- Prevents same video from being counted multiple times

CREATE TABLE IF NOT EXISTS processed_content (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id TEXT NOT NULL,                 -- video_id or md5(news_url)
    content_type TEXT NOT NULL,               -- 'youtube' or 'news'
    alliance TEXT NOT NULL,
    file_path TEXT,
    sentiment_score FLOAT,                    -- Store for audit trail
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(content_id, alliance)
);

CREATE INDEX IF NOT EXISTS idx_processed_content_id ON processed_content(content_id);
CREATE INDEX IF NOT EXISTS idx_processed_content_type ON processed_content(content_type);

-- ============================================
-- DEAD LETTER QUEUE
-- ============================================
-- Failed jobs are moved here for inspection and retry
-- Prevents pipeline crashes from losing data

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_job_id UUID,
    file_path TEXT,
    error_message TEXT,
    error_type TEXT,                          -- 'JSON_PARSE', 'ML_INFERENCE', 'DB_ERROR', 'NETWORK'
    payload JSONB,
    failed_at TIMESTAMPTZ DEFAULT NOW(),
    retry_count INT DEFAULT 0,
    last_retry_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ                   -- When manually resolved
);

CREATE INDEX IF NOT EXISTS idx_dlq_error_type ON dead_letter_queue(error_type);
CREATE INDEX IF NOT EXISTS idx_dlq_retry ON dead_letter_queue(retry_count) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_dlq_unresolved ON dead_letter_queue(failed_at) WHERE resolved_at IS NULL;

-- ============================================
-- OBSERVABILITY (Pipeline Metrics)
-- ============================================
-- Time-series metrics for monitoring pipeline health
-- Enables debugging and performance optimization

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    metric_name TEXT NOT NULL,
    metric_value FLOAT NOT NULL,
    dimensions JSONB DEFAULT '{}',
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_name ON pipeline_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_metrics_time ON pipeline_metrics(recorded_at);
CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON pipeline_metrics(metric_name, recorded_at DESC);

-- ============================================
-- JOB QUEUE (If not already exists)
-- ============================================
-- Message queue for async processing
-- Producers insert PENDING, Consumers process and mark DONE/FAILED

CREATE TABLE IF NOT EXISTS job_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status TEXT DEFAULT 'PENDING',            -- PENDING | PROCESSING | DONE | FAILED
    file_path TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_queue_status ON job_queue(status);
CREATE INDEX IF NOT EXISTS idx_job_queue_created ON job_queue(created_at);

-- ============================================
-- HELPER VIEWS
-- ============================================

-- View with freshness decay applied automatically
CREATE OR REPLACE VIEW v_latest_predictions AS
SELECT 
    constituency_name,
    district,
    alliance,
    sentiment_score,
    confidence_weight,
    -- Apply freshness decay: 5% decay per day
    confidence_weight * POWER(0.95, EXTRACT(DAY FROM NOW() - last_updated)) 
        AS adjusted_confidence,
    source_count,
    model_version,
    last_updated
FROM constituency_predictions
WHERE last_updated > NOW() - INTERVAL '90 days';

-- District-level aggregation for quick summaries
CREATE OR REPLACE VIEW v_district_summary AS
SELECT 
    district,
    alliance,
    ROUND(AVG(sentiment_score)::numeric, 4) AS avg_sentiment,
    SUM(source_count) AS total_sources,
    COUNT(*) AS constituency_count,
    MAX(last_updated) AS latest_update
FROM constituency_predictions
GROUP BY district, alliance
ORDER BY district, alliance;

-- Alliance-level state summary
CREATE OR REPLACE VIEW v_alliance_summary AS
SELECT 
    alliance,
    ROUND(AVG(sentiment_score)::numeric, 4) AS avg_sentiment,
    SUM(source_count) AS total_sources,
    COUNT(DISTINCT district) AS districts_covered,
    COUNT(*) AS constituency_count
FROM constituency_predictions
GROUP BY alliance
ORDER BY avg_sentiment DESC;

-- DLQ summary for monitoring
CREATE OR REPLACE VIEW v_dlq_summary AS
SELECT 
    error_type,
    COUNT(*) AS error_count,
    MAX(failed_at) AS latest_failure,
    AVG(retry_count) AS avg_retries
FROM dead_letter_queue
WHERE resolved_at IS NULL
GROUP BY error_type
ORDER BY error_count DESC;

-- Recent metrics for dashboard
CREATE OR REPLACE VIEW v_recent_metrics AS
SELECT 
    metric_name,
    metric_value,
    dimensions,
    recorded_at
FROM pipeline_metrics
WHERE recorded_at > NOW() - INTERVAL '24 hours'
ORDER BY recorded_at DESC;

-- ============================================
-- FUNCTIONS
-- ============================================

-- Function to clean up old metrics (call via cron or scheduled job)
CREATE OR REPLACE FUNCTION cleanup_old_metrics(days_to_keep INT DEFAULT 90)
RETURNS INT AS $$
DECLARE
    deleted_count INT;
BEGIN
    DELETE FROM pipeline_metrics
    WHERE recorded_at < NOW() - (days_to_keep || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get queue depth (for backpressure monitoring)
CREATE OR REPLACE FUNCTION get_queue_depth()
RETURNS TABLE(status TEXT, count BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT job_queue.status, COUNT(*)
    FROM job_queue
    GROUP BY job_queue.status;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- ROW LEVEL SECURITY (Optional - disable for dev)
-- ============================================
-- Uncomment these lines if you want to enable RLS

-- ALTER TABLE constituency_predictions ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE processed_content ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE dead_letter_queue ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE pipeline_metrics ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE job_queue ENABLE ROW LEVEL SECURITY;

-- For development, you may want to disable RLS:
ALTER TABLE constituency_predictions DISABLE ROW LEVEL SECURITY;
ALTER TABLE processed_content DISABLE ROW LEVEL SECURITY;
ALTER TABLE dead_letter_queue DISABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline_metrics DISABLE ROW LEVEL SECURITY;
ALTER TABLE job_queue DISABLE ROW LEVEL SECURITY;

-- ============================================
-- GRANTS (for service role access)
-- ============================================
-- These are typically automatic with Supabase, but included for completeness

-- GRANT ALL ON constituency_predictions TO service_role;
-- GRANT ALL ON processed_content TO service_role;
-- GRANT ALL ON dead_letter_queue TO service_role;
-- GRANT ALL ON pipeline_metrics TO service_role;
-- GRANT ALL ON job_queue TO service_role;

-- ============================================
-- SEED DATA (Optional)
-- ============================================
-- Uncomment to initialize with empty predictions for all alliances

-- INSERT INTO constituency_predictions (constituency_name, district, alliance, sentiment_score)
-- SELECT DISTINCT 
--     c.constituency,
--     d.district,
--     a.alliance,
--     0.0
-- FROM (VALUES 
--     ('DMK_Front'), ('ADMK_Front'), ('TVK_Front'), ('NTK'), ('Neutral_Battleground')
-- ) AS a(alliance)
-- CROSS JOIN (
--     SELECT key AS district, jsonb_array_elements_text(value->'constituencies') AS constituency
--     FROM jsonb_each((SELECT content FROM storage.objects WHERE name = 'districts.json'))
-- ) AS d(district, constituency)
-- CROSS JOIN (SELECT 1) AS c(constituency)  -- Placeholder
-- ON CONFLICT DO NOTHING;

COMMENT ON TABLE constituency_predictions IS 'Sentiment predictions per constituency per alliance with moving average updates';
COMMENT ON TABLE processed_content IS 'Tracks processed content for deduplication (idempotency)';
COMMENT ON TABLE dead_letter_queue IS 'Failed jobs for inspection and retry';
COMMENT ON TABLE pipeline_metrics IS 'Time-series metrics for observability';
COMMENT ON TABLE job_queue IS 'Async job queue for producer-consumer pattern';

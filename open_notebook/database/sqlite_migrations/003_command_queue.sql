-- Migration 003: Command Queue for Background Jobs
-- Enables SQLite-based job queue for background processing (podcasts, embeddings)
-- Replaces surreal_commands dependency when DATABASE_BACKEND=sqlite

-- ============================================================================
-- COMMAND QUEUE TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS command_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL UNIQUE,
    namespace TEXT NOT NULL,
    command_name TEXT NOT NULL,
    args TEXT NOT NULL,  -- JSON string containing command arguments
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    result TEXT,  -- JSON string containing command result
    error_message TEXT
);

-- ============================================================================
-- PERFORMANCE INDEXES
-- ============================================================================

-- Index for worker to efficiently find pending jobs
CREATE INDEX IF NOT EXISTS idx_command_queue_status ON command_queue (status, created_at);

-- Index for job_id lookups (status queries)
CREATE INDEX IF NOT EXISTS idx_command_queue_job_id ON command_queue (job_id);

-- Index for cleanup queries (finding old completed/failed jobs)
CREATE INDEX IF NOT EXISTS idx_command_queue_completed ON command_queue (completed_at);

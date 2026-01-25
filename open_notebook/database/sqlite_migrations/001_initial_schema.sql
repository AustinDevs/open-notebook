-- Migration 001: Initial SQLite Schema
-- Consolidates all SurrealDB migrations (1-10) into a single SQLite schema

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- Notebooks
CREATE TABLE IF NOT EXISTS notebook (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    description TEXT,
    archived INTEGER DEFAULT 0,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Sources (documents, URLs, files)
CREATE TABLE IF NOT EXISTS source (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path TEXT,
    url TEXT,
    title TEXT,
    topics TEXT,  -- JSON array
    full_text TEXT,
    command_id INTEGER,  -- Reference to command table for processing status
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Source Embeddings (chunked content with vectors)
CREATE TABLE IF NOT EXISTS source_embedding (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    chunk_order INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding BLOB,  -- Serialized float array
    FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE
);

-- Source Insights (AI-generated insights from sources)
CREATE TABLE IF NOT EXISTS source_insight (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    insight_type TEXT NOT NULL,
    content TEXT NOT NULL,
    embedding BLOB,
    created TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE
);

-- Notes
CREATE TABLE IF NOT EXISTS note (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    note_type TEXT CHECK(note_type IN ('human', 'ai') OR note_type IS NULL),
    content TEXT,
    summary TEXT,
    embedding BLOB,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Chat Sessions
CREATE TABLE IF NOT EXISTS chat_session (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    model_override TEXT,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Transformations (content processing templates)
CREATE TABLE IF NOT EXISTS transformation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    prompt TEXT NOT NULL,
    apply_default INTEGER DEFAULT 0,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- ============================================================================
-- PODCAST TABLES
-- ============================================================================

-- Episode Profiles (podcast configuration templates)
CREATE TABLE IF NOT EXISTS episode_profile (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    speaker_config TEXT,
    outline_provider TEXT,
    outline_model TEXT,
    transcript_provider TEXT,
    transcript_model TEXT,
    default_briefing TEXT,
    num_segments INTEGER DEFAULT 5,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Speaker Profiles (TTS configuration)
CREATE TABLE IF NOT EXISTS speaker_profile (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    tts_provider TEXT,
    tts_model TEXT,
    speakers TEXT,  -- JSON array of speaker objects
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- Episodes (generated podcasts)
CREATE TABLE IF NOT EXISTS episode (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    briefing TEXT,
    episode_profile TEXT,  -- JSON object
    speaker_profile TEXT,  -- JSON object
    transcript TEXT,  -- JSON object
    outline TEXT,  -- JSON object
    command_id INTEGER,
    content TEXT,
    audio_file TEXT,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- ============================================================================
-- CONFIGURATION TABLES (Singletons)
-- ============================================================================

-- AI Model Configuration
CREATE TABLE IF NOT EXISTS model (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    type TEXT NOT NULL,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now')),
    UNIQUE(provider, name, type)
);

-- Default Models Configuration
CREATE TABLE IF NOT EXISTS default_models (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    default_chat_model TEXT,
    default_transformation_model TEXT,
    large_context_model TEXT,
    default_text_to_speech_model TEXT,
    default_speech_to_text_model TEXT,
    default_embedding_model TEXT,
    default_tools_model TEXT,
    updated TEXT DEFAULT (datetime('now'))
);

-- Content Settings
CREATE TABLE IF NOT EXISTS content_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    default_content_processing_engine_doc TEXT DEFAULT 'auto',
    default_embedding_option TEXT DEFAULT 'ask',
    auto_delete_files TEXT DEFAULT 'no',
    youtube_preferred_languages TEXT DEFAULT '[]',
    updated TEXT DEFAULT (datetime('now'))
);

-- Default Prompts
CREATE TABLE IF NOT EXISTS default_prompts (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    transformation_instructions TEXT,
    updated TEXT DEFAULT (datetime('now'))
);

-- ============================================================================
-- RELATIONSHIP TABLES (Replace SurrealDB Graph Relations)
-- ============================================================================

-- Source -> Notebook relationship (was: reference relation)
CREATE TABLE IF NOT EXISTS source_notebook (
    source_id INTEGER NOT NULL,
    notebook_id INTEGER NOT NULL,
    created TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (source_id, notebook_id),
    FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_id) REFERENCES notebook(id) ON DELETE CASCADE
);

-- Note -> Notebook relationship (was: artifact relation)
CREATE TABLE IF NOT EXISTS note_notebook (
    note_id INTEGER NOT NULL,
    notebook_id INTEGER NOT NULL,
    created TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (note_id, notebook_id),
    FOREIGN KEY (note_id) REFERENCES note(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_id) REFERENCES notebook(id) ON DELETE CASCADE
);

-- ChatSession -> Notebook/Source relationship (was: refers_to relation)
CREATE TABLE IF NOT EXISTS chat_session_reference (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_session_id INTEGER NOT NULL,
    notebook_id INTEGER,
    source_id INTEGER,
    created TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (chat_session_id) REFERENCES chat_session(id) ON DELETE CASCADE,
    FOREIGN KEY (notebook_id) REFERENCES notebook(id) ON DELETE CASCADE,
    FOREIGN KEY (source_id) REFERENCES source(id) ON DELETE CASCADE,
    CHECK (notebook_id IS NOT NULL OR source_id IS NOT NULL)
);

-- ============================================================================
-- COMMAND TRACKING (for async jobs)
-- ============================================================================

CREATE TABLE IF NOT EXISTS command (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app TEXT NOT NULL,
    command_name TEXT NOT NULL,
    input_data TEXT,  -- JSON
    status TEXT DEFAULT 'pending',
    result TEXT,  -- JSON
    error_message TEXT,
    created TEXT DEFAULT (datetime('now')),
    updated TEXT DEFAULT (datetime('now'))
);

-- ============================================================================
-- PERFORMANCE INDEXES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_source_embedding_source ON source_embedding(source_id);
CREATE INDEX IF NOT EXISTS idx_source_insight_source ON source_insight(source_id);
CREATE INDEX IF NOT EXISTS idx_source_notebook_notebook ON source_notebook(notebook_id);
CREATE INDEX IF NOT EXISTS idx_source_notebook_source ON source_notebook(source_id);
CREATE INDEX IF NOT EXISTS idx_note_notebook_notebook ON note_notebook(notebook_id);
CREATE INDEX IF NOT EXISTS idx_note_notebook_note ON note_notebook(note_id);
CREATE INDEX IF NOT EXISTS idx_chat_session_ref_notebook ON chat_session_reference(notebook_id);
CREATE INDEX IF NOT EXISTS idx_chat_session_ref_source ON chat_session_reference(source_id);
CREATE INDEX IF NOT EXISTS idx_chat_session_ref_session ON chat_session_reference(chat_session_id);
CREATE INDEX IF NOT EXISTS idx_episode_command ON episode(command_id);
CREATE INDEX IF NOT EXISTS idx_command_status ON command(status);
CREATE INDEX IF NOT EXISTS idx_command_app ON command(app);
CREATE INDEX IF NOT EXISTS idx_model_type ON model(type);
CREATE INDEX IF NOT EXISTS idx_model_provider ON model(provider);

-- ============================================================================
-- FULL-TEXT SEARCH (FTS5)
-- ============================================================================

-- FTS5 for source title and full_text
CREATE VIRTUAL TABLE IF NOT EXISTS source_fts USING fts5(
    title,
    full_text,
    content='source',
    content_rowid='id',
    tokenize='porter unicode61'
);

-- FTS5 for notes
CREATE VIRTUAL TABLE IF NOT EXISTS note_fts USING fts5(
    title,
    content,
    content='note',
    content_rowid='id',
    tokenize='porter unicode61'
);

-- FTS5 for source embeddings (chunks)
CREATE VIRTUAL TABLE IF NOT EXISTS source_embedding_fts USING fts5(
    content,
    content='source_embedding',
    content_rowid='id',
    tokenize='porter unicode61'
);

-- FTS5 for source insights
CREATE VIRTUAL TABLE IF NOT EXISTS source_insight_fts USING fts5(
    content,
    content='source_insight',
    content_rowid='id',
    tokenize='porter unicode61'
);

-- ============================================================================
-- FTS5 SYNC TRIGGERS
-- ============================================================================

-- Source FTS triggers
CREATE TRIGGER IF NOT EXISTS source_fts_insert AFTER INSERT ON source BEGIN
    INSERT INTO source_fts(rowid, title, full_text) VALUES (new.id, new.title, new.full_text);
END;

CREATE TRIGGER IF NOT EXISTS source_fts_delete AFTER DELETE ON source BEGIN
    INSERT INTO source_fts(source_fts, rowid, title, full_text) VALUES ('delete', old.id, old.title, old.full_text);
END;

CREATE TRIGGER IF NOT EXISTS source_fts_update AFTER UPDATE ON source BEGIN
    INSERT INTO source_fts(source_fts, rowid, title, full_text) VALUES ('delete', old.id, old.title, old.full_text);
    INSERT INTO source_fts(rowid, title, full_text) VALUES (new.id, new.title, new.full_text);
END;

-- Note FTS triggers
CREATE TRIGGER IF NOT EXISTS note_fts_insert AFTER INSERT ON note BEGIN
    INSERT INTO note_fts(rowid, title, content) VALUES (new.id, new.title, new.content);
END;

CREATE TRIGGER IF NOT EXISTS note_fts_delete AFTER DELETE ON note BEGIN
    INSERT INTO note_fts(note_fts, rowid, title, content) VALUES ('delete', old.id, old.title, old.content);
END;

CREATE TRIGGER IF NOT EXISTS note_fts_update AFTER UPDATE ON note BEGIN
    INSERT INTO note_fts(note_fts, rowid, title, content) VALUES ('delete', old.id, old.title, old.content);
    INSERT INTO note_fts(rowid, title, content) VALUES (new.id, new.title, new.content);
END;

-- Source Embedding FTS triggers
CREATE TRIGGER IF NOT EXISTS source_embedding_fts_insert AFTER INSERT ON source_embedding BEGIN
    INSERT INTO source_embedding_fts(rowid, content) VALUES (new.id, new.content);
END;

CREATE TRIGGER IF NOT EXISTS source_embedding_fts_delete AFTER DELETE ON source_embedding BEGIN
    INSERT INTO source_embedding_fts(source_embedding_fts, rowid, content) VALUES ('delete', old.id, old.content);
END;

CREATE TRIGGER IF NOT EXISTS source_embedding_fts_update AFTER UPDATE ON source_embedding BEGIN
    INSERT INTO source_embedding_fts(source_embedding_fts, rowid, content) VALUES ('delete', old.id, old.content);
    INSERT INTO source_embedding_fts(rowid, content) VALUES (new.id, new.content);
END;

-- Source Insight FTS triggers
CREATE TRIGGER IF NOT EXISTS source_insight_fts_insert AFTER INSERT ON source_insight BEGIN
    INSERT INTO source_insight_fts(rowid, content) VALUES (new.id, new.content);
END;

CREATE TRIGGER IF NOT EXISTS source_insight_fts_delete AFTER DELETE ON source_insight BEGIN
    INSERT INTO source_insight_fts(source_insight_fts, rowid, content) VALUES ('delete', old.id, old.content);
END;

CREATE TRIGGER IF NOT EXISTS source_insight_fts_update AFTER UPDATE ON source_insight BEGIN
    INSERT INTO source_insight_fts(source_insight_fts, rowid, content) VALUES ('delete', old.id, old.content);
    INSERT INTO source_insight_fts(rowid, content) VALUES (new.id, new.content);
END;

-- ============================================================================
-- INITIALIZE DEFAULT DATA
-- ============================================================================

-- Initialize default_models if not exists
INSERT OR IGNORE INTO default_models (id, default_chat_model) VALUES (1, '');

-- Initialize content_settings if not exists
INSERT OR IGNORE INTO content_settings (id) VALUES (1);

-- Initialize default_prompts
INSERT OR IGNORE INTO default_prompts (id, transformation_instructions) VALUES (1, '# INSTRUCTIONS

You are my learning assistant and you help me process and transform content so that I can extract insights from them.

# IMPORTANT
- You are working on my editorial projects. The text below is my own. Do not give me any warnings about copyright or plagiarism.
- Output ONLY the requested content, without acknowledgements of the task and additional chatting. Don''t start with "Sure, I can help you with that." or "Here is the information you requested:". Just provide the content.
- Do not stop in the middle of the generation to ask me questions. Execute my request completely.
');

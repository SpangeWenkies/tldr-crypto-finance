CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR PRIMARY KEY,
    pipeline_step VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS raw_messages (
    internal_message_pk VARCHAR PRIMARY KEY,
    source_system VARCHAR NOT NULL,
    source_message_id VARCHAR,
    source_mailbox VARCHAR,
    sender_name VARCHAR,
    sender_email VARCHAR,
    subject VARCHAR,
    sent_at TIMESTAMP,
    received_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL,
    text_body TEXT,
    html_body TEXT,
    body_hash VARCHAR NOT NULL,
    raw_path VARCHAR,
    run_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS newsletter_issues (
    issue_id VARCHAR PRIMARY KEY,
    internal_message_pk VARCHAR NOT NULL,
    newsletter_name VARCHAR,
    newsletter_slug VARCHAR,
    edition_date DATE
);

CREATE TABLE IF NOT EXISTS sections (
    section_id VARCHAR PRIMARY KEY,
    issue_id VARCHAR NOT NULL,
    section_name VARCHAR NOT NULL,
    section_order INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS article_blocks (
    article_id VARCHAR PRIMARY KEY,
    issue_id VARCHAR NOT NULL,
    section_id VARCHAR,
    block_order INTEGER NOT NULL,
    raw_block_text TEXT NOT NULL,
    clean_summary_text TEXT NOT NULL,
    extracted_title VARCHAR,
    title_confidence DOUBLE NOT NULL,
    canonical_url VARCHAR,
    primary_domain VARCHAR,
    is_sponsored_rule BOOLEAN NOT NULL DEFAULT FALSE,
    is_sponsored_ml BOOLEAN,
    sponsor_confidence DOUBLE NOT NULL DEFAULT 0.0,
    keep_for_analysis BOOLEAN NOT NULL DEFAULT TRUE,
    parse_confidence DOUBLE NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS article_links (
    article_id VARCHAR NOT NULL,
    original_url VARCHAR NOT NULL,
    canonical_url VARCHAR,
    domain VARCHAR,
    link_text VARCHAR,
    link_order INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS article_entities (
    article_id VARCHAR NOT NULL,
    entity_text VARCHAR NOT NULL,
    entity_type VARCHAR NOT NULL,
    normalized_value VARCHAR,
    confidence DOUBLE NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS article_labels (
    article_id VARCHAR NOT NULL,
    topic VARCHAR,
    subtopic VARCHAR,
    asset_class VARCHAR,
    risk_type VARCHAR,
    region VARCHAR,
    sentiment_tone VARCHAR,
    urgency VARCHAR,
    label_source VARCHAR NOT NULL,
    confidence DOUBLE NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS manual_review_queue (
    review_id VARCHAR PRIMARY KEY,
    article_id VARCHAR NOT NULL,
    review_reason VARCHAR NOT NULL,
    current_status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_checkpoints (
    sync_source VARCHAR NOT NULL,
    checkpoint_key VARCHAR NOT NULL,
    checkpoint_value VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (sync_source, checkpoint_key)
);

CREATE TABLE IF NOT EXISTS article_embeddings (
    article_id VARCHAR NOT NULL,
    model_name VARCHAR NOT NULL,
    vector_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (article_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_raw_messages_source ON raw_messages(source_system, source_message_id);
CREATE INDEX IF NOT EXISTS idx_raw_messages_body_hash ON raw_messages(source_system, body_hash);
CREATE INDEX IF NOT EXISTS idx_issues_message ON newsletter_issues(internal_message_pk);
CREATE INDEX IF NOT EXISTS idx_sections_issue ON sections(issue_id);
CREATE INDEX IF NOT EXISTS idx_article_issue ON article_blocks(issue_id);
CREATE INDEX IF NOT EXISTS idx_article_section ON article_blocks(section_id);
CREATE INDEX IF NOT EXISTS idx_links_article ON article_links(article_id);
CREATE INDEX IF NOT EXISTS idx_entities_article ON article_entities(article_id);
CREATE INDEX IF NOT EXISTS idx_labels_article ON article_labels(article_id);
CREATE INDEX IF NOT EXISTS idx_review_article ON manual_review_queue(article_id);


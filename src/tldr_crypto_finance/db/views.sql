CREATE OR REPLACE VIEW v_non_sponsored_articles AS
SELECT *
FROM article_blocks
WHERE keep_for_analysis = TRUE
  AND coalesce(is_sponsored_rule, FALSE) = FALSE
  AND coalesce(is_sponsored_ml, FALSE) = FALSE;

CREATE OR REPLACE VIEW v_latest_articles AS
SELECT
    ab.*,
    rm.sent_at,
    rm.sender_email,
    rm.subject
FROM article_blocks ab
JOIN newsletter_issues ni ON ni.issue_id = ab.issue_id
JOIN raw_messages rm ON rm.internal_message_pk = ni.internal_message_pk
ORDER BY rm.sent_at DESC NULLS LAST, ab.block_order ASC;

CREATE OR REPLACE VIEW v_topic_counts_by_week AS
SELECT
    date_trunc('week', rm.sent_at) AS week_start,
    al.topic,
    count(*) AS article_count
FROM article_labels al
JOIN article_blocks ab ON ab.article_id = al.article_id
JOIN newsletter_issues ni ON ni.issue_id = ab.issue_id
JOIN raw_messages rm ON rm.internal_message_pk = ni.internal_message_pk
GROUP BY 1, 2;

CREATE OR REPLACE VIEW v_crypto_mix AS
SELECT
    date_trunc('week', rm.sent_at) AS week_start,
    CASE
        WHEN lower(coalesce(al.topic, '')) = 'crypto_markets'
          OR lower(coalesce(al.asset_class, '')) = 'crypto'
        THEN 'crypto'
        ELSE 'non_crypto'
    END AS content_bucket,
    count(*) AS article_count
FROM article_blocks ab
LEFT JOIN article_labels al ON al.article_id = ab.article_id
JOIN newsletter_issues ni ON ni.issue_id = ab.issue_id
JOIN raw_messages rm ON rm.internal_message_pk = ni.internal_message_pk
GROUP BY 1, 2;

CREATE OR REPLACE VIEW v_domain_counts AS
SELECT
    coalesce(primary_domain, 'unknown') AS domain,
    count(*) AS article_count
FROM article_blocks
GROUP BY 1
ORDER BY article_count DESC;

CREATE OR REPLACE VIEW v_duplicates AS
SELECT
    a1.article_id AS article_id_left,
    a2.article_id AS article_id_right,
    a1.canonical_url,
    a1.clean_summary_text
FROM article_blocks a1
JOIN article_blocks a2
  ON a1.article_id < a2.article_id
 AND (
        (a1.canonical_url IS NOT NULL AND a1.canonical_url = a2.canonical_url)
     OR a1.clean_summary_text = a2.clean_summary_text
 )
WHERE a1.keep_for_analysis = TRUE
  AND a2.keep_for_analysis = TRUE;

CREATE OR REPLACE VIEW v_low_confidence_labels AS
SELECT *
FROM article_labels
WHERE confidence < 0.55;

CREATE OR REPLACE VIEW v_ambiguous_sponsor_cases AS
SELECT *
FROM manual_review_queue
WHERE review_reason LIKE 'sponsor:%';

CREATE OR REPLACE VIEW v_watchlist_hits AS
SELECT
    ab.article_id,
    ni.newsletter_name,
    rm.sent_at,
    ab.extracted_title,
    ab.canonical_url,
    CASE
        WHEN regexp_matches(lower(ab.clean_summary_text), 'liquidity|funding stress|collateral|margin call|market depth') THEN 'liquidity'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'insolvency|solvency|restructuring|default|bankruptcy') THEN 'insolvency'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'regulation|regulator|enforcement|rulemaking|compliance') THEN 'regulation'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'custody|custodian|safeguarding') THEN 'custody'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'stablecoin|stablecoins|usdc|tether|reserve backing') THEN 'stablecoins'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'exchange risk|exchange solvency|order book|exchange outage') THEN 'exchange_risk'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'sanctions|ofac|export controls') THEN 'sanctions'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'breach|exploit|ransomware|phishing|outage') THEN 'cyber_incidents'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'fraud|aml|money laundering|scam') THEN 'fraud'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'rates|yields|fed|ecb|tightening') THEN 'rates'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'inflation|cpi|pce|price pressures') THEN 'inflation'
        WHEN regexp_matches(lower(ab.clean_summary_text), 'capital controls|currency controls|transfer restrictions') THEN 'capital_controls'
        ELSE NULL
    END AS watchlist_name
FROM article_blocks ab
JOIN newsletter_issues ni ON ni.issue_id = ab.issue_id
JOIN raw_messages rm ON rm.internal_message_pk = ni.internal_message_pk
WHERE ab.keep_for_analysis = TRUE
  AND watchlist_name IS NOT NULL;

CREATE OR REPLACE VIEW v_asset_risk_slices AS
SELECT
    coalesce(al.asset_class, 'unknown') AS asset_class,
    coalesce(al.risk_type, 'unknown') AS risk_type,
    count(*) AS article_count
FROM article_labels al
GROUP BY 1, 2
ORDER BY article_count DESC;

CREATE OR REPLACE VIEW v_parse_quality_by_newsletter AS
SELECT
    ni.newsletter_name,
    count(DISTINCT ni.issue_id) AS issue_count,
    count(DISTINCT s.section_id) AS section_count,
    count(DISTINCT ab.article_id) AS article_count,
    avg(ab.parse_confidence) AS avg_parse_confidence
FROM newsletter_issues ni
LEFT JOIN sections s ON s.issue_id = ni.issue_id
LEFT JOIN article_blocks ab ON ab.issue_id = ni.issue_id
GROUP BY 1
ORDER BY avg_parse_confidence ASC NULLS LAST, article_count DESC;

-- Hive Ledger — Unified Provenance Protocol
-- Swarm & Bee LLC — Royal Jelly Protocol (RJP-1)
-- 7 tables: signals, pairs, batches, batch_pairs, anchors, model_lineage, revenue

-- 1. Signals — Raw intelligence from SwarmRadar
CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY,
    source_worker TEXT NOT NULL,
    source_url TEXT DEFAULT '',
    source_weight REAL NOT NULL,
    domain TEXT NOT NULL,
    title TEXT DEFAULT '',
    event_type TEXT DEFAULT '',
    vertical TEXT DEFAULT '',
    priority INTEGER DEFAULT 3,
    entity_count INTEGER DEFAULT 0,
    radar_score REAL DEFAULT 0,
    radar_decision TEXT DEFAULT '',
    corroboration_key TEXT DEFAULT '',
    collected_at TEXT NOT NULL,
    hedera_tx TEXT DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 2. Pairs — Scored training pairs (Royal Jelly Protocol)
CREATE TABLE IF NOT EXISTS pairs (
    pair_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    signal_id TEXT DEFAULT '',
    domain TEXT NOT NULL,
    task_type TEXT NOT NULL,
    score INTEGER NOT NULL,
    tier TEXT NOT NULL,
    source_confidence REAL DEFAULT 0,
    gate_integrity REAL DEFAULT 0,
    reasoning_depth REAL DEFAULT 0,
    entropy_health REAL DEFAULT 0,
    fingerprint_uniqueness REAL DEFAULT 0,
    gate_json_valid INTEGER DEFAULT 1,
    gate_output_length INTEGER DEFAULT 1,
    gate_numeric_verify INTEGER DEFAULT 1,
    gate_concept_present INTEGER DEFAULT 1,
    gate_dedup INTEGER DEFAULT 1,
    gate_degenerate INTEGER DEFAULT 1,
    gates_passed INTEGER DEFAULT 6,
    adversarial_detected INTEGER DEFAULT 0,
    adversarial_type TEXT DEFAULT '',
    adversarial_penalty REAL DEFAULT 0,
    gen_model TEXT DEFAULT '',
    cook_script TEXT DEFAULT '',
    source_file TEXT DEFAULT '',
    status TEXT DEFAULT 'active',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 3. Batches — Sealed collections with Merkle roots
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    pair_count INTEGER NOT NULL,
    merkle_root TEXT NOT NULL,
    gate_pass_rate REAL NOT NULL,
    avg_score REAL NOT NULL,
    tier_distribution TEXT DEFAULT '{}',
    audit_timestamp TEXT DEFAULT '',
    contamination_rate REAL DEFAULT 0,
    think_tags_found INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 4. Batch-Pairs junction
CREATE TABLE IF NOT EXISTS batch_pairs (
    batch_id TEXT NOT NULL,
    pair_id TEXT NOT NULL,
    line_number INTEGER DEFAULT 0,
    PRIMARY KEY (batch_id, pair_id)
);

-- 5. Anchors — Hedera blockchain proofs
CREATE TABLE IF NOT EXISTS anchors (
    anchor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    merkle_root TEXT NOT NULL,
    hedera_tx TEXT NOT NULL,
    hedera_topic TEXT DEFAULT '',
    hedera_sequence INTEGER DEFAULT 0,
    hedera_network TEXT DEFAULT 'mainnet',
    verify_url TEXT DEFAULT '',
    cell_id TEXT DEFAULT '',
    cell_score REAL DEFAULT 0,
    anchored_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 6. Model lineage
CREATE TABLE IF NOT EXISTS model_lineage (
    model_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    training_run_id TEXT DEFAULT '',
    pairs_used INTEGER DEFAULT 0,
    loss REAL DEFAULT 0,
    eval_loss REAL DEFAULT 0,
    trained_at TEXT DEFAULT '',
    PRIMARY KEY (model_id, batch_id)
);

-- 7. Revenue — Order fulfillment
CREATE TABLE IF NOT EXISTS revenue (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    pair_count INTEGER NOT NULL,
    amount_cents INTEGER NOT NULL,
    domain TEXT NOT NULL,
    tier_minimum TEXT DEFAULT '',
    batch_ids TEXT DEFAULT '[]',
    fulfilled_at TEXT DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pairs_domain_tier ON pairs(domain, tier);
CREATE INDEX IF NOT EXISTS idx_pairs_fingerprint ON pairs(fingerprint);
CREATE INDEX IF NOT EXISTS idx_pairs_signal ON pairs(signal_id);
CREATE INDEX IF NOT EXISTS idx_pairs_score ON pairs(score DESC);
CREATE INDEX IF NOT EXISTS idx_batches_domain ON batches(domain);
CREATE INDEX IF NOT EXISTS idx_batches_merkle ON batches(merkle_root);
CREATE INDEX IF NOT EXISTS idx_anchors_batch ON anchors(batch_id);
CREATE INDEX IF NOT EXISTS idx_anchors_hedera ON anchors(hedera_tx);
CREATE INDEX IF NOT EXISTS idx_signals_domain ON signals(domain);
CREATE INDEX IF NOT EXISTS idx_signals_radar ON signals(radar_decision);
CREATE INDEX IF NOT EXISTS idx_revenue_customer ON revenue(customer_id);

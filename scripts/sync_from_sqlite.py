#!/usr/bin/env python3
"""
sync_from_sqlite.py — Bulk sync from honey_ledger.db to hive-ledger API.

Exports pairs and batches from the SQLite Honey Ledger and POSTs them
to the hive-ledger D1 database in chunks.

Usage:
    python3 sync_from_sqlite.py --db /path/to/honey_ledger.db [--chunk 5000] [--dry-run]

Environment:
    HIVE_LEDGER_URL     (default: https://ledger.swarmandbee.ai)
    HIVE_ADMIN_KEY      (required)
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request
import urllib.error

LEDGER_URL = os.environ.get("HIVE_LEDGER_URL", "https://ledger.swarmandbee.ai")
ADMIN_KEY = os.environ.get("HIVE_ADMIN_KEY", "")

UA = "SwarmLedgerSync/1.0"

# Map legacy tier names to RJP-1 tiers
TIER_MAP = {
    "genesis": "royal_jelly",
    "cluster": "honey",
    "honey": "honey",
    "cell": "pollen",
    "pollen": "pollen",
    "royal_jelly": "royal_jelly",
    "propolis": "propolis",
    "jelly": "royal_jelly",
}


def post_sync(data: dict) -> dict:
    url = f"{LEDGER_URL}/api/admin/sync"
    payload = json.dumps(data).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Admin-Key": ADMIN_KEY,
            "User-Agent": UA,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:300]
        print(f"  ERROR: HTTP {e.code}: {body}", file=sys.stderr)
        return {"error": body}


def normalize_tier(tier: str) -> str:
    """Map legacy tier names to RJP-1 canonical tiers."""
    return TIER_MAP.get(tier.lower().strip(), "propolis") if tier else "propolis"


def normalize_tier_distribution(dist_str: str) -> dict:
    """Normalize tier distribution JSON from legacy names to RJP-1."""
    if not dist_str:
        return {}
    try:
        dist = json.loads(dist_str)
    except json.JSONDecodeError:
        return {}

    normalized = {}
    for tier, count in dist.items():
        rjp_tier = normalize_tier(tier)
        normalized[rjp_tier] = normalized.get(rjp_tier, 0) + count
    return normalized


def sync_batches(conn, dry_run=False):
    """Sync batches table."""
    cursor = conn.execute("SELECT COUNT(*) FROM batches")
    total = cursor.fetchone()[0]
    print(f"Batches to sync: {total}")

    # Check columns
    cursor = conn.execute("PRAGMA table_info(batches)")
    columns = {row[1] for row in cursor}

    # Build flexible SELECT
    select_cols = ["batch_id", "domain", "pair_count", "merkle_root"]
    for col in ["gate_pass_rate", "avg_score", "tier_distribution", "created_at", "status"]:
        if col in columns:
            select_cols.append(col)

    cursor = conn.execute(f"SELECT {', '.join(select_cols)} FROM batches")

    batches = []
    for row in cursor:
        data = {select_cols[i]: row[i] for i in range(len(select_cols))}

        tier_dist = normalize_tier_distribution(data.get("tier_distribution", ""))

        batches.append({
            "batch_id": data["batch_id"],
            "domain": data["domain"],
            "pair_count": data["pair_count"],
            "merkle_root": data["merkle_root"],
            "gate_pass_rate": data.get("gate_pass_rate", 0) or 0,
            "avg_score": data.get("avg_score", 0) or 0,
            "tier_distribution": tier_dist,
            "audit_timestamp": data.get("created_at", ""),
        })

    if dry_run:
        print(f"  [DRY RUN] Would sync {len(batches)} batches")
        return 0

    # POST in one chunk (usually <500 batches)
    result = post_sync({"batches": batches})
    synced = result.get("synced_batches", 0)
    print(f"  Synced {synced} batches")
    return synced


def sync_pairs(conn, chunk_size=5000, dry_run=False):
    """Sync pairs table in chunks."""
    cursor = conn.execute("SELECT COUNT(*) FROM pairs")
    total = cursor.fetchone()[0]
    print(f"Pairs to sync: {total:,}")

    # Check which columns exist
    cursor = conn.execute("PRAGMA table_info(pairs)")
    columns = {row[1] for row in cursor}

    # Build SELECT based on available columns
    select_cols = ["pair_id", "fingerprint", "domain"]
    # Add optional columns that might exist
    for col in ["task_type", "score", "tier", "signal_id",
                "source_confidence", "gate_integrity", "reasoning_depth",
                "entropy_health", "fingerprint_uniqueness",
                "gate_json_valid", "gate_output_length", "gate_numeric_verify",
                "gate_concept_present", "gate_dedup", "gate_degenerate",
                "gates_passed", "gen_model", "cook_script", "source_file", "status"]:
        if col in columns:
            select_cols.append(col)

    sql = f"SELECT {', '.join(select_cols)} FROM pairs"
    cursor = conn.execute(sql)

    synced_total = 0
    chunk = []
    chunk_num = 0
    errors = 0

    for row in cursor:
        pair = {}
        for i, col in enumerate(select_cols):
            pair[col] = row[i]

        # Ensure required fields
        pair.setdefault("task_type", "")
        pair.setdefault("score", 0)
        pair.setdefault("status", "active")

        # Normalize tier name
        raw_tier = pair.get("tier", "propolis") or "propolis"
        pair["tier"] = normalize_tier(raw_tier)

        chunk.append(pair)

        if len(chunk) >= chunk_size:
            chunk_num += 1
            if dry_run:
                print(f"  [DRY RUN] Chunk {chunk_num}: {len(chunk)} pairs ({chunk_num * chunk_size:,}/{total:,})")
            else:
                result = post_sync({"pairs": chunk})
                if "error" in result:
                    errors += 1
                    print(f"  Chunk {chunk_num}: ERROR")
                else:
                    synced = result.get("synced_pairs", 0)
                    synced_total += synced
                    print(f"  Chunk {chunk_num}: {synced}/{len(chunk)} synced ({synced_total:,}/{total:,} total)")
                time.sleep(0.3)

            chunk = []

    # Final chunk
    if chunk:
        chunk_num += 1
        if dry_run:
            print(f"  [DRY RUN] Chunk {chunk_num}: {len(chunk)} pairs")
        else:
            result = post_sync({"pairs": chunk})
            if "error" not in result:
                synced = result.get("synced_pairs", 0)
                synced_total += synced
                print(f"  Chunk {chunk_num}: {synced}/{len(chunk)} synced ({synced_total:,}/{total:,} total)")

    print(f"\n  Total synced: {synced_total:,}")
    if errors:
        print(f"  Errors: {errors}")
    return synced_total


def main():
    parser = argparse.ArgumentParser(description="Sync honey_ledger.db to hive-ledger")
    parser.add_argument("--db", required=True, help="Path to honey_ledger.db")
    parser.add_argument("--chunk", type=int, default=5000, help="Pairs per API request")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pairs-only", action="store_true")
    parser.add_argument("--batches-only", action="store_true")
    args = parser.parse_args()

    if not ADMIN_KEY and not args.dry_run:
        print("ERROR: Set HIVE_ADMIN_KEY environment variable", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    print(f"Connected to {args.db}")

    if not args.pairs_only:
        sync_batches(conn, args.dry_run)
        print()

    if not args.batches_only:
        sync_pairs(conn, args.chunk, args.dry_run)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

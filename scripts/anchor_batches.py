#!/usr/bin/env python3
"""
anchor_batches.py — Anchor unanchored hive-ledger batches to Hedera HCS.

Queries hive-ledger API for batches, finds unanchored ones, publishes
Merkle roots to HCS, and records anchors back via admin API.

Usage:
    python3 anchor_batches.py                          # Anchor all unanchored
    python3 anchor_batches.py --batch-id BATCH-XXX     # Anchor specific batch
    python3 anchor_batches.py --dry-run                # Preview without publishing
    python3 anchor_batches.py --limit 10               # Max batches to anchor

Environment:
    HIVE_LEDGER_URL     (default: https://ledger.swarmandbee.ai)
    HIVE_ADMIN_KEY      (required)
    HEDERA_OPERATOR_ID  (required, e.g. 0.0.10291827)
    HEDERA_OPERATOR_KEY (required, ECDSA hex)
    HEDERA_TOPIC_ID     (default: 0.0.10291838)
    HEDERA_NETWORK      (default: mainnet)

Run from swarmrails where hiero-sdk-python is installed.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

LEDGER_URL = os.environ.get("HIVE_LEDGER_URL", "https://ledger.swarmandbee.ai")
ADMIN_KEY = os.environ.get("HIVE_ADMIN_KEY", "")

# Hedera config
HEDERA_OPERATOR_ID = os.environ.get("HEDERA_OPERATOR_ID", "0.0.10291827")
HEDERA_OPERATOR_KEY = os.environ.get("HEDERA_OPERATOR_KEY", "")
HEDERA_TOPIC_ID = os.environ.get("HEDERA_TOPIC_ID", "0.0.10291838")
HEDERA_NETWORK = os.environ.get("HEDERA_NETWORK", "mainnet")

UA = "SwarmAnchorPipeline/1.0"


def api_get(path: str) -> dict:
    req = urllib.request.Request(
        f"{LEDGER_URL}{path}",
        headers={"User-Agent": UA},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def api_post(path: str, body: dict) -> dict:
    payload = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{LEDGER_URL}{path}",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Admin-Key": ADMIN_KEY,
            "User-Agent": UA,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def get_unanchored_batches(limit: int = 200) -> list[dict]:
    """Get batches that don't have anchors yet."""
    batches = api_get(f"/api/batches?limit={limit}")
    anchors = api_get("/api/anchors")

    anchored_ids = {a["batch_id"] for a in anchors.get("anchors", [])}
    all_batches = batches.get("items", [])

    unanchored = [b for b in all_batches if b["batch_id"] not in anchored_ids]
    return unanchored


def publish_to_hcs(merkle_root: str, metadata: dict, dry_run: bool = False) -> dict:
    """Publish a Merkle root to Hedera HCS."""
    message = {
        "type": "pair_batch",
        "merkle_root": merkle_root,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0",
        "publisher": "swarmandbee.hbar",
    }
    message.update(metadata)
    message_json = json.dumps(message, separators=(",", ":"))

    if dry_run:
        fake_tx = f"0.0.DRY_RUN@{int(time.time())}.000000000"
        print(f"  [DRY RUN] Would publish: {message_json[:100]}...")
        return {
            "tx_id": fake_tx,
            "topic_id": HEDERA_TOPIC_ID,
            "sequence": 0,
            "verify_url": f"https://hashscan.io/{HEDERA_NETWORK}/transaction/{fake_tx}",
            "network": HEDERA_NETWORK,
            "dry_run": True,
        }

    from hiero_sdk_python import AccountId, Client, PrivateKey, TopicId, TopicMessageSubmitTransaction

    # Init client
    operator_id = AccountId.from_string(HEDERA_OPERATOR_ID)
    key_str = HEDERA_OPERATOR_KEY.strip()
    if len(key_str) == 64:
        key_bytes = bytes.fromhex(key_str)
        operator_key = PrivateKey.from_bytes_ecdsa(key_bytes)
    else:
        operator_key = PrivateKey.from_string(key_str)

    if HEDERA_NETWORK == "mainnet":
        client = Client.for_mainnet()
    elif HEDERA_NETWORK == "previewnet":
        client = Client.for_previewnet()
    else:
        client = Client.for_testnet()
    client.set_operator(operator_id, operator_key)

    # Parse topic ID
    parts = HEDERA_TOPIC_ID.split(".")
    topic_id = TopicId(int(parts[0]), int(parts[1]), int(parts[2]))

    # Submit message
    tx = (
        TopicMessageSubmitTransaction()
        .set_topic_id(topic_id)
        .set_message(message_json)
        .freeze_with(client)
        .sign(operator_key)
    )

    receipt = tx.execute(client)
    tx_id = str(receipt.transaction_id)
    sequence = getattr(receipt, "sequence_number", 0)

    verify_url = f"https://hashscan.io/{HEDERA_NETWORK}/transaction/{tx_id}"
    print(f"  [hedera] TX: {tx_id}")
    print(f"  [hedera] Sequence: {sequence}")
    print(f"  [hedera] Verify: {verify_url}")

    return {
        "tx_id": tx_id,
        "topic_id": HEDERA_TOPIC_ID,
        "sequence": sequence,
        "verify_url": verify_url,
        "network": HEDERA_NETWORK,
    }


def record_anchor(batch_id: str, merkle_root: str, hcs_result: dict) -> dict:
    """Record the anchor in hive-ledger D1."""
    return api_post("/api/admin/anchor", {
        "batch_id": batch_id,
        "merkle_root": merkle_root,
        "hedera_tx": hcs_result["tx_id"],
        "hedera_topic": hcs_result["topic_id"],
        "hedera_sequence": hcs_result.get("sequence", 0),
        "hedera_network": hcs_result["network"],
        "verify_url": hcs_result["verify_url"],
    })


def main():
    parser = argparse.ArgumentParser(description="Anchor batches to Hedera HCS")
    parser.add_argument("--batch-id", help="Anchor a specific batch")
    parser.add_argument("--limit", type=int, default=50, help="Max batches to anchor")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not ADMIN_KEY:
        print("ERROR: Set HIVE_ADMIN_KEY", file=sys.stderr)
        sys.exit(1)

    if not HEDERA_OPERATOR_KEY and not args.dry_run:
        print("ERROR: Set HEDERA_OPERATOR_KEY", file=sys.stderr)
        sys.exit(1)

    if args.batch_id:
        # Anchor specific batch
        batch = api_get(f"/api/batches/{args.batch_id}")
        if "error" in batch:
            print(f"Batch not found: {args.batch_id}")
            sys.exit(1)
        batches = [batch]
    else:
        batches = get_unanchored_batches(args.limit)

    print(f"Unanchored batches: {len(batches)}")
    if not batches:
        print("All batches anchored.")
        return

    anchored = 0
    for batch in batches:
        bid = batch["batch_id"]
        root = batch["merkle_root"]
        domain = batch["domain"]
        pairs = batch.get("pair_count", 0)

        print(f"\n  Anchoring: {bid}")
        print(f"    Domain: {domain}, Pairs: {pairs}, Root: {root[:24]}...")

        metadata = {
            "domain": domain,
            "pair_count": pairs,
            "avg_score": batch.get("avg_score", 0),
            "batch_id": bid,
        }

        try:
            result = publish_to_hcs(root, metadata, args.dry_run)

            if not args.dry_run:
                record_anchor(bid, root, result)
                print(f"    Anchor recorded in D1")

            anchored += 1
            time.sleep(0.5)  # Rate limit HCS submissions

        except Exception as e:
            print(f"    ERROR: {e}")
            continue

    print(f"\nAnchored: {anchored}/{len(batches)} batches")


if __name__ == "__main__":
    main()

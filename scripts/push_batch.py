#!/usr/bin/env python3
"""
push_batch.py — Push a cook auditor checkpoint batch to hive-ledger API.

Called by cook_auditor.py after each 3-hour checkpoint.
Reads the checkpoint data from the cook auditor's output and POSTs to the API.

Usage:
    python3 push_batch.py --batch-file <path> --domain <domain>
    python3 push_batch.py --cook-dir <dir> --domain <domain> --checkpoint <n>

Environment:
    HIVE_LEDGER_URL     (default: https://ledger.swarmandbee.ai)
    HIVE_ADMIN_KEY      (required)
"""

import argparse
import hashlib
import json
import os
import sys
import urllib.request
import urllib.error

LEDGER_URL = os.environ.get("HIVE_LEDGER_URL", "https://ledger.swarmandbee.ai")
ADMIN_KEY = os.environ.get("HIVE_ADMIN_KEY", "")
UA = "SwarmCookAuditor/1.0"

# Canonical 3-letter domain codes (matches virgin-jelly/protocol.py)
DOMAIN_CODES: dict[str, str] = {
    "ai": "AIS", "medical": "MED", "aviation": "AVI", "cre": "CRE",
    "economic": "ECO", "legal": "LGL", "energy": "NRG", "climate": "CLM",
    "crypto": "CRY", "finance": "FIN", "software": "SFT",
    "supply_chain": "SCH", "patents": "PAT", "general": "GEN",
}


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def merkle_root(fingerprints: list[str]) -> str:
    if not fingerprints:
        return ""
    leaves = [sha256(fp) for fp in fingerprints]
    if len(leaves) % 2 != 0:
        leaves.append(leaves[-1])
    while len(leaves) > 1:
        next_level = []
        for i in range(0, len(leaves), 2):
            combined = leaves[i] + leaves[i + 1]
            next_level.append(sha256(combined))
        leaves = next_level
        if len(leaves) > 1 and len(leaves) % 2 != 0:
            leaves.append(leaves[-1])
    return leaves[0]


def post_batch(batch_data: dict) -> dict:
    url = f"{LEDGER_URL}/api/admin/batch"
    payload = json.dumps(batch_data).encode()

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
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"ERROR: HTTP {e.code}: {body}", file=sys.stderr)
        sys.exit(1)


def load_pairs_from_jsonl(path: str, domain: str) -> list[dict]:
    """Load pairs from a cook output JSONL file."""
    pairs = []
    with open(path) as f:
        for line_num, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Extract pair data from cook output format
            conv = record.get("conversations", [])
            if len(conv) < 2:
                continue

            user_msg = conv[0].get("value", "") if conv[0].get("from") == "human" else ""
            asst_msg = conv[1].get("value", "") if conv[1].get("from") == "gpt" else ""

            if not user_msg or not asst_msg:
                continue

            fp = sha256((user_msg + asst_msg).strip().lower())
            domain_code = DOMAIN_CODES.get(domain, domain.upper()[:3])
            pair_id = f"HIVE-{domain_code}-{fp[:12]}"

            meta = record.get("metadata", {})

            pairs.append({
                "pair_id": pair_id,
                "fingerprint": fp,
                "domain": domain,
                "task_type": meta.get("task_type", "analysis"),
                "score": meta.get("jelly_score", 0),
                "tier": meta.get("tier", "propolis"),
                "source_confidence": meta.get("source_confidence", 0),
                "gate_integrity": meta.get("gate_integrity", 0),
                "reasoning_depth": meta.get("reasoning_depth", 0),
                "entropy_health": meta.get("entropy_health", 0),
                "fingerprint_uniqueness": meta.get("fingerprint_uniqueness", 0),
                "gate_json_valid": int(meta.get("gate_json_valid", 1)),
                "gate_output_length": int(meta.get("gate_output_length", 1)),
                "gate_numeric_verify": int(meta.get("gate_numeric_verify", 1)),
                "gate_concept_present": int(meta.get("gate_concept_present", 1)),
                "gate_dedup": int(meta.get("gate_dedup", 1)),
                "gate_degenerate": int(meta.get("gate_degenerate", 1)),
                "gates_passed": meta.get("gates_passed", 6),
                "gen_model": meta.get("gen_model", ""),
                "cook_script": meta.get("cook_script", "cook_openalex.py"),
                "source_file": meta.get("source_file", os.path.basename(path)),
            })

    return pairs


def main():
    parser = argparse.ArgumentParser(description="Push batch to hive-ledger")
    parser.add_argument("--batch-file", help="JSONL file with scored pairs")
    parser.add_argument("--domain", required=True)
    parser.add_argument("--batch-id", help="Override batch ID")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not ADMIN_KEY:
        print("ERROR: Set HIVE_ADMIN_KEY environment variable", file=sys.stderr)
        sys.exit(1)

    if not args.batch_file:
        print("ERROR: --batch-file required", file=sys.stderr)
        sys.exit(1)

    pairs = load_pairs_from_jsonl(args.batch_file, args.domain)
    if not pairs:
        print("No pairs found in file")
        return

    fingerprints = [p["fingerprint"] for p in pairs]
    root = merkle_root(fingerprints)

    # Compute batch stats
    scores = [p["score"] for p in pairs if p["score"] > 0]
    avg_score = sum(scores) / len(scores) if scores else 0
    gate_counts = [p["gates_passed"] for p in pairs]
    gate_pass_rate = sum(1 for g in gate_counts if g == 6) / len(gate_counts) if gate_counts else 0

    tier_dist = {}
    for p in pairs:
        tier_dist[p["tier"]] = tier_dist.get(p["tier"], 0) + 1

    batch_id = args.batch_id or f"BATCH-{args.domain.upper()[:3]}-{sha256(root)[:12]}"

    batch_data = {
        "batch_id": batch_id,
        "domain": args.domain,
        "pairs": pairs,
        "merkle_root": root,
        "gate_pass_rate": round(gate_pass_rate, 4),
        "avg_score": round(avg_score, 1),
        "tier_distribution": tier_dist,
        "audit_timestamp": "",  # Will default to now
        "contamination_rate": 0.0,
        "think_tags_found": 0,
    }

    print(f"Batch: {batch_id}")
    print(f"Pairs: {len(pairs)}")
    print(f"Root:  {root}")
    print(f"Score: {avg_score:.1f}")
    print(f"Tiers: {tier_dist}")

    if args.dry_run:
        print("[DRY RUN] Would POST to", LEDGER_URL)
        return

    result = post_batch(batch_data)
    print(f"Registered: {result}")


if __name__ == "__main__":
    main()

# Lessons — Hive Ledger

Patterns learned from mistakes. Review at session start.

## 2026-03-14

### 1. Merkle Proof Double-Hashing
**Bug:** `verifyProof()` hashes its input internally, but the `/api/verify` endpoint pre-hashed the fingerprint before passing it in. Result: double-hash → `valid: false`.
**Fix:** When fingerprint param is provided, pass raw fingerprint to `verifyProof()` (it hashes internally). When leaf (pre-hashed) is provided, walk the proof without initial hash.
**Rule:** When composing crypto functions, trace the hash chain end-to-end before shipping. If function A hashes internally, never pre-hash before calling A.

### 2. Test Data vs Real Data
**Pattern:** Test batch had a made-up `merkle_root` that didn't match the computed root from actual fingerprints. This made verification appear broken when it was actually correct.
**Rule:** When testing crypto verification, always use correctly computed values — never fake hashes. Compute the real root from the test fingerprints.

### 3. D1 Account Auth
**Pattern:** `wrangler d1 list` worked but `wrangler d1 create` initially failed with auth error on first attempt, then succeeded on retry from project directory.
**Rule:** Always run wrangler commands from the project directory with `CLOUDFLARE_ACCOUNT_ID` set. Retry once before investigating — transient auth issues happen.

### 4. Cloudflare Bot Fight Mode vs Python urllib
**Bug:** Python `urllib.request` with default User-Agent (`Python-urllib/3.x`) triggers Cloudflare Bot Fight Mode → HTTP 403 error 1010. Curl works fine from the same server.
**Fix:** Set `User-Agent: SwarmCookAuditor/1.0` (or any non-default UA) in urllib headers.
**Rule:** Any Python HTTP client hitting a Cloudflare-proxied endpoint MUST set a custom User-Agent header. Default Python UAs are flagged by Bot Fight Mode. Test with `python3 -c "urllib..."` before deploying cron jobs.

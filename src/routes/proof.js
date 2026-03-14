/**
 * Merkle Proof Endpoints
 * GET /api/proof/:pair_id — Generate inclusion proof
 * GET /api/verify         — Verify a proof
 */

import { ok, badRequest, notFound } from '../utils/response.js';
import { computeMerkleRoot, generateProof, verifyProof, sha256 } from '../services/merkle.js';

export async function handleProof(request, env, url) {
  const path = url.pathname;

  // GET /api/verify
  if (path === '/api/verify') {
    return await verify(url);
  }

  // GET /api/proof/:pair_id
  if (path.match(/^\/api\/proof\/[^/]+$/)) {
    const pairId = path.split('/')[3];
    return await proof(pairId, env);
  }

  return notFound();
}

async function proof(pairId, env) {
  // Find the pair
  const pair = await env.DB.prepare(
    'SELECT pair_id, fingerprint FROM pairs WHERE pair_id = ?'
  ).bind(pairId).first();

  if (!pair) return notFound('Pair not found');

  // Find its batch
  const junction = await env.DB.prepare(
    'SELECT batch_id, line_number FROM batch_pairs WHERE pair_id = ?'
  ).bind(pairId).first();

  if (!junction) return notFound('Pair not in any batch');

  // Get batch merkle root
  const batch = await env.DB.prepare(
    'SELECT batch_id, merkle_root FROM batches WHERE batch_id = ?'
  ).bind(junction.batch_id).first();

  if (!batch) return notFound('Batch not found');

  // Load fingerprint manifest from R2
  let fingerprints = null;
  if (env.MANIFESTS) {
    const obj = await env.MANIFESTS.get(`manifests/${batch.batch_id}.json`);
    if (obj) {
      const manifest = await obj.json();
      fingerprints = manifest.fingerprints;
    }
  }

  // Fallback: load fingerprints from D1
  if (!fingerprints) {
    const fps = await env.DB.prepare(`
      SELECT p.fingerprint FROM batch_pairs bp
      JOIN pairs p ON bp.pair_id = p.pair_id
      WHERE bp.batch_id = ?
      ORDER BY bp.line_number ASC
    `).bind(batch.batch_id).all();

    fingerprints = (fps?.results || []).map(r => r.fingerprint);
  }

  // Find target index
  const targetIndex = fingerprints.indexOf(pair.fingerprint);
  if (targetIndex === -1) {
    return notFound('Fingerprint not found in batch manifest');
  }

  // Generate proof and compute actual root
  const proofSteps = await generateProof(fingerprints, targetIndex);
  const leaf = await sha256(pair.fingerprint);
  const computedRoot = await computeMerkleRoot(fingerprints);

  // Verify the proof against the computed root
  const verified = await verifyProof(pair.fingerprint, proofSteps, computedRoot);

  // Get anchor info
  const anchor = await env.DB.prepare(
    'SELECT hedera_tx, verify_url FROM anchors WHERE batch_id = ?'
  ).bind(batch.batch_id).first();

  return ok({
    pair_id: pairId,
    leaf,
    fingerprint: pair.fingerprint,
    proof: proofSteps,
    merkle_root: computedRoot,
    batch_id: batch.batch_id,
    verified,
    anchor: anchor ? {
      hedera_tx: anchor.hedera_tx,
      verify_url: anchor.verify_url,
    } : null,
  });
}

async function verify(url) {
  const leaf = url.searchParams.get('leaf');
  const proofParam = url.searchParams.get('proof');
  const root = url.searchParams.get('root');
  const fingerprint = url.searchParams.get('fingerprint');

  if (!root) return badRequest('Required: root');
  if (!leaf && !fingerprint) return badRequest('Required: leaf or fingerprint');

  let proofSteps;
  try {
    proofSteps = JSON.parse(proofParam || '[]');
  } catch {
    return badRequest('proof must be valid JSON array');
  }

  // verifyProof hashes its first arg, so pass raw fingerprint directly.
  // If only leaf (pre-hashed) is provided, walk proof without initial hash.
  let valid;
  let leafHash;

  if (fingerprint) {
    // verifyProof will sha256(fingerprint) internally
    valid = await verifyProof(fingerprint, proofSteps, root);
    leafHash = await sha256(fingerprint);
  } else {
    // leaf is already hashed — walk proof from here
    let current = leaf;
    for (const step of proofSteps) {
      if (step.position === 'left') {
        current = await sha256(step.hash + current);
      } else {
        current = await sha256(current + step.hash);
      }
    }
    valid = current === root;
    leafHash = leaf;
  }

  return ok({
    valid,
    leaf: leafHash,
    root,
    proof_steps: proofSteps.length,
  });
}

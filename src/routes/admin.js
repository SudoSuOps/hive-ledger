/**
 * Admin Write Endpoints — X-Admin-Key authenticated
 * POST /api/admin/batch   — Register batch from cook auditor
 * POST /api/admin/signal  — Register signal from SwarmRadar
 * POST /api/admin/anchor  — Record Hedera anchor
 * POST /api/admin/model   — Record model training lineage
 * POST /api/admin/order   — Record revenue/fulfillment
 * POST /api/admin/quarantine — Quarantine contaminated pairs
 * POST /api/admin/sync    — Bulk sync from SQLite export
 */

import { ok, created, badRequest, notFound } from '../utils/response.js';

export async function handleAdmin(request, env, url) {
  const path = url.pathname;
  const body = await request.json().catch(() => null);
  if (!body) return badRequest('Invalid JSON body');

  // POST /api/admin/batch
  if (path === '/api/admin/batch') {
    return await registerBatch(body, env);
  }

  // POST /api/admin/signal
  if (path === '/api/admin/signal') {
    return await registerSignal(body, env);
  }

  // POST /api/admin/anchor
  if (path === '/api/admin/anchor') {
    return await registerAnchor(body, env);
  }

  // POST /api/admin/model
  if (path === '/api/admin/model') {
    return await registerModel(body, env);
  }

  // POST /api/admin/order
  if (path === '/api/admin/order') {
    return await registerOrder(body, env);
  }

  // POST /api/admin/quarantine
  if (path === '/api/admin/quarantine') {
    return await quarantinePairs(body, env);
  }

  // POST /api/admin/sync
  if (path === '/api/admin/sync') {
    return await bulkSync(body, env);
  }

  return notFound('Unknown admin endpoint');
}

async function registerBatch(body, env) {
  const { batch_id, domain, pairs, merkle_root, gate_pass_rate, avg_score,
          tier_distribution, audit_timestamp, contamination_rate, think_tags_found } = body;

  if (!batch_id || !domain || !pairs || !merkle_root) {
    return badRequest('Required: batch_id, domain, pairs[], merkle_root');
  }

  // Insert batch
  await env.DB.prepare(`
    INSERT OR REPLACE INTO batches
    (batch_id, domain, pair_count, merkle_root, gate_pass_rate, avg_score,
     tier_distribution, audit_timestamp, contamination_rate, think_tags_found)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    batch_id, domain, pairs.length, merkle_root,
    gate_pass_rate || 0, avg_score || 0,
    JSON.stringify(tier_distribution || {}),
    audit_timestamp || new Date().toISOString(),
    contamination_rate || 0, think_tags_found || 0
  ).run();

  // Insert pairs + junction in batched statements
  const pairStmt = env.DB.prepare(`
    INSERT OR REPLACE INTO pairs
    (pair_id, fingerprint, signal_id, domain, task_type, score, tier,
     source_confidence, gate_integrity, reasoning_depth, entropy_health, fingerprint_uniqueness,
     gate_json_valid, gate_output_length, gate_numeric_verify, gate_concept_present, gate_dedup, gate_degenerate,
     gates_passed, adversarial_detected, adversarial_type, adversarial_penalty,
     gen_model, cook_script, source_file, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const junctionStmt = env.DB.prepare(`
    INSERT OR REPLACE INTO batch_pairs (batch_id, pair_id, line_number) VALUES (?, ?, ?)
  `);

  // Store fingerprints for Merkle proof reconstruction
  const fingerprints = [];

  // D1 batch limit is 100 statements per batch
  const BATCH_SIZE = 50; // 2 stmts per pair = 100 per batch
  for (let i = 0; i < pairs.length; i += BATCH_SIZE) {
    const chunk = pairs.slice(i, i + BATCH_SIZE);
    const stmts = [];

    for (let j = 0; j < chunk.length; j++) {
      const p = chunk[j];
      const lineNum = i + j;
      fingerprints.push(p.fingerprint);

      stmts.push(pairStmt.bind(
        p.pair_id, p.fingerprint, p.signal_id || '', p.domain || domain, p.task_type || '',
        p.score || 0, p.tier || 'propolis',
        p.source_confidence || 0, p.gate_integrity || 0, p.reasoning_depth || 0,
        p.entropy_health || 0, p.fingerprint_uniqueness || 0,
        p.gate_json_valid ?? 1, p.gate_output_length ?? 1, p.gate_numeric_verify ?? 1,
        p.gate_concept_present ?? 1, p.gate_dedup ?? 1, p.gate_degenerate ?? 1,
        p.gates_passed ?? 6, p.adversarial_detected ?? 0, p.adversarial_type || '',
        p.adversarial_penalty || 0, p.gen_model || '', p.cook_script || '',
        p.source_file || '', p.status || 'active'
      ));

      stmts.push(junctionStmt.bind(batch_id, p.pair_id, lineNum));
    }

    await env.DB.batch(stmts);
  }

  // Store fingerprint array in R2 for proof reconstruction
  if (env.MANIFESTS) {
    await env.MANIFESTS.put(
      `manifests/${batch_id}.json`,
      JSON.stringify({ batch_id, merkle_root, fingerprints })
    );
  }

  return created({
    batch_id,
    pairs_registered: pairs.length,
    merkle_root,
    manifest_stored: !!env.MANIFESTS,
  });
}

async function registerSignal(body, env) {
  const { signal_id, source_worker, source_weight, domain, collected_at } = body;
  if (!signal_id || !source_worker || !domain || !collected_at) {
    return badRequest('Required: signal_id, source_worker, domain, collected_at');
  }

  await env.DB.prepare(`
    INSERT OR REPLACE INTO signals
    (signal_id, source_worker, source_url, source_weight, domain, title, event_type,
     vertical, priority, entity_count, radar_score, radar_decision, corroboration_key,
     collected_at, hedera_tx)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    signal_id, source_worker, body.source_url || '', source_weight || 0,
    domain, body.title || '', body.event_type || '', body.vertical || '',
    body.priority || 3, body.entity_count || 0, body.radar_score || 0,
    body.radar_decision || '', body.corroboration_key || '',
    collected_at, body.hedera_tx || ''
  ).run();

  return created({ signal_id, registered: true });
}

async function registerAnchor(body, env) {
  const { batch_id, merkle_root, hedera_tx } = body;
  if (!batch_id || !merkle_root || !hedera_tx) {
    return badRequest('Required: batch_id, merkle_root, hedera_tx');
  }

  await env.DB.prepare(`
    INSERT INTO anchors
    (batch_id, merkle_root, hedera_tx, hedera_topic, hedera_sequence,
     hedera_network, verify_url, cell_id, cell_score)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    batch_id, merkle_root, hedera_tx,
    body.hedera_topic || '', body.hedera_sequence || 0,
    body.hedera_network || 'mainnet', body.verify_url || '',
    body.cell_id || '', body.cell_score || 0
  ).run();

  return created({ batch_id, hedera_tx, anchored: true });
}

async function registerModel(body, env) {
  const { model_id, batch_id } = body;
  if (!model_id || !batch_id) {
    return badRequest('Required: model_id, batch_id');
  }

  await env.DB.prepare(`
    INSERT OR REPLACE INTO model_lineage
    (model_id, batch_id, training_run_id, pairs_used, loss, eval_loss, trained_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).bind(
    model_id, batch_id, body.training_run_id || '',
    body.pairs_used || 0, body.loss || 0, body.eval_loss || 0,
    body.trained_at || ''
  ).run();

  return created({ model_id, batch_id, registered: true });
}

async function registerOrder(body, env) {
  const { order_id, customer_id, pair_count, amount_cents, domain } = body;
  if (!order_id || !customer_id || !pair_count || !amount_cents || !domain) {
    return badRequest('Required: order_id, customer_id, pair_count, amount_cents, domain');
  }

  await env.DB.prepare(`
    INSERT OR REPLACE INTO revenue
    (order_id, customer_id, pair_count, amount_cents, domain,
     tier_minimum, batch_ids, fulfilled_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    order_id, customer_id, pair_count, amount_cents, domain,
    body.tier_minimum || '', JSON.stringify(body.batch_ids || []),
    body.fulfilled_at || ''
  ).run();

  return created({ order_id, registered: true });
}

async function quarantinePairs(body, env) {
  const { pair_ids, reason } = body;
  if (!pair_ids || !Array.isArray(pair_ids) || pair_ids.length === 0) {
    return badRequest('Required: pair_ids[] (non-empty array)');
  }

  // D1 doesn't support IN with bound params well, batch individual updates
  const stmt = env.DB.prepare('UPDATE pairs SET status = ? WHERE pair_id = ?');
  const BATCH_SIZE = 100;

  let updated = 0;
  for (let i = 0; i < pair_ids.length; i += BATCH_SIZE) {
    const chunk = pair_ids.slice(i, i + BATCH_SIZE);
    const stmts = chunk.map(id => stmt.bind('quarantined', id));
    await env.DB.batch(stmts);
    updated += chunk.length;
  }

  return ok({ quarantined: updated, reason: reason || '' });
}

async function bulkSync(body, env) {
  const { batches, pairs } = body;

  let batchCount = 0;
  let pairCount = 0;

  // Insert batches
  if (batches && Array.isArray(batches)) {
    const stmt = env.DB.prepare(`
      INSERT OR REPLACE INTO batches
      (batch_id, domain, pair_count, merkle_root, gate_pass_rate, avg_score,
       tier_distribution, audit_timestamp, contamination_rate, think_tags_found)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (let i = 0; i < batches.length; i += 100) {
      const chunk = batches.slice(i, i + 100);
      const stmts = chunk.map(b => stmt.bind(
        b.batch_id, b.domain, b.pair_count, b.merkle_root,
        b.gate_pass_rate || 0, b.avg_score || 0,
        JSON.stringify(b.tier_distribution || {}),
        b.audit_timestamp || '', b.contamination_rate || 0, b.think_tags_found || 0
      ));
      await env.DB.batch(stmts);
      batchCount += chunk.length;
    }
  }

  // Insert pairs
  if (pairs && Array.isArray(pairs)) {
    const stmt = env.DB.prepare(`
      INSERT OR REPLACE INTO pairs
      (pair_id, fingerprint, signal_id, domain, task_type, score, tier,
       source_confidence, gate_integrity, reasoning_depth, entropy_health, fingerprint_uniqueness,
       gate_json_valid, gate_output_length, gate_numeric_verify, gate_concept_present, gate_dedup, gate_degenerate,
       gates_passed, gen_model, cook_script, source_file, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (let i = 0; i < pairs.length; i += 100) {
      const chunk = pairs.slice(i, i + 100);
      const stmts = chunk.map(p => stmt.bind(
        p.pair_id, p.fingerprint, p.signal_id || '', p.domain, p.task_type || '',
        p.score || 0, p.tier || 'propolis',
        p.source_confidence || 0, p.gate_integrity || 0, p.reasoning_depth || 0,
        p.entropy_health || 0, p.fingerprint_uniqueness || 0,
        p.gate_json_valid ?? 1, p.gate_output_length ?? 1, p.gate_numeric_verify ?? 1,
        p.gate_concept_present ?? 1, p.gate_dedup ?? 1, p.gate_degenerate ?? 1,
        p.gates_passed ?? 6, p.gen_model || '', p.cook_script || '',
        p.source_file || '', p.status || 'active'
      ));
      await env.DB.batch(stmts);
      pairCount += chunk.length;
    }
  }

  return ok({ synced_batches: batchCount, synced_pairs: pairCount });
}

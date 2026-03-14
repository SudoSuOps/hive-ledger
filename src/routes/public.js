/**
 * Public Read Endpoints — No auth required
 * GET /api/health, /api/summary, /api/pairs, /api/batches, etc.
 */

import { ok, notFound } from '../utils/response.js';
import { parsePagination, paginatedResult } from '../utils/pagination.js';

export async function handlePublic(request, env, url) {
  const path = url.pathname;

  // GET /api/health
  if (path === '/api/health') {
    return await health(env);
  }

  // GET /api/summary
  if (path === '/api/summary') {
    return await summary(env);
  }

  // GET /api/pairs/:pair_id/trace
  if (path.match(/^\/api\/pairs\/[^/]+\/trace$/)) {
    const pairId = path.split('/')[3];
    return await pairTrace(pairId, env);
  }

  // GET /api/pairs/:pair_id
  if (path.match(/^\/api\/pairs\/[^/]+$/) && !path.endsWith('/pairs')) {
    const pairId = path.split('/')[3];
    return await pairDetail(pairId, env);
  }

  // GET /api/pairs
  if (path === '/api/pairs') {
    return await listPairs(url, env);
  }

  // GET /api/batches/:batch_id
  if (path.match(/^\/api\/batches\/[^/]+$/) && !path.endsWith('/batches')) {
    const batchId = path.split('/')[3];
    return await batchDetail(batchId, env);
  }

  // GET /api/batches
  if (path === '/api/batches') {
    return await listBatches(url, env);
  }

  // GET /api/signals
  if (path === '/api/signals') {
    return await listSignals(url, env);
  }

  // GET /api/anchors
  if (path === '/api/anchors') {
    return await listAnchors(url, env);
  }

  // GET /api/tiers
  if (path === '/api/tiers') {
    return await tierBreakdown(env);
  }

  // GET /api/domains
  if (path === '/api/domains') {
    return await domainBreakdown(env);
  }

  return notFound();
}

// --- Implementations ---

async function health(env) {
  const [pairs, batches, anchors] = await Promise.all([
    env.DB.prepare('SELECT COUNT(*) as cnt FROM pairs WHERE status = ?').bind('active').first(),
    env.DB.prepare('SELECT COUNT(*) as cnt FROM batches').first(),
    env.DB.prepare('SELECT COUNT(DISTINCT batch_id) as cnt FROM anchors').first(),
  ]);

  const pairCount = pairs?.cnt || 0;
  const batchCount = batches?.cnt || 0;
  const anchorCount = anchors?.cnt || 0;

  return ok({
    status: 'ok',
    protocol: 'Royal Jelly Protocol v1 (RJP-1)',
    pairs_count: pairCount,
    batches_count: batchCount,
    anchor_coverage: batchCount > 0 ? +(anchorCount / batchCount).toFixed(4) : 0,
    version: '1.0.0',
    by: 'Swarm & Bee LLC',
  });
}

async function summary(env) {
  const [totals, tierDist, domainDist, recentBatches, anchorCount, signalCount] = await Promise.all([
    env.DB.prepare(`
      SELECT COUNT(*) as total_pairs,
             AVG(score) as avg_score,
             SUM(CASE WHEN status = 'quarantined' THEN 1 ELSE 0 END) as quarantined
      FROM pairs
    `).first(),
    env.DB.prepare(`
      SELECT tier, COUNT(*) as count, AVG(score) as avg_score
      FROM pairs WHERE status = 'active'
      GROUP BY tier ORDER BY avg_score DESC
    `).all(),
    env.DB.prepare(`
      SELECT domain, COUNT(*) as count, AVG(score) as avg_score
      FROM pairs WHERE status = 'active'
      GROUP BY domain ORDER BY count DESC
    `).all(),
    env.DB.prepare(`
      SELECT batch_id, domain, pair_count, merkle_root, avg_score, created_at
      FROM batches ORDER BY created_at DESC LIMIT 10
    `).all(),
    env.DB.prepare('SELECT COUNT(DISTINCT batch_id) as cnt FROM anchors').first(),
    env.DB.prepare('SELECT COUNT(*) as cnt FROM signals').first(),
  ]);

  const batchTotal = await env.DB.prepare('SELECT COUNT(*) as cnt FROM batches').first();

  return ok({
    total_pairs: totals?.total_pairs || 0,
    avg_score: totals?.avg_score ? +totals.avg_score.toFixed(1) : 0,
    quarantined: totals?.quarantined || 0,
    total_batches: batchTotal?.cnt || 0,
    total_signals: signalCount?.cnt || 0,
    anchor_coverage: batchTotal?.cnt > 0 ? +((anchorCount?.cnt || 0) / batchTotal.cnt).toFixed(4) : 0,
    tiers: tierDist?.results || [],
    domains: domainDist?.results || [],
    recent_batches: recentBatches?.results || [],
  });
}

async function pairDetail(pairId, env) {
  const pair = await env.DB.prepare('SELECT * FROM pairs WHERE pair_id = ?').bind(pairId).first();
  if (!pair) return notFound('Pair not found');

  // Get batch membership
  const batchRow = await env.DB.prepare(`
    SELECT b.batch_id, b.merkle_root, b.domain, b.pair_count, b.avg_score, b.created_at
    FROM batch_pairs bp JOIN batches b ON bp.batch_id = b.batch_id
    WHERE bp.pair_id = ?
  `).bind(pairId).first();

  // Get anchor if exists
  let anchor = null;
  if (batchRow) {
    anchor = await env.DB.prepare(`
      SELECT hedera_tx, hedera_topic, hedera_network, verify_url, anchored_at
      FROM anchors WHERE batch_id = ?
    `).bind(batchRow.batch_id).first();
  }

  return ok({
    ...pair,
    batch: batchRow || null,
    anchor: anchor || null,
    jelly_score: {
      total: pair.score,
      tier: pair.tier,
      components: {
        source_confidence: pair.source_confidence,
        gate_integrity: pair.gate_integrity,
        reasoning_depth: pair.reasoning_depth,
        entropy_health: pair.entropy_health,
        fingerprint_uniqueness: pair.fingerprint_uniqueness,
      },
      gates: {
        json_valid: !!pair.gate_json_valid,
        output_length: !!pair.gate_output_length,
        numeric_verify: !!pair.gate_numeric_verify,
        concept_present: !!pair.gate_concept_present,
        dedup: !!pair.gate_dedup,
        degenerate: !!pair.gate_degenerate,
        passed: pair.gates_passed,
      },
      adversarial: {
        detected: !!pair.adversarial_detected,
        type: pair.adversarial_type,
        penalty: pair.adversarial_penalty,
      },
    },
  });
}

async function pairTrace(pairId, env) {
  // Full provenance trace: signal → pair → batch → anchor → model → revenue
  const pair = await env.DB.prepare('SELECT * FROM pairs WHERE pair_id = ?').bind(pairId).first();
  if (!pair) return notFound('Pair not found');

  const [signal, batchRow, models] = await Promise.all([
    pair.signal_id
      ? env.DB.prepare('SELECT * FROM signals WHERE signal_id = ?').bind(pair.signal_id).first()
      : null,
    env.DB.prepare(`
      SELECT b.* FROM batch_pairs bp JOIN batches b ON bp.batch_id = b.batch_id
      WHERE bp.pair_id = ?
    `).bind(pairId).first(),
    env.DB.prepare(`
      SELECT ml.* FROM model_lineage ml
      JOIN batch_pairs bp ON ml.batch_id = bp.batch_id
      WHERE bp.pair_id = ?
    `).bind(pairId).all(),
  ]);

  let anchor = null;
  let revenue = null;
  if (batchRow) {
    [anchor, revenue] = await Promise.all([
      env.DB.prepare('SELECT * FROM anchors WHERE batch_id = ?').bind(batchRow.batch_id).first(),
      env.DB.prepare(`
        SELECT r.* FROM revenue r
        WHERE r.batch_ids LIKE ?
      `).bind(`%${batchRow.batch_id}%`).first(),
    ]);
  }

  return ok({
    trace: {
      signal: signal || null,
      pair: {
        pair_id: pair.pair_id,
        fingerprint: pair.fingerprint,
        domain: pair.domain,
        score: pair.score,
        tier: pair.tier,
        status: pair.status,
        created_at: pair.created_at,
      },
      batch: batchRow ? {
        batch_id: batchRow.batch_id,
        merkle_root: batchRow.merkle_root,
        pair_count: batchRow.pair_count,
        avg_score: batchRow.avg_score,
        created_at: batchRow.created_at,
      } : null,
      anchor: anchor ? {
        hedera_tx: anchor.hedera_tx,
        hedera_network: anchor.hedera_network,
        verify_url: anchor.verify_url,
        anchored_at: anchor.anchored_at,
      } : null,
      models: models?.results || [],
      revenue: revenue || null,
    },
  });
}

async function listPairs(url, env) {
  const { limit, cursor } = parsePagination(url);
  const domain = url.searchParams.get('domain');
  const tier = url.searchParams.get('tier');
  const status = url.searchParams.get('status') || 'active';

  let sql = 'SELECT pair_id, fingerprint, domain, task_type, score, tier, gates_passed, status, created_at FROM pairs WHERE status = ?';
  const binds = [status];

  if (domain) { sql += ' AND domain = ?'; binds.push(domain); }
  if (tier) { sql += ' AND tier = ?'; binds.push(tier); }
  if (cursor) { sql += ' AND created_at < ?'; binds.push(cursor); }

  sql += ' ORDER BY created_at DESC LIMIT ?';
  binds.push(limit + 1);

  const rows = await env.DB.prepare(sql).bind(...binds).all();
  const result = paginatedResult(rows?.results || [], limit);

  return ok(result);
}

async function listBatches(url, env) {
  const { limit, cursor } = parsePagination(url);
  const domain = url.searchParams.get('domain');

  let sql = 'SELECT * FROM batches WHERE 1=1';
  const binds = [];

  if (domain) { sql += ' AND domain = ?'; binds.push(domain); }
  if (cursor) { sql += ' AND created_at < ?'; binds.push(cursor); }

  sql += ' ORDER BY created_at DESC LIMIT ?';
  binds.push(limit + 1);

  const rows = await env.DB.prepare(sql).bind(...binds).all();
  const result = paginatedResult(rows?.results || [], limit);

  return ok(result);
}

async function batchDetail(batchId, env) {
  const batch = await env.DB.prepare('SELECT * FROM batches WHERE batch_id = ?').bind(batchId).first();
  if (!batch) return notFound('Batch not found');

  const anchor = await env.DB.prepare(
    'SELECT * FROM anchors WHERE batch_id = ?'
  ).bind(batchId).first();

  return ok({
    ...batch,
    tier_distribution: JSON.parse(batch.tier_distribution || '{}'),
    anchor: anchor || null,
  });
}

async function listSignals(url, env) {
  const { limit, cursor } = parsePagination(url);
  const domain = url.searchParams.get('domain');
  const decision = url.searchParams.get('decision');

  let sql = 'SELECT * FROM signals WHERE 1=1';
  const binds = [];

  if (domain) { sql += ' AND domain = ?'; binds.push(domain); }
  if (decision) { sql += ' AND radar_decision = ?'; binds.push(decision); }
  if (cursor) { sql += ' AND created_at < ?'; binds.push(cursor); }

  sql += ' ORDER BY created_at DESC LIMIT ?';
  binds.push(limit + 1);

  const rows = await env.DB.prepare(sql).bind(...binds).all();
  const result = paginatedResult(rows?.results || [], limit);

  return ok(result);
}

async function listAnchors(url, env) {
  const { limit } = parsePagination(url);

  const rows = await env.DB.prepare(`
    SELECT a.*, b.domain, b.pair_count, b.avg_score
    FROM anchors a JOIN batches b ON a.batch_id = b.batch_id
    ORDER BY a.anchored_at DESC LIMIT ?
  `).bind(limit).all();

  return ok({ anchors: rows?.results || [] });
}

async function tierBreakdown(env) {
  const rows = await env.DB.prepare(`
    SELECT tier, COUNT(*) as count, AVG(score) as avg_score,
           MIN(score) as min_score, MAX(score) as max_score
    FROM pairs WHERE status = 'active'
    GROUP BY tier ORDER BY avg_score DESC
  `).all();

  return ok({ tiers: rows?.results || [] });
}

async function domainBreakdown(env) {
  const rows = await env.DB.prepare(`
    SELECT p.domain, COUNT(*) as pair_count, AVG(p.score) as avg_score,
           COUNT(DISTINCT bp.batch_id) as batch_count
    FROM pairs p
    LEFT JOIN batch_pairs bp ON p.pair_id = bp.pair_id
    WHERE p.status = 'active'
    GROUP BY p.domain ORDER BY pair_count DESC
  `).all();

  // Get anchor coverage per domain
  const anchored = await env.DB.prepare(`
    SELECT b.domain, COUNT(DISTINCT a.batch_id) as anchored_batches
    FROM anchors a JOIN batches b ON a.batch_id = b.batch_id
    GROUP BY b.domain
  `).all();

  const anchorMap = {};
  for (const r of (anchored?.results || [])) {
    anchorMap[r.domain] = r.anchored_batches;
  }

  const domains = (rows?.results || []).map(d => ({
    ...d,
    avg_score: d.avg_score ? +d.avg_score.toFixed(1) : 0,
    anchored_batches: anchorMap[d.domain] || 0,
  }));

  return ok({ domains });
}

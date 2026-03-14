/**
 * Hive Ledger — Unified Provenance Protocol
 * Swarm & Bee LLC
 *
 * Single source of truth for the Royal Jelly Protocol chain:
 * Signal → Pair → Batch → Anchor → Model → Revenue
 */

import { handlePublic } from './routes/public.js';
import { handleAdmin } from './routes/admin.js';
import { handleProof } from './routes/proof.js';
import { requireAdmin } from './middleware/auth.js';
import { ok, unauthorized, forbidden, notFound, serverError } from './utils/response.js';

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-Admin-Key, X-API-Key',
    };

    if (method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      let response;

      // Dashboard
      if (path === '/' || path === '/index.html') {
        response = await serveDashboard(env);
      }
      // Admin write endpoints (POST /api/admin/*)
      else if (path.startsWith('/api/admin') && method === 'POST') {
        const auth = await requireAdmin(request, env);
        if (!auth.ok) {
          response = unauthorized(auth.error);
        } else {
          response = await handleAdmin(request, env, url);
        }
      }
      // Merkle proof endpoints
      else if (path.startsWith('/api/proof') || path === '/api/verify') {
        response = await handleProof(request, env, url);
      }
      // Public read endpoints
      else if (path.startsWith('/api/')) {
        response = await handlePublic(request, env, url);
      }
      // Root info
      else {
        response = notFound();
      }

      // Add CORS headers
      const finalHeaders = new Headers(response.headers);
      Object.entries(corsHeaders).forEach(([k, v]) => finalHeaders.set(k, v));

      return new Response(response.body, {
        status: response.status,
        headers: finalHeaders,
      });

    } catch (err) {
      console.error('Worker error:', err);
      const errResp = serverError('Internal server error');
      const finalHeaders = new Headers(errResp.headers);
      Object.entries(corsHeaders).forEach(([k, v]) => finalHeaders.set(k, v));
      return new Response(errResp.body, { status: 500, headers: finalHeaders });
    }
  },
};

async function serveDashboard(env) {
  // Serve static dashboard HTML
  return new Response(DASHBOARD_HTML, {
    status: 200,
    headers: { 'Content-Type': 'text/html; charset=utf-8' },
  });
}

// Dashboard HTML is inlined below to avoid R2 dependency for serving
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hive Ledger — Swarm & Bee</title>
<style>
  :root {
    --bg: #0a0a0f;
    --surface: #12121a;
    --border: #1e1e2e;
    --text: #e0e0e8;
    --dim: #6b6b80;
    --gold: #f0b429;
    --honey: #f59e0b;
    --green: #10b981;
    --red: #ef4444;
    --blue: #3b82f6;
    --purple: #8b5cf6;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'SF Mono', 'Fira Code', 'JetBrains Mono', monospace;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
  }
  .header {
    border-bottom: 1px solid var(--border);
    padding: 20px 32px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .header h1 {
    font-size: 18px;
    font-weight: 600;
    color: var(--gold);
    letter-spacing: 2px;
  }
  .header .protocol {
    font-size: 11px;
    color: var(--dim);
    letter-spacing: 1px;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
    padding: 24px 32px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
  }
  .card .label {
    font-size: 10px;
    color: var(--dim);
    letter-spacing: 1.5px;
    text-transform: uppercase;
    margin-bottom: 8px;
  }
  .card .value {
    font-size: 28px;
    font-weight: 700;
    color: var(--gold);
  }
  .card .sub {
    font-size: 11px;
    color: var(--dim);
    margin-top: 4px;
  }
  .section {
    padding: 0 32px 24px;
  }
  .section h2 {
    font-size: 13px;
    color: var(--dim);
    letter-spacing: 1.5px;
    text-transform: uppercase;
    margin-bottom: 12px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
  }
  th {
    font-size: 10px;
    color: var(--dim);
    letter-spacing: 1px;
    text-transform: uppercase;
    text-align: left;
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
  }
  td {
    font-size: 12px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
  }
  tr:last-child td { border-bottom: none; }
  .tier-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.5px;
  }
  .tier-royal_jelly { background: rgba(240,180,41,0.2); color: var(--gold); }
  .tier-honey { background: rgba(245,158,11,0.2); color: var(--honey); }
  .tier-pollen { background: rgba(16,185,129,0.2); color: var(--green); }
  .tier-propolis { background: rgba(107,107,128,0.2); color: var(--dim); }
  .status-dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 6px;
  }
  .status-ok { background: var(--green); }
  .status-warn { background: var(--honey); }
  .anchor-link {
    color: var(--blue);
    text-decoration: none;
    font-size: 11px;
  }
  .anchor-link:hover { text-decoration: underline; }
  .loading { color: var(--dim); font-size: 12px; padding: 20px; }
  .footer {
    border-top: 1px solid var(--border);
    padding: 16px 32px;
    font-size: 10px;
    color: var(--dim);
    text-align: center;
    letter-spacing: 1px;
  }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>HIVE LEDGER</h1>
    <div class="protocol">ROYAL JELLY PROTOCOL v1 &mdash; UNIFIED PROVENANCE</div>
  </div>
  <div style="text-align: right">
    <div id="status" style="font-size: 12px;"><span class="status-dot status-ok"></span>LIVE</div>
    <div id="last-update" style="font-size: 10px; color: var(--dim); margin-top: 4px;"></div>
  </div>
</div>

<div class="grid" id="stats-grid">
  <div class="card"><div class="label">Total Pairs</div><div class="value" id="total-pairs">-</div><div class="sub" id="pairs-sub"></div></div>
  <div class="card"><div class="label">Batches</div><div class="value" id="total-batches">-</div><div class="sub" id="batches-sub"></div></div>
  <div class="card"><div class="label">Avg JellyScore</div><div class="value" id="avg-score">-</div><div class="sub" id="score-sub"></div></div>
  <div class="card"><div class="label">Anchor Coverage</div><div class="value" id="anchor-cov">-</div><div class="sub" id="anchor-sub"></div></div>
  <div class="card"><div class="label">Signals</div><div class="value" id="total-signals">-</div><div class="sub">SwarmRadar intelligence</div></div>
  <div class="card"><div class="label">Quarantined</div><div class="value" id="quarantined">-</div><div class="sub">Contamination hold</div></div>
</div>

<div class="section">
  <h2>Tier Distribution</h2>
  <table>
    <thead><tr><th>Tier</th><th>Count</th><th>Avg Score</th><th>Share</th></tr></thead>
    <tbody id="tier-body"><tr><td colspan="4" class="loading">Loading...</td></tr></tbody>
  </table>
</div>

<div class="section">
  <h2>Domain Breakdown</h2>
  <table>
    <thead><tr><th>Domain</th><th>Pairs</th><th>Batches</th><th>Avg Score</th><th>Anchored</th></tr></thead>
    <tbody id="domain-body"><tr><td colspan="5" class="loading">Loading...</td></tr></tbody>
  </table>
</div>

<div class="section">
  <h2>Recent Batches</h2>
  <table>
    <thead><tr><th>Batch</th><th>Domain</th><th>Pairs</th><th>Avg Score</th><th>Merkle Root</th><th>Sealed</th></tr></thead>
    <tbody id="batch-body"><tr><td colspan="6" class="loading">Loading...</td></tr></tbody>
  </table>
</div>

<div class="footer">
  SWARM & BEE LLC &mdash; HIVE LEDGER &mdash; ROYAL JELLY PROTOCOL v1 (RJP-1)
</div>

<script>
const API = location.origin;

async function load() {
  try {
    const [summary, tiers, domains] = await Promise.all([
      fetch(API + '/api/summary').then(r => r.json()),
      fetch(API + '/api/tiers').then(r => r.json()),
      fetch(API + '/api/domains').then(r => r.json()),
    ]);

    // Stats cards
    document.getElementById('total-pairs').textContent = fmt(summary.total_pairs);
    document.getElementById('total-batches').textContent = fmt(summary.total_batches);
    document.getElementById('avg-score').textContent = summary.avg_score || '-';
    document.getElementById('anchor-cov').textContent = (summary.anchor_coverage * 100).toFixed(1) + '%';
    document.getElementById('total-signals').textContent = fmt(summary.total_signals);
    document.getElementById('quarantined').textContent = fmt(summary.quarantined);
    document.getElementById('last-update').textContent = 'Updated ' + new Date().toLocaleTimeString();

    // Tier table
    const tierBody = document.getElementById('tier-body');
    if (tiers.tiers && tiers.tiers.length > 0) {
      const total = tiers.tiers.reduce((s, t) => s + t.count, 0);
      tierBody.innerHTML = tiers.tiers.map(t => '<tr>' +
        '<td><span class="tier-badge tier-' + t.tier + '">' + t.tier.replace('_', ' ').toUpperCase() + '</span></td>' +
        '<td>' + fmt(t.count) + '</td>' +
        '<td>' + (t.avg_score ? t.avg_score.toFixed(1) : '-') + '</td>' +
        '<td>' + (total > 0 ? ((t.count / total) * 100).toFixed(1) + '%' : '-') + '</td>' +
      '</tr>').join('');
    } else {
      tierBody.innerHTML = '<tr><td colspan="4" style="color:var(--dim)">No pairs registered yet</td></tr>';
    }

    // Domain table
    const domBody = document.getElementById('domain-body');
    if (domains.domains && domains.domains.length > 0) {
      domBody.innerHTML = domains.domains.map(d => '<tr>' +
        '<td style="text-transform:uppercase">' + d.domain + '</td>' +
        '<td>' + fmt(d.pair_count) + '</td>' +
        '<td>' + (d.batch_count || 0) + '</td>' +
        '<td>' + (d.avg_score || '-') + '</td>' +
        '<td>' + (d.anchored_batches || 0) + '</td>' +
      '</tr>').join('');
    } else {
      domBody.innerHTML = '<tr><td colspan="5" style="color:var(--dim)">No domains yet</td></tr>';
    }

    // Recent batches
    const batchBody = document.getElementById('batch-body');
    if (summary.recent_batches && summary.recent_batches.length > 0) {
      batchBody.innerHTML = summary.recent_batches.map(b => '<tr>' +
        '<td style="font-size:10px">' + b.batch_id.slice(0, 20) + '...</td>' +
        '<td style="text-transform:uppercase">' + b.domain + '</td>' +
        '<td>' + fmt(b.pair_count) + '</td>' +
        '<td>' + (b.avg_score ? b.avg_score.toFixed(1) : '-') + '</td>' +
        '<td style="font-size:9px;font-family:monospace;color:var(--dim)">' + b.merkle_root.slice(0, 16) + '...</td>' +
        '<td style="font-size:10px">' + new Date(b.created_at).toLocaleDateString() + '</td>' +
      '</tr>').join('');
    } else {
      batchBody.innerHTML = '<tr><td colspan="6" style="color:var(--dim)">No batches yet</td></tr>';
    }

  } catch (err) {
    console.error('Dashboard load error:', err);
    document.getElementById('status').innerHTML = '<span class="status-dot status-warn"></span>ERROR';
  }
}

function fmt(n) {
  if (n === undefined || n === null) return '-';
  return n.toLocaleString();
}

load();
setInterval(load, 30000);
</script>
</body>
</html>`;

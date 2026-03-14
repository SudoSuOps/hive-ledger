/**
 * Admin Auth Middleware
 * Validates X-Admin-Key header against SHA-256 hash stored in env.
 */

export async function requireAdmin(request, env) {
  const key = request.headers.get('X-Admin-Key');
  if (!key) {
    return { ok: false, error: 'X-Admin-Key header required' };
  }

  const hash = await sha256(key);
  if (hash !== env.ADMIN_KEY_HASH) {
    return { ok: false, error: 'Invalid admin key' };
  }

  return { ok: true };
}

async function sha256(str) {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

export { sha256 };

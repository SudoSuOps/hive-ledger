/**
 * JSON response helpers
 */

export function json(status, data, extraHeaders = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...extraHeaders,
    },
  });
}

export function ok(data) { return json(200, data); }
export function created(data) { return json(201, data); }
export function badRequest(msg) { return json(400, { error: msg }); }
export function unauthorized(msg) { return json(401, { error: msg || 'Unauthorized' }); }
export function forbidden(msg) { return json(403, { error: msg || 'Forbidden' }); }
export function notFound(msg) { return json(404, { error: msg || 'Not found' }); }
export function serverError(msg) { return json(500, { error: msg || 'Internal server error' }); }

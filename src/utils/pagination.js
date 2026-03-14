/**
 * Cursor-based pagination helper
 * Cursor = created_at ISO string (lexicographic sort)
 */

export function parsePagination(url) {
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);
  const cursor = url.searchParams.get('cursor') || null;
  return { limit, cursor };
}

export function paginatedResult(rows, limit) {
  const hasMore = rows.length > limit;
  const items = hasMore ? rows.slice(0, limit) : rows;
  const nextCursor = hasMore ? items[items.length - 1].created_at : null;
  return { items, next_cursor: nextCursor, has_more: hasMore };
}

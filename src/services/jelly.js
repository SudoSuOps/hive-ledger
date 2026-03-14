/**
 * JellyScore tier classification
 * Mirrors virgin-jelly/src/royal_jelly/protocol.py
 */

export const TIER_THRESHOLDS = {
  royal_jelly: 95,
  honey: 85,
  pollen: 70,
  propolis: 0,
};

export function classifyTier(score) {
  if (score >= TIER_THRESHOLDS.royal_jelly) return 'royal_jelly';
  if (score >= TIER_THRESHOLDS.honey) return 'honey';
  if (score >= TIER_THRESHOLDS.pollen) return 'pollen';
  return 'propolis';
}

export const SOURCE_WEIGHTS = {
  edgar: 0.90,
  fred: 0.90,
  human: 0.90,
  openalex: 0.85,
  arxiv: 0.80,
  zenodo: 0.85,
  github: 0.72,
  swarmjelly: 0.70,
  cre_news: 0.65,
  rss: 0.60,
  webhook: 0.60,
  hn: 0.50,
  reddit: 0.50,
  trending: 0.40,
};

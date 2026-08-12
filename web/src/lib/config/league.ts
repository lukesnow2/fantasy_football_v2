import { env } from '$env/dynamic/public';

/**
 * The Yahoo league people are actually playing in.
 *
 * Deliberately configuration rather than something derived from `edw.dim_league`.
 * The ETL's newest season is 2025 (`461.l.654923`), while the active league is
 * 744846 — a season Yahoo has but the warehouse has not ingested yet. Deriving
 * this from the warehouse would point everyone at last year for the whole
 * preseason, every year.
 *
 * `$env/dynamic/public` rather than `$env/static/public` so an unset variable
 * falls back instead of failing the build.
 */
export const YAHOO_LEAGUE_URL =
	env.PUBLIC_YAHOO_LEAGUE_URL || 'https://football.fantasysports.yahoo.com/f1/744846';

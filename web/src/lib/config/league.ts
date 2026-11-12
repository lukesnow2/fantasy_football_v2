import { env } from '$env/dynamic/public';

/**
 * The Yahoo league people are actually playing in.
 *
 * Yahoo's league-name slug (`/league/oakdale_park`) rather than a numbered
 * `/f1/<number>` URL: the number is reissued every August for the new season,
 * the slug is not. It always resolves to the current season's league, so this
 * needs no maintenance and cannot go stale mid-preseason.
 *
 * Deliberately configuration rather than something derived from `edw.dim_league`,
 * whose newest season is 2025 (`461.l.654923`) — the warehouse trails Yahoo by a
 * season, so deriving this would point everyone at last year all preseason.
 *
 * `$env/dynamic/public` rather than `$env/static/public` so an unset variable
 * falls back instead of failing the build.
 */
export const YAHOO_LEAGUE_URL =
	env.PUBLIC_YAHOO_LEAGUE_URL || 'https://football.fantasysports.yahoo.com/league/oakdale_park';

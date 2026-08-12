/**
 * Yahoo league URL helpers.
 *
 * `edw.dim_league.league_id` holds Yahoo's league key in `<gameKey>.l.<leagueNumber>`
 * form — e.g. `461.l.654923` for 2025, `124.l.109785` for 2005. The game key is the
 * sport-season id and is not part of any public URL; only the league number is.
 */

/** `461.l.654923` -> `654923`. Null for anything that isn't a Yahoo league key. */
export function yahooLeagueNumber(leagueId: string | null | undefined): string | null {
	const match = /^\d+\.l\.(\d+)$/.exec(leagueId ?? '');
	return match ? match[1] : null;
}

/**
 * The archive URL for a finished season. Yahoo keeps closed leagues under
 * `/archive/nfl/<year>/<leagueNumber>`; the current season lives at `/f1/<number>`
 * instead, which is why the active league is configured rather than derived
 * (see `$lib/config/league`).
 *
 * NOT WIRED UP YET, on purpose. Yahoo gates archived leagues behind a login, so
 * this URL shape could not be confirmed from here, and shipping 21 unverified
 * links would recreate the dead-end problem this work exists to fix. Open two of
 * them while signed in to Yahoo — 2019 (`390.l.777720`) and 2012
 * (`273.l.107980`) — and if they resolve, render them from the season selector
 * on /power-rankings, which already has the season in hand.
 *
 * Returns null when the league id is unparseable, so callers render no link at
 * all rather than a broken one.
 */
export function yahooArchiveUrl(
	leagueId: string | null | undefined,
	seasonYear: number
): string | null {
	const number = yahooLeagueNumber(leagueId);
	if (!number || !Number.isInteger(seasonYear)) return null;
	return `https://football.fantasysports.yahoo.com/archive/nfl/${seasonYear}/${number}`;
}

import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { SeasonLuck } from '$lib/manager-luck';
import { getSeasonOdds } from '$lib/server/playoff-odds-service';

/**
 * Season-level queries shared by /api/managers/performance (career rows and a
 * manager's season history) and /api/managers/seasons (the League Table's
 * season range), so the two can never disagree about a season.
 */

/**
 * Postgres `numeric` arrives over the wire as a string, so every average, rate
 * and points total in this payload was a string. Consumers coerced by accident
 * (`'0.4317' * 100` happens to work) right up until one of them called
 * `.toFixed()` and crashed the whole manager profile page. Coerce once, here,
 * so the API's contract is "numbers are numbers".
 */
export function num(value: unknown): number | null {
	if (value === null || value === undefined || value === '') return null;
	const parsed = typeof value === 'number' ? value : Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Each league's state, one grouped pass over fact_matchup: finished once its
 * championship game is in, bracketed once any playoff game is. Shared by the
 * luck rows and Season History so the two cannot disagree about a season.
 */
export const leagueState = sql`
	SELECT league_key,
		bool_or(is_championship) AS finished,
		bool_or(is_playoffs)     AS bracketed
	FROM edw.fact_matchup
	GROUP BY league_key
`;

/**
 * Each manager's expected (Pythagorean) wins and luck, one row per season,
 * taken from the power rankings' latest ranked week of that season: the final
 * regular-season standing once it is over, this week's while it is still
 * being played. Reused rather than recomputed so the manager pages and the
 * power rankings always show the same numbers.
 *
 * A season is in progress until its championship game is in fact_matchup.
 * Wins are regular-season only, like the expected wins they are compared with.
 * With `manager`, only that manager's seasons are read. `latestSeason` is the
 * latest season the power rankings have ranked, so the luck card can show it
 * between seasons. Not dim_league's latest: the new season's league is loaded
 * at the draft, weeks before a game, and every manager would have read
 * "Didn't play" until week 1 was ranked.
 *
 * Playoff odds for a season still being played are the This Season widget's
 * simulation (getSeasonOdds), not the power rankings' rank-and-games-back
 * formula, so a team's odds are the same number everywhere on the site.
 *
 * Once a playoff game is in (`bracketSet`), the regular season is over and the
 * team's bracket place (`madePlayoffs`) replaces its week-14 playoff odds. The
 * place comes from the final regular-season standings (record_rank within the
 * bracket size), not is_playoff_team: that flag marks teams that have PLAYED a
 * playoff game, so the top seeds, on a first-round bye, would read "missed"
 * for a week. With each league's own bracket size (its stored setting, else
 * what its finished bracket held), the standings rule matches the real bracket
 * for every team 2005-2025. The rankings cover every team in a league-week;
 * should a week ever rank fewer teams than the league has, the ranks below are
 * off and the flag is used instead.
 */
export async function loadLuckBySeason(
	manager: string | null,
	/**
	 * false: skip the live season's simulated odds (no simulation run) for a
	 * caller that doesn't show them; playoffOdds is then the stored value.
	 */
	{ odds: withOdds = true }: { odds?: boolean } = {}
): Promise<{ byManager: Map<string, SeasonLuck[]>; latestSeason: number | null }> {
	const [latestResult, luckResult] = await Promise.all([
		db.execute(sql`
			SELECT MAX(l.season_year) AS season_year
			FROM edw.mart_weekly_power_rankings p
			JOIN edw.dim_league l ON l.league_key = p.league_key
		`),
		db.execute(sql`
			WITH league_state AS (${leagueState}),
			-- Teams ranked each week: equal to the league's size only when no
			-- team was left out of the power rankings.
			ranked AS (
				SELECT league_key, week_key, COUNT(*) AS n
				FROM edw.mart_weekly_power_rankings
				GROUP BY league_key, week_key
			),
			-- Bracket size when the league's own setting isn't stored (migration
			-- 011). A finished league's is what it played: by the championship
			-- every bracket team has played. It varied (all 8 teams in 2007).
			bracket AS (
				SELECT fm.league_key, COUNT(DISTINCT v.team_key) AS n
				FROM edw.fact_matchup fm
				CROSS JOIN LATERAL (VALUES (fm.team1_key), (fm.team2_key)) AS v(team_key)
				WHERE fm.is_playoffs AND NOT fm.is_consolation
				  AND fm.league_key IN (
					SELECT c.league_key FROM edw.fact_matchup c WHERE c.is_championship
				  )
				GROUP BY fm.league_key
			),
			-- A league still in its playoffs uses the latest finished season's
			-- size (6 since 2008): its first-round byes have not played yet.
			spots AS (
				SELECT b.n FROM bracket b
				JOIN edw.dim_league bl ON bl.league_key = b.league_key
				ORDER BY bl.season_year DESC LIMIT 1
			),
			latest AS (
				SELECT
					t.manager_name AS manager_name,
					l.season_year  AS season_year,
					p.team_key     AS team_key,
					w.week_number  AS week_number,
					p.wins, p.ties, p.luck_factor, p.playoff_odds,
					-- Expected wins are all-play wins (WP15), so wins minus them is
					-- the luck. Before migration 019 the luck is Pythagorean, and so
					-- are the expected wins that go with it.
					COALESCE((to_jsonb(p) ->> 'all_play_wins')::numeric, p.pythagorean_wins)
						AS pythagorean_wins,
					p.record_rank, p.league_key, p.week_key, l.num_teams,
					-- to_jsonb: still runs against a warehouse not yet migrated.
					(to_jsonb(l) ->> 'num_playoff_teams')::int AS stored_spots,
					ROW_NUMBER() OVER (
						PARTITION BY p.league_key, p.team_key ORDER BY w.week_number DESC
					) AS rn
				FROM edw.mart_weekly_power_rankings p
				JOIN edw.dim_week w   ON w.week_key   = p.week_key
				JOIN edw.dim_team t   ON t.team_key   = p.team_key
				JOIN edw.dim_league l ON l.league_key = p.league_key
				WHERE t.manager_name IS NOT NULL
				${manager ? sql`AND t.manager_name = ${manager}` : sql``}
			)
			SELECT
				latest.manager_name, latest.season_year, latest.team_key, latest.week_number,
				latest.wins, latest.ties, latest.pythagorean_wins, latest.luck_factor,
				latest.playoff_odds,
				NOT COALESCE(ls.finished, false) AS in_progress,
				COALESCE(ls.bracketed, false) AS bracket_set,
				CASE
					WHEN ranked.n = latest.num_teams
						THEN COALESCE(
							latest.record_rank
								<= COALESCE(latest.stored_spots, bracket.n, (SELECT n FROM spots)),
							false)
					ELSE EXISTS (
						SELECT 1 FROM edw.fact_team_performance ftp
						WHERE ftp.team_key = latest.team_key AND ftp.is_playoff_team
					)
				END AS made_playoffs
			FROM latest
			LEFT JOIN league_state ls ON ls.league_key = latest.league_key
			LEFT JOIN ranked ON ranked.league_key = latest.league_key
				AND ranked.week_key = latest.week_key
			LEFT JOIN bracket ON bracket.league_key = latest.league_key
			WHERE latest.rn = 1
			ORDER BY latest.manager_name, latest.season_year
		`)
	]);
	const [latest] = Array.from(latestResult) as any[];
	const rows = Array.from(luckResult) as any[];

	const byManager = new Map<string, SeasonLuck[]>();
	for (const r of rows) {
		const name = String(r.managerName);
		const list = byManager.get(name) ?? [];
		list.push({
			seasonYear: Number(r.seasonYear),
			teamKey: Number(r.teamKey),
			weekNumber: Number(r.weekNumber),
			wins: Number(r.wins ?? 0),
			ties: Number(r.ties ?? 0),
			pythagoreanWins: num(r.pythagoreanWins),
			luckFactor: num(r.luckFactor),
			playoffOdds: num(r.playoffOdds),
			inProgress: Boolean(r.inProgress),
			bracketSet: Boolean(r.bracketSet),
			madePlayoffs: Boolean(r.madePlayoffs)
		});
		byManager.set(name, list);
	}

	if (!withOdds) return { byManager, latestSeason: num(latest?.seasonYear) };

	// Live odds from the simulation, one run per season still being played
	// (normally just the current one; cached across requests).
	const live = new Set(
		[...byManager.values()]
			.flat()
			.filter((s) => s.inProgress && !s.bracketSet)
			.map((s) => s.seasonYear)
	);
	for (const season of live) {
		let odds = new Map<number, number>();
		try {
			const result = await getSeasonOdds(season);
			if (result.available) odds = new Map(result.teams.map((t) => [t.teamKey, t.playoffOdds]));
		} catch (error) {
			// No number rather than the formula's: two disagreeing odds is worse
			// than a dash on the card.
			console.error('[managers/performance] playoff odds unavailable:', error);
		}
		for (const s of [...byManager.values()].flat()) {
			if (s.seasonYear === season && s.inProgress && !s.bracketSet) {
				s.playoffOdds = odds.get(s.teamKey) ?? null;
			}
		}
	}
	return { byManager, latestSeason: num(latest?.seasonYear) };
}

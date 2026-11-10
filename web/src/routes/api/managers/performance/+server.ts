import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';
import type { SeasonLuck } from '$lib/manager-luck';
import { summarizeManagerLuck } from '$lib/utils/power-rankings';
import { getSeasonOdds } from '$lib/server/playoff-odds-service';

/**
 * Postgres `numeric` arrives over the wire as a string, so every average, rate
 * and points total in this payload was a string. Consumers coerced by accident
 * (`'0.4317' * 100` happens to work) right up until one of them called
 * `.toFixed()` and crashed the whole manager profile page. Coerce once, here,
 * so the API's contract is "numbers are numbers".
 */
function num(value: unknown): number | null {
	if (value === null || value === undefined || value === '') return null;
	const parsed = typeof value === 'number' ? value : Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Each league's state, one grouped pass over fact_matchup: finished once its
 * championship game is in, bracketed once any playoff game is. Shared by the
 * luck rows and Season History so the two cannot disagree about a season.
 */
const leagueState = sql`
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
async function loadLuckBySeason(
	manager: string | null
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
					p.wins, p.ties, p.pythagorean_wins, p.luck_factor, p.playoff_odds,
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

export const GET: RequestHandler = async ({ url }) => {
	try {
		const manager = url.searchParams.get('manager');
		const analysis = url.searchParams.get('analysis') || 'overview';

		let data: any = {};

		// Every manager, always: the category ranks below compare a manager with
		// the rest of the league, and filtering to the requested manager first
		// left a field of one, so every profile read "#1" in every category.
		// The requested manager is picked out after ranking (exact match on the
		// whole dim_manager.manager_name, never ILIKE: `Gabe` must not match
		// `Gabe the Younger`). `manager` is still only ever a bound parameter.
		const statsResult = await db.execute(sql`
			SELECT
				manager_name,
				first_season,
				last_season,
				total_seasons,
				total_wins,
				total_losses,
				total_ties,
				career_win_percentage as win_percentage,
				total_points_scored,
				avg_points_per_game,
				avg_points_per_season as avg_points_for,
				championships_won as total_championships,
				championship_appearances,
				playoff_appearances,
				playoff_win_percentage,
				avg_draft_grade,
				best_draft_year,
				worst_draft_year,
				total_transactions,
				avg_transactions_per_season,
				faab_efficiency_rating,
				season_consistency_score,
				best_season_record,
				worst_season_record
			FROM edw.mart_manager_performance
			WHERE manager_name IS NOT NULL
			ORDER BY career_win_percentage DESC, championships_won DESC
		`);
		const managerStats = Array.from(statsResult);
		// Optional like the sections below: a failure here blanks the luck
		// figures, it does not take the whole profile down with it.
		let luck = new Map<string, SeasonLuck[]>();
		let latestSeason: number | null = null;
		try {
			({ byManager: luck, latestSeason } = await loadLuckBySeason(manager));
		} catch (luckError) {
			console.error('[managers/performance] luck query failed:', luckError);
		}

		// Add computed rankings and tier classification with camelCase field names
		const enrichedStats: any[] = managerStats.map((mgr: any, index: number) => ({
			// Handle both snake_case and camelCase field names from database
			managerName: mgr.manager_name || mgr.managerName,
			firstSeason: mgr.first_season || mgr.firstSeason,
			lastSeason: mgr.last_season || mgr.lastSeason,
			totalSeasons: num(mgr.total_seasons ?? mgr.totalSeasons),
			totalWins: num(mgr.total_wins ?? mgr.totalWins),
			totalLosses: num(mgr.total_losses ?? mgr.totalLosses),
			totalTies: num(mgr.total_ties ?? mgr.totalTies),
			winPercentage: num(mgr.win_percentage ?? mgr.winPercentage),
			totalPointsScored: num(mgr.total_points_scored ?? mgr.totalPointsScored),
			avgPointsPerGame: num(mgr.avg_points_per_game ?? mgr.avgPointsPerGame),
			// avg_points_per_season, despite the name. Kept for compatibility with
			// existing consumers; per-game display should use avgPointsPerGame.
			avgPointsFor: num(mgr.avg_points_for ?? mgr.avgPointsFor),
			totalChampionships: num(mgr.total_championships ?? mgr.totalChampionships),
			championshipAppearances: num(mgr.championship_appearances ?? mgr.championshipAppearances),
			playoffAppearances: num(mgr.playoff_appearances ?? mgr.playoffAppearances),
			playoffWinPercentage: num(mgr.playoff_win_percentage ?? mgr.playoffWinPercentage),
			avgDraftGrade: num(mgr.avg_draft_grade ?? mgr.avgDraftGrade),
			bestDraftYear: num(mgr.best_draft_year ?? mgr.bestDraftYear),
			worstDraftYear: num(mgr.worst_draft_year ?? mgr.worstDraftYear),
			totalTransactions: num(mgr.total_transactions ?? mgr.totalTransactions),
			avgTransactionsPerSeason: num(
				mgr.avg_transactions_per_season ?? mgr.avgTransactionsPerSeason
			),
			// Starter points per FAAB dollar; null for a manager who never paid for a claim.
			faabEfficiencyRating: num(mgr.faab_efficiency_rating ?? mgr.faabEfficiencyRating),
			// Career luck counts finished seasons only (G8); the live one has its own row.
			...summarizeManagerLuck(luck.get(mgr.manager_name || mgr.managerName) ?? [], latestSeason),
			seasonConsistencyScore: num(mgr.season_consistency_score ?? mgr.seasonConsistencyScore),
			bestSeasonRecord: mgr.best_season_record || mgr.bestSeasonRecord,
			worstSeasonRecord: mgr.worst_season_record || mgr.worstSeasonRecord,
			// Tier classification based on performance
			tierClassification:
				(mgr.manager_name || mgr.managerName) === 'Bobby'
					? 'League Alum'
					: (() => {
							const championships =
								parseInt(mgr.total_championships || mgr.totalChampionships) || 0;
							const winPct = parseFloat(mgr.win_percentage || mgr.winPercentage) || 0;

							if (championships >= 3) return 'League Legend';
							if (championships >= 2) return 'Dynasty Builder';
							if (championships >= 1 && winPct >= 0.6) return 'Championship Elite';
							if (championships >= 1) return 'Champion';
							if (winPct >= 0.65) return 'Elite Competitor';
							if (winPct >= 0.55) return 'Playoff Contender';
							if (winPct >= 0.45) return 'League Regular';
							return 'Rebuilding';
						})()
		}));

		// Competition ranking: a manager's rank is one more than the number of
		// managers strictly ahead, so equal values share a rank (three managers
		// with 3 titles are all #2) instead of being split by sort order.
		const rankBy = (value: (m: any) => number) => (mgr: any) =>
			1 + enrichedStats.filter((m) => (value(m) || 0) > (value(mgr) || 0)).length;
		const championshipRank = rankBy((m) => Number(m.totalChampionships));
		const winPctRank = rankBy((m) => Number(m.winPercentage));
		// Scoring rank is a rate, not a volume: avgPointsFor is points *per season*,
		// so ranking on it rewarded whoever played the most weeks rather than
		// whoever scored the most per game.
		const scoringRank = rankBy((m) => Number(m.avgPointsPerGame));
		const playoffRank = rankBy((m) => Number(m.playoffWinPercentage));

		// Add rankings to enriched stats. Overall is the query's order: career
		// win percentage, then championships.
		enrichedStats.forEach((mgr, index) => {
			mgr.overallRank = index + 1;
			mgr.championshipRank = championshipRank(mgr);
			mgr.winPctRank = winPctRank(mgr);
			mgr.scoringRank = scoringRank(mgr);
			mgr.playoffRank = playoffRank(mgr);
		});

		// Ranked against the whole league above; now narrow to the one asked for.
		const selected = manager
			? enrichedStats.filter((m) => m.managerName === manager)
			: enrichedStats;
		data.performance = selected;
		data.rankings = selected;

		// Get achievements based on computed stats with camelCase field names
		const achievements = selected.map((mgr: any) => ({
			managerName: mgr.managerName,
			championshipAchievement:
				(mgr.totalChampionships || 0) >= 3
					? 'Dynasty Builder'
					: (mgr.totalChampionships || 0) >= 2
						? 'Repeat Champion'
						: (mgr.totalChampionships || 0) >= 1
							? 'Champion'
							: null,
			consistencyAchievement:
				(mgr.winPercentage || 0) >= 0.7
					? 'Dominance'
					: (mgr.winPercentage || 0) >= 0.6
						? 'Consistent Winner'
						: (mgr.winPercentage || 0) >= 0.5
							? 'Above Average'
							: null,
			// Thresholds of 130/120 are per-game numbers. They were being compared
			// against avgPointsFor (points per *season*, ~1800), so every manager
			// in league history qualified as an Offensive Powerhouse.
			scoringAchievement:
				(mgr.avgPointsPerGame || 0) >= 130
					? 'Offensive Powerhouse'
					: (mgr.avgPointsPerGame || 0) >= 120
						? 'High Scorer'
						: null,
			longevityAchievement:
				(mgr.totalSeasons || 0) >= 15
					? 'League Veteran'
					: (mgr.totalSeasons || 0) >= 10
						? 'Long Timer'
						: (mgr.totalSeasons || 0) >= 5
							? 'Established'
							: 'Newcomer',
			allAchievements: [
				(mgr.totalChampionships || 0) >= 1 ? 'Champion' : null,
				(mgr.winPercentage || 0) >= 0.6 ? 'Consistent Winner' : null,
				(mgr.avgPointsPerGame || 0) >= 120 ? 'High Scorer' : null,
				(mgr.totalSeasons || 0) >= 10
					? 'Long Timer'
					: (mgr.totalSeasons || 0) >= 5
						? 'Established'
						: 'Newcomer'
			].filter((a) => a !== null)
		}));

		data.achievements = achievements;

		// Get detailed season-by-season breakdown. fact_team_performance is per-week,
		// so aggregate to one row per season (the old query returned per-week rows).
		if (analysis === 'seasons' || analysis === 'all') {
			try {
				// One row per team-season. A manager can run two teams in a year
				// (Justin in 2007, Bobby in 2008); grouping by year alone merged
				// them into one 30-game season.
				const seasonsResult = await db.execute(sql`
					WITH league_state AS (${leagueState})
					SELECT
						tp.season_year                              AS season_year,
						tp.team_key                                 AS team_key,
						MAX(dt.team_name)                           AS team_name,
						SUM(tp.wins)                                AS wins,
						SUM(tp.losses)                              AS losses,
						SUM(tp.ties)                                AS ties,
						CASE WHEN SUM(tp.wins + tp.losses + tp.ties) > 0
							THEN (SUM(tp.wins) + 0.5 * SUM(tp.ties)) / SUM(tp.wins + tp.losses + tp.ties)
							ELSE 0 END                              AS win_percentage,
						SUM(tp.points_for)                          AS points_for,
						SUM(tp.points_against)                      AS points_against,
						bool_or(tp.is_playoff_team)                 AS made_playoffs,
						EXISTS (
							SELECT 1 FROM edw.fact_matchup fm
							WHERE fm.is_championship AND fm.winner_team_key = tp.team_key
						)                                           AS is_champion,
						-- From the league, not the luck rows: those are optional, and
						-- without them the season read "Missed Playoffs" again.
						NOT COALESCE(bool_or(ls.finished), false)   AS in_progress,
						-- A playoff game is in: the regular season and bracket are set.
						COALESCE(bool_or(ls.bracketed), false)      AS bracket_set
					FROM edw.fact_team_performance tp
					JOIN edw.dim_manager dm ON tp.manager_key = dm.manager_key
					JOIN edw.dim_team dt ON dt.team_key = tp.team_key
					LEFT JOIN league_state ls ON ls.league_key = tp.league_key
					WHERE dm.manager_name = ${manager}
					GROUP BY tp.season_year, tp.team_key
					ORDER BY tp.season_year DESC, tp.team_key
				`);
				// Expected wins, luck and (for a season still being played) playoff
				// odds, from the power rankings' last ranked week of each season, so
				// this page and the power rankings never disagree.
				const byTeam = new Map((luck.get(manager ?? '') ?? []).map((l) => [l.teamKey, l]));
				data.seasons = Array.from(seasonsResult).map((row: any) => {
					const l = byTeam.get(Number(row.teamKey));
					return {
						...row,
						inProgress: Boolean(row.inProgress),
						bracketSet: Boolean(row.bracketSet),
						regularSeasonWins: l ? l.wins + 0.5 * l.ties : null,
						pythagoreanWins: l?.pythagoreanWins ?? null,
						luckFactor: l?.luckFactor ?? null,
						playoffOdds: row.inProgress && !row.bracketSet && l ? l.playoffOdds : null,
						// Live playoffs: the standings place, not is_playoff_team, which
						// misses top seeds on a first-round bye (see loadLuckBySeason).
						madePlayoffs: row.inProgress && row.bracketSet && l ? l.madePlayoffs : row.madePlayoffs,
						throughWeek: l?.weekNumber ?? null
					};
				});
			} catch (seasonsError) {
				console.error('[managers/performance] seasons query failed:', seasonsError);
				data.seasons = [];
			}

			// Seasons this manager won the championship (for per-season outcome + history).
			try {
				const champResult = await db.execute(sql`
					SELECT DISTINCT fm.season_year AS season_year
					FROM edw.fact_matchup fm
					JOIN edw.dim_manager dm ON fm.winner_manager_key = dm.manager_key
					WHERE fm.is_championship = true AND dm.manager_name = ${manager}
					ORDER BY fm.season_year DESC
				`);
				data.championshipSeasons = Array.from(champResult).map((r: any) => Number(r.seasonYear));
			} catch {
				data.championshipSeasons = [];
			}

			// Every finished title game the manager played (WP6 S6.8), newest
			// first. A placeholder manager ('--hidden--', a Yahoo-private
			// account) is no one to name, so its opponent is null.
			try {
				const titleResult = await db.execute(sql`
					SELECT
						fm.season_year AS season_year,
						CASE WHEN fm.manager1_key = dm.manager_key
							THEN fm.team1_points ELSE fm.team2_points END AS points,
						CASE WHEN fm.manager1_key = dm.manager_key
							THEN fm.team2_points ELSE fm.team1_points END AS opponent_points,
						CASE WHEN opp.manager_name ~* '^[-[:space:]]*hidden[-[:space:]]*$' THEN NULL
							ELSE opp.manager_name END AS opponent,
						COALESCE(fm.winner_manager_key = dm.manager_key, false) AS won
					FROM edw.fact_matchup fm
					JOIN edw.dim_manager dm ON dm.manager_name = ${manager}
					LEFT JOIN edw.dim_manager opp ON opp.manager_key = CASE
						WHEN fm.manager1_key = dm.manager_key THEN fm.manager2_key
						ELSE fm.manager1_key END
					WHERE fm.is_championship AND NOT fm.is_consolation
					  AND (fm.manager1_key = dm.manager_key OR fm.manager2_key = dm.manager_key)
					ORDER BY fm.season_year DESC
				`);
				data.titleGames = Array.from(titleResult).map((r: any) => ({
					seasonYear: Number(r.seasonYear),
					points: num(r.points),
					opponentPoints: num(r.opponentPoints),
					opponent: r.opponent ?? null,
					won: Boolean(r.won)
				}));
			} catch (titleError) {
				console.error('[managers/performance] title games query failed:', titleError);
				data.titleGames = [];
			}

			// Longest win/loss streaks from the manager's chronological game results
			// (one matchup per week; ordered by season then week_number). Finished
			// seasons only (WP6 S6.5): a league counts once its title game is in.
			try {
				const gamesResult = await db.execute(sql`
					WITH league_state AS (${leagueState})
					SELECT
						CASE WHEN fm.winner_manager_key = dm.manager_key THEN 'W'
							 WHEN fm.winner_manager_key IS NULL THEN 'T'
							 ELSE 'L' END AS result
					FROM edw.fact_matchup fm
					JOIN edw.dim_week dw ON fm.week_key = dw.week_key
					JOIN edw.dim_manager dm ON dm.manager_name = ${manager}
					JOIN league_state ls ON ls.league_key = fm.league_key AND ls.finished
					WHERE (fm.manager1_key = dm.manager_key OR fm.manager2_key = dm.manager_key)
					-- Team within season, so a manager's two teams in one year
					-- don't interleave week by week into one streak.
					ORDER BY fm.season_year,
						CASE WHEN fm.manager1_key = dm.manager_key THEN fm.team1_key ELSE fm.team2_key END,
						dw.week_number
				`);
				let longestWin = 0,
					longestLoss = 0,
					curWin = 0,
					curLoss = 0;
				for (const g of Array.from(gamesResult) as any[]) {
					if (g.result === 'W') {
						curWin++;
						curLoss = 0;
						if (curWin > longestWin) longestWin = curWin;
					} else if (g.result === 'L') {
						curLoss++;
						curWin = 0;
						if (curLoss > longestLoss) longestLoss = curLoss;
					} else {
						curWin = 0;
						curLoss = 0;
					}
				}
				data.streaks = {
					longestWinStreak: longestWin,
					longestLossStreak: longestLoss,
					totalGames: gamesResult.length
				};
			} catch {
				data.streaks = null;
			}
		}

		// Get manager head-to-head dominance (if table exists)
		if (analysis === 'h2h' || analysis === 'all') {
			try {
				// Parameterized for the same reason as the stats query above, and
				// exact-matched so `Gabe` stops pulling in `Gabe the Younger`'s rows.
				// `subject` is bound once and reused; the CASE arms orient each row so
				// the requested manager is always the `manager` side.
				const subject = manager ?? '';
				const h2hResult = await db.execute(sql`
					SELECT
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_name
							WHEN manager_b_name = ${subject} THEN manager_b_name
							ELSE manager_a_name
						END AS manager,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_b_name
							WHEN manager_b_name = ${subject} THEN manager_a_name
							ELSE manager_b_name
						END AS opponent,
						total_matchups,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_wins
							WHEN manager_b_name = ${subject} THEN manager_b_wins
							ELSE manager_a_wins
						END AS wins,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_losses
							WHEN manager_b_name = ${subject} THEN manager_b_losses
							ELSE manager_a_losses
						END AS losses,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_win_percentage
							WHEN manager_b_name = ${subject} THEN manager_b_win_percentage
							ELSE manager_a_win_percentage
						END AS win_percentage,
						-- WP6 S6.7: ties, per-meeting averages and the streak, all
						-- oriented the same way. A positive streak means the
						-- requested manager won the last N.
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_ties
							WHEN manager_b_name = ${subject} THEN manager_b_ties
							ELSE manager_a_ties
						END AS ties,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_avg_points
							WHEN manager_b_name = ${subject} THEN manager_b_avg_points
							ELSE manager_a_avg_points
						END AS avg_points,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_b_avg_points
							WHEN manager_b_name = ${subject} THEN manager_a_avg_points
							ELSE manager_b_avg_points
						END AS opponent_avg_points,
						CASE
							WHEN manager_a_name = ${subject} THEN manager_a_current_streak
							WHEN manager_b_name = ${subject} THEN manager_b_current_streak
							ELSE manager_a_current_streak
						END AS current_streak
					FROM edw.mart_manager_h2h
					WHERE total_matchups >= 5
					${manager ? sql`AND (manager_a_name = ${manager} OR manager_b_name = ${manager})` : sql``}
					ORDER BY win_percentage DESC, total_matchups DESC
				`);

				// Bracket games between the requested manager and each opponent,
				// from fact_matchup so the bracket rule (playoffs, not consolation)
				// is the one the rest of this API uses. Matched on manager keys,
				// as the streak query is.
				const playoffs = new Map<string, { wins: number; losses: number }>();
				if (manager) {
					const playoffResult = await db.execute(sql`
						SELECT
							opp.manager_name AS opponent,
							COUNT(*) FILTER (WHERE fm.winner_manager_key = dm.manager_key)  AS wins,
							COUNT(*) FILTER (WHERE fm.winner_manager_key = opp.manager_key) AS losses
						FROM edw.fact_matchup fm
						JOIN edw.dim_manager dm ON dm.manager_name = ${manager}
						JOIN edw.dim_manager opp ON opp.manager_key = CASE
							WHEN fm.manager1_key = dm.manager_key THEN fm.manager2_key
							ELSE fm.manager1_key END
						WHERE fm.is_playoffs AND NOT fm.is_consolation
						  AND (fm.manager1_key = dm.manager_key OR fm.manager2_key = dm.manager_key)
						GROUP BY opp.manager_name
					`);
					for (const r of Array.from(playoffResult) as any[]) {
						playoffs.set(String(r.opponent), {
							wins: Number(r.wins ?? 0),
							losses: Number(r.losses ?? 0)
						});
					}
				}
				const lastSeasonOf = new Map(
					managerStats.map((m: any) => [
						m.manager_name ?? m.managerName,
						num(m.last_season ?? m.lastSeason)
					])
				);

				data.head_to_head = Array.from(h2hResult).map((row: any) => {
					const opponent = row.opponent;
					const bracket = manager
						? (playoffs.get(String(opponent)) ?? { wins: 0, losses: 0 })
						: null;
					return {
						manager: row.manager,
						opponent,
						totalMatchups: num(row.totalMatchups ?? row.total_matchups),
						wins: num(row.wins),
						losses: num(row.losses),
						winPercentage: num(row.winPercentage ?? row.win_percentage),
						ties: num(row.ties) ?? 0,
						avgPoints: num(row.avgPoints),
						opponentAvgPoints: num(row.opponentAvgPoints),
						currentStreak: num(row.currentStreak) ?? 0,
						playoffWins: bracket?.wins ?? null,
						playoffLosses: bracket?.losses ?? null,
						opponentLastSeason: lastSeasonOf.get(opponent) ?? null
					};
				});
			} catch (h2hError) {
				console.error('[managers/performance] head-to-head query failed:', h2hError);
				data.head_to_head = [];
			}
		}

		// Available managers list - check both snake_case and camelCase field names
		data.availableManagers = managerStats
			.filter((mgr: any) => mgr.manager_name != null || mgr.managerName != null)
			.map((mgr: any) => mgr.manager_name || mgr.managerName);

		// Meta information
		data.meta = {
			totalManagers: data.availableManagers?.length || 0,
			requestedManager: manager,
			analysisLevel: analysis
		};

		return json({
			data,
			meta: data.meta
		});
	} catch (error) {
		console.error('Manager performance API error:', error);
		return json(
			{
				error: 'Failed to fetch manager performance data',
				details: error instanceof Error ? error.message : 'Unknown error'
			},
			{ status: 500 }
		);
	}
};

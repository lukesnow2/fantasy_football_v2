import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';
import { summarizeLuck, type SeasonLuck } from '$lib/manager-luck';
import { leagueState, loadLuckBySeason, num } from '$lib/server/manager-seasons';
import { longestStreaks, streaksByManager, type GameResult, type Streaks } from '$lib/streaks';

interface CareerExtras {
	pointsAgainst: number | null;
	finishedPointsPerSeason: number | null;
	streaks: Streaks | null;
	playoffWins: number | null;
	playoffLosses: number | null;
}

/**
 * Three career fields for every manager at once (WP14 S14.6), for the League
 * Table: points against over every game, points per finished season (points
 * in finished seasons ÷ finished seasons, so the live season can't drag the
 * average down), the longest win and losing streaks, read with the same
 * game order as the single-manager path below, and every playoff and title
 * game won and lost, counted as the mart counts them for its playoff win
 * percentage (so the two always agree).
 */
async function loadCareerExtras(): Promise<Map<string, CareerExtras>> {
	const [totalsResult, gamesResult, playoffResult] = await Promise.all([
		db.execute(sql`
			WITH league_state AS (${leagueState})
			SELECT
				dm.manager_name AS manager_name,
				SUM(tp.points_against) AS points_against,
				SUM(tp.points_for) FILTER (WHERE ls.finished)
					/ NULLIF(COUNT(DISTINCT tp.season_year) FILTER (WHERE ls.finished), 0)
					AS finished_points_per_season
			FROM edw.fact_team_performance tp
			JOIN edw.dim_manager dm ON dm.manager_key = tp.manager_key
			LEFT JOIN league_state ls ON ls.league_key = tp.league_key
			WHERE dm.manager_name IS NOT NULL
			GROUP BY dm.manager_name
		`),
		db.execute(sql`
			SELECT
				dm.manager_name AS manager_name,
				CASE WHEN fm.winner_manager_key = side.manager_key THEN 'W'
					 WHEN fm.winner_manager_key IS NULL THEN 'T'
					 ELSE 'L' END AS result
			FROM edw.fact_matchup fm
			CROSS JOIN LATERAL (VALUES
				(fm.manager1_key, fm.team1_key),
				(fm.manager2_key, fm.team2_key)
			) AS side(manager_key, team_key)
			JOIN edw.dim_week dw ON fm.week_key = dw.week_key
			JOIN edw.dim_manager dm ON dm.manager_key = side.manager_key
			WHERE dm.manager_name IS NOT NULL
			-- Team within season, as on the single-manager path.
			ORDER BY dm.manager_name, fm.season_year, side.team_key, dw.week_number
		`),
		db.execute(sql`
			SELECT
				dt.manager_name AS manager_name,
				COUNT(*) FILTER (WHERE fm.winner_team_key = side.team_key) AS playoff_wins,
				COUNT(*) FILTER (
					WHERE fm.winner_team_key <> side.team_key AND NOT fm.is_tie
				) AS playoff_losses
			FROM edw.fact_matchup fm
			CROSS JOIN LATERAL (VALUES (fm.team1_key), (fm.team2_key)) AS side(team_key)
			JOIN edw.dim_team dt ON dt.team_key = side.team_key
			WHERE fm.matchup_type IN ('playoffs', 'championship')
			  AND dt.manager_name IS NOT NULL
			GROUP BY dt.manager_name
		`)
	]);
	const playoffs = new Map(
		(Array.from(playoffResult) as any[]).map((r) => [
			String(r.managerName),
			{ wins: num(r.playoffWins), losses: num(r.playoffLosses) }
		])
	);
	const streaks = streaksByManager(
		(Array.from(gamesResult) as any[]).map((g) => ({
			managerName: String(g.managerName),
			result: g.result as GameResult
		}))
	);
	const extras = new Map<string, CareerExtras>();
	for (const r of Array.from(totalsResult) as any[]) {
		const name = String(r.managerName);
		extras.set(name, {
			pointsAgainst: num(r.pointsAgainst),
			finishedPointsPerSeason: num(r.finishedPointsPerSeason),
			streaks: streaks.get(name) ?? null,
			// No bracket games yet is a 0-0 record, not a missing one.
			playoffWins: playoffs.get(name)?.wins ?? 0,
			playoffLosses: playoffs.get(name)?.losses ?? 0
		});
	}
	return extras;
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
		// Both optional like the sections below: a failure blanks those figures,
		// it does not take the whole page down with it. The career extras are
		// for the all-managers rows only (the League Table). Independent, so
		// they run at the same time.
		const [luckResult, extrasResult] = await Promise.all([
			loadLuckBySeason(manager).catch((luckError) => {
				console.error('[managers/performance] luck query failed:', luckError);
				return null;
			}),
			manager
				? Promise.resolve(null)
				: loadCareerExtras().catch((extrasError) => {
						console.error('[managers/performance] career extras query failed:', extrasError);
						return null;
					})
		]);
		const luck = luckResult?.byManager ?? new Map<string, SeasonLuck[]>();
		const latestSeason: number | null = luckResult?.latestSeason ?? null;
		const extras = extrasResult ?? new Map<string, CareerExtras>();

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
			// Career luck adds up every season, the one in progress too, like the
			// career record (Luke, 2026-11-10: running totals count the live season;
			// only season-level picks and outcomes wait for it to finish).
			...summarizeLuck(luck.get(mgr.manager_name || mgr.managerName) ?? [], latestSeason),
			seasonConsistencyScore: num(mgr.season_consistency_score ?? mgr.seasonConsistencyScore),
			bestSeasonRecord: mgr.best_season_record || mgr.bestSeasonRecord,
			worstSeasonRecord: mgr.worst_season_record || mgr.worstSeasonRecord,
			...(manager
				? {}
				: (extras.get(mgr.manager_name || mgr.managerName) ?? {
						pointsAgainst: null,
						finishedPointsPerSeason: null,
						streaks: null,
						playoffWins: null,
						playoffLosses: null
					})),
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
			// No bracket games is no playoff win rate, so no rank: ranking it
			// as 0% put "Tied 12th" under a rate that reads None.
			mgr.playoffRank = mgr.playoffWinPercentage == null ? null : playoffRank(mgr);
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
		// Per-manager history. With no manager (the Managers page) every query
		// here can only come back empty, so skip them and send the empty shape.
		if ((analysis === 'seasons' || analysis === 'all') && !manager) {
			data.seasons = [];
			data.championshipSeasons = [];
			data.titleGames = [];
			data.streaks = { longestWinStreak: 0, longestLossStreak: 0, totalGames: 0 };
		} else if (analysis === 'seasons' || analysis === 'all') {
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
			{
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
			}

			// Longest win/loss streaks from the manager's chronological game results
			// (one matchup per week; ordered by season then week_number). Every
			// game counts, the season in progress too: a streak is a running total,
			// not a season-level judgment (Luke, 2026-11-10, over S6.5's wording).
			try {
				const gamesResult = await db.execute(sql`
					SELECT
						CASE WHEN fm.winner_manager_key = dm.manager_key THEN 'W'
							 WHEN fm.winner_manager_key IS NULL THEN 'T'
							 ELSE 'L' END AS result
					FROM edw.fact_matchup fm
					JOIN edw.dim_week dw ON fm.week_key = dw.week_key
					JOIN edw.dim_manager dm ON dm.manager_name = ${manager}
					WHERE (fm.manager1_key = dm.manager_key OR fm.manager2_key = dm.manager_key)
					-- Team within season, so a manager's two teams in one year
					-- don't interleave week by week into one streak.
					ORDER BY fm.season_year,
						CASE WHEN fm.manager1_key = dm.manager_key THEN fm.team1_key ELSE fm.team2_key END,
						dw.week_number
				`);
				data.streaks = longestStreaks(
					(Array.from(gamesResult) as any[]).map((g) => g.result as GameResult)
				);
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
				// as the streak query is. Title games are split out of the
				// playoff record and counted on their own.
				const playoffs = new Map<
					string,
					{ wins: number; losses: number; titleWins: number; titleLosses: number }
				>();
				if (manager) {
					const playoffResult = await db.execute(sql`
						SELECT
							opp.manager_name AS opponent,
							COUNT(*) FILTER (WHERE NOT fm.is_championship AND fm.winner_manager_key = dm.manager_key)  AS wins,
							COUNT(*) FILTER (WHERE NOT fm.is_championship AND fm.winner_manager_key = opp.manager_key) AS losses,
							COUNT(*) FILTER (WHERE fm.is_championship AND fm.winner_manager_key = dm.manager_key)      AS title_wins,
							COUNT(*) FILTER (WHERE fm.is_championship AND fm.winner_manager_key = opp.manager_key)     AS title_losses
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
							losses: Number(r.losses ?? 0),
							titleWins: Number(r.titleWins ?? r.title_wins ?? 0),
							titleLosses: Number(r.titleLosses ?? r.title_losses ?? 0)
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
						? (playoffs.get(String(opponent)) ?? {
								wins: 0,
								losses: 0,
								titleWins: 0,
								titleLosses: 0
							})
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
						titleWins: bracket?.titleWins ?? null,
						titleLosses: bracket?.titleLosses ?? null,
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

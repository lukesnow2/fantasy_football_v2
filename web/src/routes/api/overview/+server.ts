import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';
import {
	balanceOf,
	coinFlipBand,
	leagueEvents,
	recordPersistence,
	splitEras,
	recordRank,
	rollingMean,
	scoringRank,
	seasonRecords,
	type RegularGame
} from '$lib/league-trends';

const POSITIONS = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF'] as const;
const rows = async (q: string) => Array.from(await db.execute(sql.raw(q))) as any[];

// Coin-flip bands depend only on teams and games (and a fixed seed), so they
// are simulated once per server process, not on every request.
const coinFlipBands = new Map<string, { lo: number; hi: number }>();
function coinFlipBandFor(teams: number, games: number) {
	const key = `${teams}|${games}`;
	if (!coinFlipBands.has(key)) coinFlipBands.set(key, coinFlipBand(teams, games));
	return coinFlipBands.get(key)!;
}

/**
 * Long-run league measures for the trends chart ($lib/league-trends): balance
 * and skill share per season, each champion's regular-season ranks, who won
 * when, where starting-lineup points came from by position, waiver activity
 * per team, and the headline numbers above the chart. Regular season only;
 * the season still being played is flagged, and left out of anything that
 * needs a finished season (balance, champions, the headlines).
 */
async function loadTrends() {
	const [gameRows, champRows, teamRows, positionRows, draftedRows, moveRows, tradeRows, leagueRows, scoreRows, faabRows] =
		await Promise.all([
			rows(`
				SELECT fm.season_year, fm.team1_key, fm.team2_key, fm.team1_points, fm.team2_points,
					fm.winner_team_key, fm.is_tie
				FROM edw.fact_matchup fm WHERE fm.matchup_type = 'regular'
			`),
			rows(`
				SELECT fm.season_year, fm.winner_team_key,
					dw.manager_name AS winner_manager, dl.manager_name AS loser_manager,
					GREATEST(fm.team1_points, fm.team2_points) AS winner_points,
					LEAST(fm.team1_points, fm.team2_points) AS loser_points
				FROM edw.fact_matchup fm
				JOIN edw.dim_team dw ON dw.team_key = fm.winner_team_key
				LEFT JOIN edw.dim_team dl ON dl.team_key =
					CASE WHEN fm.winner_team_key = fm.team1_key THEN fm.team2_key ELSE fm.team1_key END
				WHERE fm.is_championship
			`),
			rows(`SELECT team_key, manager_name FROM edw.dim_team`),
			rows(`
				SELECT fr.season_year, dp.primary_position AS position, SUM(fr.weekly_points) AS points
				FROM edw.fact_roster fr
				JOIN edw.dim_player dp ON dp.player_key = fr.player_key
				JOIN edw.dim_week dw ON dw.week_key = fr.week_key
				WHERE fr.is_starter AND dw.week_type = 'regular'
				GROUP BY 1, 2
			`),
			rows(`
				WITH drafted AS (SELECT DISTINCT season_year, team_key, player_key FROM edw.fact_draft)
				SELECT fr.season_year,
					SUM(fr.weekly_points) FILTER (WHERE d.player_key IS NOT NULL) AS drafted_points,
					SUM(fr.weekly_points) AS all_points
				FROM edw.fact_roster fr
				JOIN edw.dim_week dw ON dw.week_key = fr.week_key
				LEFT JOIN drafted d ON d.season_year = fr.season_year AND d.team_key = fr.team_key
					AND d.player_key = fr.player_key
				WHERE fr.is_starter AND dw.week_type = 'regular'
				GROUP BY 1
			`),
			// A move is one transaction (an add/drop is one move, not two rows).
			rows(`
				SELECT season_year, COUNT(DISTINCT split_part(source_transaction_id, '_', 1)) AS moves
				FROM edw.fact_transaction WHERE transaction_type <> 'trade' GROUP BY 1
			`),
			rows(`
				SELECT season_year, COUNT(DISTINCT split_part(source_transaction_id, '_', 1)) AS trades
				FROM edw.fact_transaction WHERE transaction_type = 'trade' GROUP BY 1
			`),
			rows(`SELECT season_year, num_teams, playoff_start_week, num_playoff_teams FROM edw.dim_league`),
			// Every team score in every game, as the scoring chart counts them.
			rows(`
				SELECT fm.season_year, ROUND(v.points::numeric, 1) AS points
				FROM edw.fact_matchup fm
				CROSS JOIN LATERAL (VALUES (fm.team1_points), (fm.team2_points)) AS v(points)
				WHERE v.points > 0
			`),
			rows(`SELECT MIN(season_year) AS season FROM edw.fact_transaction WHERE faab_bid > 0`)
		]);

	const games: RegularGame[] = gameRows.map((g) => ({
		season: Number(g.seasonYear),
		teamA: Number(g.team1Key),
		teamB: Number(g.team2Key),
		pointsA: Number(g.team1Points ?? 0),
		pointsB: Number(g.team2Points ?? 0),
		winner: g.winnerTeamKey == null ? null : Number(g.winnerTeamKey),
		tie: g.isTie === true
	}));
	const champions = new Map(
		champRows.map((c) => [
			Number(c.seasonYear),
			{
				team: Number(c.winnerTeamKey),
				manager: String(c.winnerManager),
				runnerUp: c.loserManager == null || String(c.loserManager).includes('hidden') ? null : String(c.loserManager),
				score: [Number(c.winnerPoints ?? 0), Number(c.loserPoints ?? 0)] as [number, number]
			}
		])
	);
	const managerOf = new Map(teamRows.map((t) => [Number(t.teamKey), t.managerName == null ? null : String(t.managerName)]));
	const teamsIn = new Map(leagueRows.map((l) => [Number(l.seasonYear), Number(l.numTeams)]));
	const moves = new Map(moveRows.map((m) => [Number(m.seasonYear), Number(m.moves)]));
	const trades = new Map(tradeRows.map((t) => [Number(t.seasonYear), Number(t.trades)]));
	const positions = new Map<number, Record<string, number>>();
	for (const p of positionRows) {
		const season = Number(p.seasonYear);
		const m = positions.get(season) ?? {};
		m[String(p.position ?? '?')] = Number(p.points ?? 0);
		positions.set(season, m);
	}

	const records = seasonRecords(games);
	const seasons = [...new Set([...records.keys(), ...positions.keys(), ...moves.keys()])].sort((a, b) => a - b);
	const finished = (s: number) => champions.has(s);

	const perSeason = seasons.map((season) => {
		const recs = records.get(season) ?? [];
		const done = finished(season);
		const balance = done ? balanceOf(season, recs) : null;
		const champ = champions.get(season);
		const pos = positions.get(season) ?? {};
		const posTotal = POSITIONS.reduce((a, k) => a + (pos[k] ?? 0), 0);
		const teams = teamsIn.get(season) ?? recs.length;
		return {
			seasonYear: season,
			inProgress: !done,
			teams,
			gamesPerTeam: balance?.gamesPerTeam ?? null,
			nollScully: balance?.nollScully ?? null,
			skillShare: balance?.skillShare ?? null,
			champion: champ
				? {
						manager: champ.manager,
						runnerUp: champ.runnerUp,
						score: champ.score,
						pointsRank: scoringRank(recs, champ.team),
						recordRank: recordRank(recs, champ.team)
					}
				: null,
			positionShare: posTotal
				? Object.fromEntries(POSITIONS.map((k) => [k, (pos[k] ?? 0) / posTotal]))
				: null,
			movesPerTeam: teams && moves.has(season) ? moves.get(season)! / teams : null,
			trades: trades.get(season) ?? 0
		};
	});
	// What a league of coin flips would give for each season's teams and games.
	for (const s of perSeason) {
		if (s.nollScully == null || !s.gamesPerTeam) continue;
		const band = coinFlipBandFor(s.teams, Math.round(s.gamesPerTeam));
		Object.assign(s, { chanceLo: band.lo, chanceHi: band.hi });
	}
	const rolling = rollingMean(perSeason.map((s) => s.nollScully), 5);
	perSeason.forEach((s, i) => Object.assign(s, { nollScullyRolling: s.nollScully == null ? null : rolling[i] }));

	// Headlines, finished seasons only.
	const byManagerSeason = new Map<string, number>();
	for (const [season, recs] of records) {
		if (!finished(season)) continue;
		for (const r of recs) {
			const m = managerOf.get(r.team);
			if (m && !m.includes('hidden')) byManagerSeason.set(`${m}|${season}`, r.wins / r.games);
		}
	}
	const done = perSeason.filter((s) => !s.inProgress);

	/** The headline numbers over finished seasons, optionally one era's. */
	const headlineFor = (start = -Infinity, end = Infinity) => {
		const inRange = (season: number) => season >= start && season <= end;
		const seasons = done.filter((s) => inRange(s.seasonYear));
		const skill = seasons.map((s) => s.skillShare).filter((v): v is number => v != null);
		const champs = seasons.filter((s) => s.champion?.pointsRank != null);
		const drafted = draftedRows.filter((d) => finished(Number(d.seasonYear)) && inRange(Number(d.seasonYear)));
		const draftedPoints = drafted.reduce((a, d) => a + Number(d.draftedPoints ?? 0), 0);
		const allPoints = drafted.reduce((a, d) => a + Number(d.allPoints ?? 0), 0);
		// Pairs of consecutive seasons, both inside the range.
		const pairs = new Map([...byManagerSeason].filter(([key]) => inRange(Number(key.split('|')[1]))));
		return {
			recordPersistence: recordPersistence(pairs),
			skillShare: skill.length ? skill.reduce((a, b) => a + b, 0) / skill.length : null,
			championTopThree: {
				count: champs.filter((s) => s.champion!.pointsRank! <= 3).length,
				seasons: champs.length
			},
			topScorerTitles: champs.filter((s) => s.champion!.pointsRank === 1).length,
			draftedShare: allPoints ? draftedPoints / allPoints : null,
			seasons: seasons.length
		};
	};
	const eras = splitEras(perSeason.map((s) => s.seasonYear));

	// Who reached the final and who won it, one row per finalist: titles,
	// finals lost, and the finished seasons they played (so the chart can tell
	// "didn't win" from "wasn't in the league").
	const played = new Map<string, number[]>();
	for (const [key] of byManagerSeason) {
		const [manager, season] = key.split('|');
		if (!played.has(manager)) played.set(manager, []);
		played.get(manager)!.push(Number(season));
	}
	const finalists = new Map<string, { years: number[]; finals: number[] }>();
	const entry = (m: string) => finalists.get(m) ?? finalists.set(m, { years: [], finals: [] }).get(m)!;
	for (const s of done) {
		if (!s.champion) continue;
		entry(s.champion.manager).years.push(s.seasonYear);
		if (s.champion.runnerUp) entry(s.champion.runnerUp).finals.push(s.seasonYear);
	}

	return {
		seasons: perSeason,
		titles: [...finalists.entries()]
			.map(([manager, f]) => ({ manager, ...f, seasons: (played.get(manager) ?? []).sort((a, b) => a - b) }))
			.sort(
				(a, b) =>
					b.years.length - a.years.length ||
					b.finals.length - a.finals.length ||
					Math.max(0, ...b.years, ...b.finals) - Math.max(0, ...a.years, ...a.finals)
			),
		headline: headlineFor(),
		eras: eras.map((e) => ({ ...e, headline: headlineFor(e.start, e.end) })),
		// Seasons where the league's stored settings changed.
		events: leagueEvents(
			leagueRows.map((l) => ({
				season: Number(l.seasonYear),
				teams: Number(l.numTeams),
				regularWeeks: l.playoffStartWeek == null ? null : Number(l.playoffStartWeek) - 1,
				playoffTeams: l.numPlayoffTeams == null ? null : Number(l.numPlayoffTeams)
			})),
			faabRows[0]?.season == null ? null : Number(faabRows[0].season)
		),
		// Every team score by season, for the distribution view.
		scores: scoreRows.reduce<Record<number, number[]>>((bySeason, r) => {
			(bySeason[Number(r.seasonYear)] ??= []).push(Number(r.points));
			return bySeason;
		}, {})
	};
}

export const GET: RequestHandler = async ({ url }) => {
	try {
		const metric = url.searchParams.get('metric') || 'all';

		let data: any = {};

		// Core league evolution metrics
		if (metric === 'league' || metric === 'all') {
			const leagueQuery = `
				SELECT 
					season_year,
					league_name,
					total_teams,
					total_transactions,
					average_weekly_score,
					highest_single_week_score,
					champion_manager,
					runner_up_manager,
					highest_scorer_manager
				FROM edw.mart_league_summary
				ORDER BY season_year
			`;

			const leagueResult = await db.execute(sql.raw(leagueQuery));
			data.league_evolution = Array.from(leagueResult);
		}

		// League competitiveness metrics
		if (metric === 'competitiveness' || metric === 'all') {
			const competitivenessQuery = `
				SELECT 
					season_year,
					win_parity_score,
					point_spread_score,
					playoff_race_score,
					close_games_score,
					total_teams,
					total_transactions
				FROM edw.vw_league_competitiveness
				ORDER BY season_year
			`;

			const competitivenessResult = await db.execute(sql.raw(competitivenessQuery));
			data.competitiveness = Array.from(competitivenessResult);
		}

		// Scoring patterns by season
		if (metric === 'scoring' || metric === 'all') {
			// Every team's score in every game, from fact_matchup. This used to read
			// fact_team_performance.weekly_points, which is the season-to-date running
			// average, not the week's score: it halved the spread (2025 sd 15.4 vs
			// 27.7 for real games) and skewed the average.
			const scoringQuery = `
				WITH games AS (
					SELECT fm.season_year, v.points
					FROM edw.fact_matchup fm
					CROSS JOIN LATERAL (VALUES (fm.team1_points), (fm.team2_points)) AS v(points)
					WHERE v.points > 0
				)
				SELECT
					season_year,
					AVG(points) as avg_weekly_score,
					MAX(points) as max_weekly_score,
					MIN(points) as min_weekly_score,
					STDDEV(points) as score_volatility,
					COUNT(*) as total_team_weeks,
					PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY points) as p90_score,
					PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY points) as p75_score,
					PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY points) as median_score,
					PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY points) as p25_score,
					PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY points) as p10_score
				FROM games
				GROUP BY season_year
				ORDER BY season_year
			`;

			const scoringResult = await db.execute(sql.raw(scoringQuery));
			data.scoring_patterns = Array.from(scoringResult);
		}

		// Trade activity evolution
		if (metric === 'trades' || metric === 'all') {
			const tradesQuery = `
				SELECT 
					season_year,
					COUNT(*) as total_trades,
					AVG(total_players) as avg_players_per_trade,
					COUNT(*) FILTER (WHERE (team_a_champion = 1 OR team_b_champion = 1) AND production_differential >= 50) as championship_trades,
					AVG(ABS(team_a_final_score - team_b_final_score)) as avg_score_differential,
					COUNT(*) FILTER (WHERE trade_winner != 'Even Trade') as decisive_trades,
					AVG(ABS(production_differential)) as avg_production_impact
				FROM edw.vw_trade_analysis
				GROUP BY season_year
				ORDER BY season_year
			`;

			const tradesResult = await db.execute(sql.raw(tradesQuery));
			data.trade_activity = Array.from(tradesResult);
		}

		// Player value trends
		if (metric === 'players' || metric === 'all') {
			const playersQuery = `
				SELECT 
					season_year,
					COUNT(*) as total_players,
					AVG(total_fantasy_points) as avg_fantasy_points,
					AVG(career_avg_draft_position) as avg_draft_position,
					COUNT(*) FILTER (WHERE draft_value_score > 0) as value_picks,
					MAX(total_fantasy_points) as highest_player_score,
					AVG(avg_ownership_percentage) as avg_ownership_rate
				FROM edw.mart_player_value
				WHERE total_fantasy_points > 0
				GROUP BY season_year
				ORDER BY season_year
			`;

			const playersResult = await db.execute(sql.raw(playersQuery));
			data.player_trends = Array.from(playersResult);
		}

		// Manager performance evolution
		if (metric === 'managers' || metric === 'all') {
			const managersQuery = `
				WITH seasonal_stats AS (
					SELECT 
						season_year,
						COUNT(DISTINCT CASE WHEN wins > losses THEN performance_key END) as winning_teams,
						COUNT(DISTINCT performance_key) as total_teams,
						AVG(weekly_points) as avg_team_score,
						STDDEV(weekly_points) as score_parity,
						AVG(win_percentage) as avg_win_pct
					FROM edw.fact_team_performance
					WHERE weekly_points > 0
					GROUP BY season_year
				)
				SELECT 
					season_year,
					winning_teams,
					total_teams,
					ROUND(avg_team_score, 1) as avg_team_score,
					ROUND(score_parity, 1) as score_parity,
					ROUND(avg_win_pct * 100, 1) as avg_win_percentage
				FROM seasonal_stats
				ORDER BY season_year
			`;

			const managersResult = await db.execute(sql.raw(managersQuery));
			data.manager_evolution = Array.from(managersResult);
		}

		// Championship and playoff trends
		if (metric === 'playoffs' || metric === 'all') {
			const playoffsQuery = `
				SELECT 
					season_year,
					champion_manager,
					runner_up_manager,
					highest_scorer_manager,
					total_teams,
					CASE WHEN total_teams >= 10 THEN 4 ELSE 2 END as playoff_teams
				FROM edw.mart_league_summary
				ORDER BY season_year
			`;

			const playoffsResult = await db.execute(sql.raw(playoffsQuery));
			data.championship_trends = Array.from(playoffsResult);
		}

		// Advanced metrics
		if (metric === 'advanced' || metric === 'all') {
			const advancedQuery = `
				WITH yearly_metrics AS (
					SELECT 
						ls.season_year,
						ls.total_transactions,
						ls.average_weekly_score,
						lc.win_parity_score,
						lc.point_spread_score,
						lc.playoff_race_score,
						lc.close_games_score,
						COALESCE(ta.total_trades, 0) as trade_count,
						COALESCE(ta.avg_score_differential, 0) as trade_impact
					FROM edw.mart_league_summary ls
					LEFT JOIN edw.vw_league_competitiveness lc ON ls.season_year = lc.season_year
					LEFT JOIN (
						SELECT 
							season_year,
							COUNT(*) as total_trades,
							AVG(ABS(team_a_final_score - team_b_final_score)) as avg_score_differential
						FROM edw.vw_trade_analysis
						GROUP BY season_year
					) ta ON ls.season_year = ta.season_year
				)
				SELECT 
					season_year,
					total_transactions as waiver_activity,
					ROUND(average_weekly_score, 1) as scoring_level,
					ROUND(win_parity_score, 1) as competitiveness,
					ROUND(point_spread_score, 1) as scoring_spread,
					trade_count,
					ROUND(trade_impact, 1) as trade_impact_score
				FROM yearly_metrics
				ORDER BY season_year
			`;

			const advancedResult = await db.execute(sql.raw(advancedQuery));
			data.advanced_metrics = Array.from(advancedResult);
		}

		// Convert snake_case to camelCase for frontend compatibility
		const camelCaseData: any = {};
		
		if (data.league_evolution) {
			camelCaseData.leagueEvolution = data.league_evolution.map((item: any) => ({
				seasonYear: item.seasonYear || item.season_year,
				leagueName: item.leagueName || item.league_name,
				totalTeams: item.totalTeams || item.total_teams,
				totalTransactions: item.totalTransactions || item.total_transactions,
				averageWeeklyScore: item.averageWeeklyScore || item.average_weekly_score,
				highestSingleWeekScore: item.highestSingleWeekScore || item.highest_single_week_score,
				championManager: item.championManager || item.champion_manager,
				runnerUpManager: item.runnerUpManager || item.runner_up_manager,
				highestScorerManager: item.highestScorerManager || item.highest_scorer_manager
			}));
		}

		if (data.competitiveness) {
			camelCaseData.competitiveness = data.competitiveness.map((item: any) => ({
				seasonYear: item.seasonYear || item.season_year,
				winParityScore: item.winParityScore || item.win_parity_score,
				pointSpreadScore: item.pointSpreadScore || item.point_spread_score,
				playoffRaceScore: item.playoffRaceScore || item.playoff_race_score,
				closeGamesScore: item.closeGamesScore || item.close_games_score,
				totalTeams: item.totalTeams || item.total_teams,
				totalTransactions: item.totalTransactions || item.total_transactions
			}));
		}

		if (data.scoring_patterns) {
			camelCaseData.scoringPatterns = data.scoring_patterns.map((item: any) => ({
				seasonYear: item.seasonYear || item.season_year,
				avgWeeklyScore: item.avgWeeklyScore || item.avg_weekly_score,
				maxWeeklyScore: item.maxWeeklyScore || item.max_weekly_score,
				minWeeklyScore: item.minWeeklyScore || item.min_weekly_score,
				scoreVolatility: item.scoreVolatility || item.score_volatility,
				totalTeamWeeks: item.totalTeamWeeks || item.total_team_weeks,
				p90Score: item.p90Score || item.p90_score,
				p75Score: item.p75Score || item.p75_score,
				medianScore: item.medianScore || item.median_score,
				p25Score: item.p25Score || item.p25_score,
				p10Score: item.p10Score || item.p10_score
			}));
		}

		if (data.trade_activity) {
			camelCaseData.tradeActivity = data.trade_activity.map((item: any) => ({
				seasonYear: item.seasonYear || item.season_year,
				totalTrades: item.totalTrades || item.total_trades,
				avgPlayersPerTrade: item.avgPlayersPerTrade || item.avg_players_per_trade,
				championshipTrades: item.championshipTrades || item.championship_trades,
				avgScoreDifferential: item.avgScoreDifferential || item.avg_score_differential,
				decisiveTrades: item.decisiveTrades || item.decisive_trades,
				avgProductionImpact: item.avgProductionImpact || item.avg_production_impact
			}));
		}

		if (data.player_trends) {
			camelCaseData.playerTrends = data.player_trends.map((item: any) => ({
				seasonYear: item.seasonYear || item.season_year,
				totalPlayers: item.totalPlayers || item.total_players,
				avgFantasyPoints: item.avgFantasyPoints || item.avg_fantasy_points,
				avgDraftPosition: item.avgDraftPosition || item.avg_draft_position,
				valuePicks: item.valuePicks || item.value_picks,
				highestPlayerScore: item.highestPlayerScore || item.highest_player_score,
				avgOwnershipRate: item.avgOwnershipRate || item.avg_ownership_rate
			}));
		}

		if (data.manager_evolution) {
			camelCaseData.managerEvolution = data.manager_evolution.map((item: any) => ({
				seasonYear: item.seasonYear || item.season_year,
				winningTeams: item.winningTeams || item.winning_teams,
				totalTeams: item.totalTeams || item.total_teams,
				avgTeamScore: item.avgTeamScore || item.avg_team_score,
				scoreParity: item.scoreParity || item.score_parity,
				avgWinPercentage: item.avgWinPercentage || item.avg_win_percentage
			}));
		}

		if (data.championship_trends) {
			camelCaseData.championshipTrends = data.championship_trends.map((item: any) => ({
				seasonYear: item.seasonYear || item.season_year,
				championManager: item.championManager || item.champion_manager,
				runnerUpManager: item.runnerUpManager || item.runner_up_manager,
				highestScorerManager: item.highestScorerManager || item.highest_scorer_manager,
				totalTeams: item.totalTeams || item.total_teams,
				playoffTeams: item.playoffTeams || item.playoff_teams
			}));
		}

		if (data.advanced_metrics) {
			camelCaseData.advancedMetrics = data.advanced_metrics.map((item: any) => ({
				seasonYear: item.seasonYear || item.season_year,
				waiverActivity: item.waiverActivity || item.waiver_activity,
				scoringLevel: item.scoringLevel || item.scoring_level,
				competitiveness: item.competitiveness,
				scoringSpread: item.scoringSpread || item.scoring_spread,
				tradeCount: item.tradeCount || item.trade_count,
				tradeImpactScore: item.tradeImpactScore || item.trade_impact_score
			}));
		}

		
		// Derive coverage from the actual data rather than hardcoding it.
		const seasonYears = (
			camelCaseData.leagueEvolution ||
			camelCaseData.scoringPatterns ||
			camelCaseData.competitiveness ||
			[]
		)
			.map((x: any) => Number(x.seasonYear))
			.filter((y: number) => Number.isFinite(y));
		const firstYear = seasonYears.length ? Math.min(...seasonYears) : null;
		const lastYear = seasonYears.length ? Math.max(...seasonYears) : null;

		if (metric === 'all' || metric === 'trends') {
			camelCaseData.trends = await loadTrends();
		}

		// The season still being played (no championship game yet), so charts can
		// mark it as partial rather than a finished year.
		const [live] = Array.from(
			await db.execute(sql.raw(`
				SELECT MAX(fm.season_year) AS season_year,
					MAX(dw.week_number) AS through_week
				FROM edw.fact_matchup fm
				JOIN edw.dim_week dw ON dw.week_key = fm.week_key
				WHERE fm.season_year = (SELECT MAX(season_year) FROM edw.fact_matchup)
				  AND NOT EXISTS (
					SELECT 1 FROM edw.fact_matchup c
					WHERE c.season_year = fm.season_year AND c.is_championship
				  )
			`))
		) as any[];
		camelCaseData.seasonInProgress =
			live?.seasonYear != null
				? { seasonYear: Number(live.seasonYear), throughWeek: Number(live.throughWeek) }
				: null;

		return json({
			data: camelCaseData,
			meta: {
				metric,
				seasonsCovered: firstYear && lastYear ? `${firstYear}-${lastYear}` : 'Unknown',
				firstSeason: firstYear,
				lastSeason: lastYear,
				totalSeasons: new Set(seasonYears).size,
				generatedAt: new Date().toISOString()
			}
		});

	} catch (error) {
		console.error('Error fetching overview data:', error);
		return json({ 
			error: 'Failed to fetch overview data',
			details: error instanceof Error ? error.message : String(error)
		}, { status: 500 });
	}
}; 
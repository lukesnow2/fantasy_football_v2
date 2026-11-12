import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import {
	careerGrade,
	seasonDraftScores,
	seasonGrade,
	spreadOf,
	topBusts,
	topSteals,
	valueDraftPicks
} from '$lib/draft-value';
import type { RequestHandler } from './$types';

/**
 * Every pick in league history, valued against what its position and round
 * were expected to score (see $lib/draft-value). Fitted on all managers' picks
 * from finished seasons; the views below then show only managers in analysis.
 */
async function loadValuedPicks() {
	const [pickRows, doneRows] = await Promise.all([
		db.execute(sql`
			SELECT fd.season_year, fd.overall_pick, fd.round_number,
				dp.primary_position AS position, fd.season_points,
				dp.player_name, dm.manager_name, dm.include_in_analysis
			FROM edw.fact_draft fd
			JOIN edw.dim_player dp ON dp.player_key = fd.player_key
			JOIN edw.dim_manager dm ON dm.manager_key = fd.manager_key
		`),
		db.execute(sql`SELECT DISTINCT season_year FROM edw.fact_matchup WHERE is_championship`)
	]);
	const picks = (Array.from(pickRows) as any[]).map((r) => ({
		seasonYear: Number(r.seasonYear),
		overallPick: Number(r.overallPick),
		roundNumber: Number(r.roundNumber),
		position: r.position == null ? null : String(r.position),
		seasonPoints: Number(r.seasonPoints ?? 0),
		playerName: String(r.playerName ?? ''),
		managerName: String(r.managerName ?? ''),
		included: r.includeInAnalysis === true
	}));
	const done = new Set((Array.from(doneRows) as any[]).map((r) => Number(r.seasonYear)));
	return { valued: valueDraftPicks(picks, done), completedSeasons: done };
}

const round1 = (v: number) => Math.round(v * 10) / 10;

// nflTeam is the team the player was on in THAT league-season (his earliest
// rostered week), from edw.fact_roster - not dim_player's single current value,
// which would put 2017 Aaron Rodgers on Pittsburgh. Seasons before live loading
// have no team (Yahoo only reports current teams), so it is null there.
export const GET: RequestHandler = async ({ url }) => {
	try {
		const season = url.searchParams.get('season') || 'all';
		const analysis = url.searchParams.get('analysis') || 'basic';

		let data: any = {};



		// Basic draft data - all drafts with complete player info
		const draftQuery = `
			SELECT 
				fd.season_year,
				fd.overall_pick,
				fd.round_number,
				fd.pick_in_round,
				fd.draft_cost,
				fd.is_keeper_pick,
				fd.season_points,
				fd.fantasy_games_played,
				fd.points_per_week,
				dm.manager_name,
				dt.team_name,
				dp.player_name,
				dp.primary_position,
				dp.eligible_positions,
				(SELECT fr.nfl_team FROM edw.fact_roster fr JOIN edw.dim_week fw ON fw.week_key = fr.week_key WHERE fr.player_key = fd.player_key AND fr.league_key = fd.league_key AND fr.nfl_team IS NOT NULL ORDER BY fw.week_number LIMIT 1) AS nfl_team,
				dp.jersey_number,
				dp.rookie_year,
				dl.league_name,
				dl.num_teams
			FROM edw.fact_draft fd
			JOIN edw.dim_manager dm ON fd.manager_key = dm.manager_key AND dm.include_in_analysis = true
			JOIN edw.dim_team dt ON fd.team_key = dt.team_key
			JOIN edw.dim_player dp ON fd.player_key = dp.player_key
			JOIN edw.dim_league dl ON fd.league_key = dl.league_key
			${season !== 'all' ? `WHERE fd.season_year = ${parseInt(season)}` : ''}
			ORDER BY fd.season_year DESC, fd.overall_pick ASC
		`;

		const [draftResult, { valued, completedSeasons }] = await Promise.all([
			db.execute(sql.raw(draftQuery)),
			loadValuedPicks()
		]);
		// A season still being played is judged on its points so far.
		data.seasonInProgress = season !== 'all' && !completedSeasons.has(parseInt(season));
		const valueOf = new Map(valued.map((p) => [`${p.seasonYear}:${p.overallPick}`, p]));
		const shown = valued.filter(
			(p) => p.included && (season === 'all' || p.seasonYear === parseInt(season))
		);
		// Convert snake_case to camelCase for frontend compatibility
		data.drafts = Array.from(draftResult).map((pick: any) => ({
			seasonYear: pick.seasonYear || pick.season_year,
			overallPick: pick.overallPick || pick.overall_pick,
			roundNumber: pick.roundNumber || pick.round_number,
			pickInRound: pick.pickInRound || pick.pick_in_round,
			draftCost: pick.draftCost || pick.draft_cost,
			isKeeperPick: pick.isKeeperPick || pick.is_keeper_pick,
			seasonPoints: pick.seasonPoints || pick.season_points,
			fantasyGamesPlayed: pick.fantasyGamesPlayed || pick.fantasy_games_played,
			pointsPerWeek: pick.pointsPerWeek || pick.points_per_week,
			managerName: pick.managerName || pick.manager_name,
			teamName: pick.teamName || pick.team_name,
			playerName: pick.playerName || pick.player_name,
			primaryPosition: pick.primaryPosition || pick.primary_position,
			eligiblePositions: pick.eligiblePositions || pick.eligible_positions,
			nflTeam: pick.nflTeam || pick.nfl_team,
			jerseyNumber: pick.jerseyNumber || pick.jersey_number,
			rookieYear: pick.rookieYear || pick.rookie_year,
			leagueName: pick.leagueName || pick.league_name,
			numTeams: pick.numTeams || pick.num_teams,
			...(() => {
				const v = valueOf.get(`${pick.seasonYear ?? pick.season_year}:${pick.overallPick ?? pick.overall_pick}`);
				return v
					? {
							expectedPoints: round1(v.expectedPoints),
							valueOverExpected: round1(v.valueOverExpected),
							highExpectation: v.highExpectation
						}
					: { expectedPoints: null, valueOverExpected: null, highExpectation: false };
			})()
		}));

		// Draft analytics
		if (analysis === 'all' || analysis === 'analytics') {
			// Overall draft statistics by season
			const seasonStatsQuery = `
				SELECT 
					fd.season_year,
					COUNT(*) as total_picks,
					COUNT(DISTINCT fd.manager_key) as num_managers,
					AVG(fd.season_points) as avg_season_points,
					MAX(fd.season_points) as highest_season_points,
					MIN(fd.season_points) as lowest_season_points,
					COUNT(*) FILTER (WHERE fd.is_keeper_pick = true) as keeper_picks,
					COUNT(*) FILTER (WHERE dp.primary_position = 'QB') as qb_picks,
					COUNT(*) FILTER (WHERE dp.primary_position = 'RB') as rb_picks,
					COUNT(*) FILTER (WHERE dp.primary_position = 'WR') as wr_picks,
					COUNT(*) FILTER (WHERE dp.primary_position = 'TE') as te_picks,
					COUNT(*) FILTER (WHERE dp.primary_position IN ('K', 'DEF')) as special_picks,
					AVG(fd.draft_cost) as avg_draft_cost
				FROM edw.fact_draft fd
				JOIN edw.dim_manager dm ON fd.manager_key = dm.manager_key AND dm.include_in_analysis = true
				JOIN edw.dim_player dp ON fd.player_key = dp.player_key
				${season !== 'all' ? `WHERE fd.season_year = ${parseInt(season)}` : ''}
				GROUP BY fd.season_year
				ORDER BY fd.season_year DESC
			`;

			const seasonStatsResult = await db.execute(sql.raw(seasonStatsQuery));
			data.seasonStats = Array.from(seasonStatsResult).map((stat: any) => ({
				seasonYear: stat.seasonYear || stat.season_year,
				totalPicks: stat.totalPicks || stat.total_picks,
				numManagers: stat.numManagers || stat.num_managers,
				avgSeasonPoints: stat.avgSeasonPoints || stat.avg_season_points,
				highestSeasonPoints: stat.highestSeasonPoints || stat.highest_season_points,
				lowestSeasonPoints: stat.lowestSeasonPoints || stat.lowest_season_points,
				keeperPicks: stat.keeperPicks || stat.keeper_picks,
				qbPicks: stat.qbPicks || stat.qb_picks,
				rbPicks: stat.rbPicks || stat.rb_picks,
				wrPicks: stat.wrPicks || stat.wr_picks,
				tePicks: stat.tePicks || stat.te_picks,
				specialPicks: stat.specialPicks || stat.special_picks,
				avgDraftCost: stat.avgDraftCost || stat.avg_draft_cost
			}));

			// Manager draft performance
			const managerStatsQuery = `
				SELECT 
					dm.manager_name,
					${season !== 'all' ? 'fd.season_year,' : ''}
					COUNT(*) as picks_count,
					AVG(fd.season_points) as avg_points_per_pick,
					SUM(fd.season_points) as total_draft_points,
					AVG(fd.overall_pick) as avg_pick_position,
					MIN(fd.overall_pick) as earliest_pick,
					MAX(fd.overall_pick) as latest_pick,
					COUNT(*) FILTER (WHERE fd.is_keeper_pick = true) as keeper_count,
					AVG(fd.draft_cost) as avg_draft_cost
				FROM edw.fact_draft fd
				JOIN edw.dim_manager dm ON fd.manager_key = dm.manager_key AND dm.include_in_analysis = true
				${season !== 'all' ? `WHERE fd.season_year = ${parseInt(season)}` : ''}
				GROUP BY dm.manager_name${season !== 'all' ? ', fd.season_year' : ''}
				ORDER BY ${season !== 'all' ? 'total_draft_points' : 'avg_points_per_pick'} DESC
			`;

			const managerStatsResult = await db.execute(sql.raw(managerStatsQuery));
			data.managerPerformance = Array.from(managerStatsResult).map((manager: any) => ({
				managerName: manager.managerName || manager.manager_name,
				...((manager.seasonYear || manager.season_year) && { seasonYear: manager.seasonYear || manager.season_year }),
				picksCount: manager.picksCount || manager.picks_count,
				avgPointsPerPick: manager.avgPointsPerPick || manager.avg_points_per_pick,
				totalDraftPoints: manager.totalDraftPoints || manager.total_draft_points,
				avgPickPosition: manager.avgPickPosition || manager.avg_pick_position,
				earliestPick: manager.earliestPick || manager.earliest_pick,
				latestPick: manager.latestPick || manager.latest_pick,
				keeperCount: manager.keeperCount || manager.keeper_count,
				avgDraftCost: manager.avgDraftCost || manager.avg_draft_cost
			}));

			// Best value and biggest busts, three of each per season, judged against
			// what each pick's position and round were expected to score. This used to
			// rank raw season points, so the "busts" were always last-round kickers and
			// defenses that were cut - picks nobody counted on.
			const toCard = (pickType: 'best' | 'worst') => (p: (typeof shown)[number]) => ({
				seasonYear: p.seasonYear,
				overallPick: p.overallPick,
				roundNumber: p.roundNumber,
				seasonPoints: round1(p.seasonPoints),
				expectedPoints: round1(p.expectedPoints),
				valueOverExpected: round1(p.valueOverExpected),
				managerName: p.managerName,
				playerName: p.playerName,
				primaryPosition: p.position,
				pickType
			});
			const shownSeasons = [...new Set(shown.map((p) => p.seasonYear))].sort((a, b) => b - a);
			data.bestWorstPicks = shownSeasons.flatMap((year) => {
				const inSeason = shown.filter((p) => p.seasonYear === year);
				return [
					...topSteals(inSeason, 3).map(toCard('best')),
					...topBusts(inSeason, 3).map(toCard('worst'))
				];
			});

			// Position trends over time
			const positionTrendsQuery = `
				SELECT 
					fd.season_year,
					dp.primary_position,
					COUNT(*) as picks_count,
					AVG(fd.overall_pick) as avg_pick_position,
					AVG(fd.season_points) as avg_season_points,
					MIN(fd.overall_pick) as earliest_position_pick,
					MAX(fd.overall_pick) as latest_position_pick
				FROM edw.fact_draft fd
				JOIN edw.dim_manager dm ON fd.manager_key = dm.manager_key AND dm.include_in_analysis = true
				JOIN edw.dim_player dp ON fd.player_key = dp.player_key
				${season !== 'all' ? `WHERE fd.season_year = ${parseInt(season)}` : ''}
				GROUP BY fd.season_year, dp.primary_position
				ORDER BY fd.season_year DESC, dp.primary_position
			`;

			const positionTrendsResult = await db.execute(sql.raw(positionTrendsQuery));
			data.positionTrends = Array.from(positionTrendsResult).map((trend: any) => ({
				seasonYear: trend.seasonYear || trend.season_year,
				primaryPosition: trend.primaryPosition || trend.primary_position,
				picksCount: trend.picksCount || trend.picks_count,
				avgPickPosition: trend.avgPickPosition || trend.avg_pick_position,
				avgSeasonPoints: trend.avgSeasonPoints || trend.avg_season_points,
				earliestPositionPick: trend.earliestPositionPick || trend.earliest_position_pick,
				latestPositionPick: trend.latestPositionPick || trend.latest_position_pick
			}));

			// Draft Value: points over expectation, the same model as above.
			const mapValue = (p: (typeof shown)[number]) => ({
				seasonYear: p.seasonYear,
				overallPick: p.overallPick,
				roundNumber: p.roundNumber,
				playerName: p.playerName,
				position: p.position,
				managerName: p.managerName,
				seasonPoints: round1(p.seasonPoints),
				expectedPoints: round1(p.expectedPoints),
				valueOverExpected: round1(p.valueOverExpected)
			});
			const byManager = new Map<string, { picks: number; total: number }>();
			for (const p of shown) {
				const m = byManager.get(p.managerName) ?? { picks: 0, total: 0 };
				m.picks += 1;
				m.total += p.valueOverExpected;
				byManager.set(p.managerName, m);
			}
			data.draftValue = {
				bestValue: topSteals(shown, 10).map(mapValue),
				biggestBusts: topBusts(shown, 10).map(mapValue),
				byManager: [...byManager.entries()]
					.filter(([, m]) => m.picks >= 20)
					.map(([managerName, m]) => ({
						managerName,
						picks: m.picks,
						avgValue: round1(m.total / m.picks),
						totalValue: round1(m.total)
					}))
					.sort((a, b) => b.avgValue - a.avgValue),
				methodology:
					"Points over expectation: a pick's points minus what players at the same position, taken around the same round, have scored in this league's finished seasons (scaled to each season's scoring). Busts are only picks from rounds 1-8 expected to beat an average pick, so a late flier that was cut doesn't count. Kickers and defenses are never steals or busts, wherever they were taken."
			};

			// Manager draft grades ($lib/draft-value): a draft's points over
			// expectation in average picks. Season grades place a score among every
			// finished manager-season; career grades weigh the evidence (average
			// over its standard error). History and spread use every manager's
			// finished seasons; only managers in analysis are shown.
			const included = new Set(valued.filter((p) => p.included).map((p) => p.managerName));
			const scores = seasonDraftScores(valued);
			const finished = scores.filter((x) => completedSeasons.has(x.seasonYear));
			const history = finished.map((x) => x.score);
			const spread = spreadOf(history);
			data.draftGrades = {
				bySeason: scores
					.filter(
						(x) =>
							included.has(x.managerName) &&
							(season === 'all' || x.seasonYear === parseInt(season))
					)
					.map((x) => ({
						managerName: x.managerName,
						seasonYear: x.seasonYear,
						score: Math.round(x.score * 100) / 100,
						picks: x.picks,
						inProgress: !completedSeasons.has(x.seasonYear),
						...seasonGrade(x.score, history)
					}))
					.sort((a, b) => b.seasonYear - a.seasonYear || b.score - a.score),
				byManager: [...included]
					.map((managerName) => {
						const c = careerGrade(
							finished.filter((x) => x.managerName === managerName).map((x) => x.score),
							spread
						);
						return {
							managerName,
							grade: c.grade,
							averageScore: Math.round(c.average * 100) / 100,
							evidence: Math.round(c.z * 100) / 100,
							seasons: c.seasons
						};
					})
					.filter((m) => m.seasons > 0)
					.sort((a, b) => b.evidence - a.evidence),
				method:
					"A draft's score is its picks' points over what their positions and rounds were expected to score, in average picks: +2.4 means about two and a half average picks more than its slots promised. Season grades rank a score among every finished manager-season in league history (A top 15%, B next 25%, C middle 30%, D next 20%, F bottom 10%). Career grades weigh the evidence: a manager's average season score over its standard error, so a grade above or below C means a consistent record, not one big year."
			};
		}

		// Draft board recreation data (for specific season)
		if (season !== 'all') {
			const boardQuery = `
				SELECT 
					fd.overall_pick,
					fd.round_number,
					fd.pick_in_round,
					fd.season_points,
					fd.draft_cost,
					fd.is_keeper_pick,
					dm.manager_name,
					dt.team_name,
					dp.player_name,
					dp.primary_position,
					(SELECT fr.nfl_team FROM edw.fact_roster fr JOIN edw.dim_week fw ON fw.week_key = fr.week_key WHERE fr.player_key = fd.player_key AND fr.league_key = fd.league_key AND fr.nfl_team IS NOT NULL ORDER BY fw.week_number LIMIT 1) AS nfl_team,
					dl.num_teams
				FROM edw.fact_draft fd
				JOIN edw.dim_manager dm ON fd.manager_key = dm.manager_key AND dm.include_in_analysis = true
				JOIN edw.dim_team dt ON fd.team_key = dt.team_key
				JOIN edw.dim_player dp ON fd.player_key = dp.player_key
				JOIN edw.dim_league dl ON fd.league_key = dl.league_key
				WHERE fd.season_year = ${parseInt(season)}
				ORDER BY fd.overall_pick ASC
			`;

			const boardResult = await db.execute(sql.raw(boardQuery));
			data.draftBoard = Array.from(boardResult).map((pick: any) => ({
				overallPick: pick.overallPick || pick.overall_pick,
				roundNumber: pick.roundNumber || pick.round_number,
				pickInRound: pick.pickInRound || pick.pick_in_round,
				seasonPoints: pick.seasonPoints || pick.season_points,
				draftCost: pick.draftCost || pick.draft_cost,
				isKeeperPick: pick.isKeeperPick || pick.is_keeper_pick,
				managerName: pick.managerName || pick.manager_name,
				teamName: pick.teamName || pick.team_name,
				playerName: pick.playerName || pick.player_name,
				primaryPosition: pick.primaryPosition || pick.primary_position,
				nflTeam: pick.nflTeam || pick.nfl_team,
				numTeams: pick.numTeams || pick.num_teams,
				...(() => {
					const v = valueOf.get(`${parseInt(season)}:${pick.overallPick ?? pick.overall_pick}`);
					return v
						? {
								expectedPoints: round1(v.expectedPoints),
								valueOverExpected: round1(v.valueOverExpected),
								highExpectation: v.highExpectation
							}
						: { expectedPoints: null, valueOverExpected: null, highExpectation: false };
				})()
			}));
		}

		// Available seasons
		const seasonsQuery = `
			SELECT DISTINCT fd.season_year
			FROM edw.fact_draft fd
			WHERE fd.season_year IS NOT NULL
			ORDER BY fd.season_year DESC
		`;

		const seasonsResult = await db.execute(sql.raw(seasonsQuery));
		data.availableSeasons = Array.from(seasonsResult).map((row: any) => row.seasonYear || row.season_year);

		// Meta information
		data.meta = {
			totalDrafts: data.drafts?.length || 0,
			seasonsAvailable: data.availableSeasons?.length || 0,
			requestedSeason: season,
			analysisLevel: analysis
		};

		return json({
			data,
			meta: data.meta
		});

	} catch (error) {
		console.error('Draft API error:', error);
		return json(
			{ error: 'Failed to fetch draft data', details: error instanceof Error ? error.message : 'Unknown error' },
			{ status: 500 }
		);
	}
}; 
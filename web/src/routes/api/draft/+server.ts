import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

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
				dp.nfl_team,
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

		const draftResult = await db.execute(sql.raw(draftQuery));
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
			numTeams: pick.numTeams || pick.num_teams
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

			// Best and worst picks by season
			const bestWorstQuery = `
				WITH pick_rankings AS (
					SELECT 
						fd.season_year,
						fd.overall_pick,
						fd.round_number,
						fd.season_points,
						dm.manager_name,
						dp.player_name,
						dp.primary_position,
						ROW_NUMBER() OVER (PARTITION BY fd.season_year ORDER BY fd.season_points DESC) as best_rank,
						ROW_NUMBER() OVER (PARTITION BY fd.season_year ORDER BY fd.season_points ASC) as worst_rank
					FROM edw.fact_draft fd
					JOIN edw.dim_manager dm ON fd.manager_key = dm.manager_key AND dm.include_in_analysis = true
					JOIN edw.dim_player dp ON fd.player_key = dp.player_key
					WHERE fd.season_points > 0
					${season !== 'all' ? `AND fd.season_year = ${parseInt(season)}` : ''}
				)
				SELECT 
					season_year,
					overall_pick,
					round_number,
					season_points,
					manager_name,
					player_name,
					primary_position,
					CASE WHEN best_rank <= 3 THEN 'best' WHEN worst_rank <= 3 THEN 'worst' END as pick_type
				FROM pick_rankings
				WHERE best_rank <= 3 OR worst_rank <= 3
				ORDER BY season_year DESC, pick_type, season_points DESC
			`;

			const bestWorstResult = await db.execute(sql.raw(bestWorstQuery));
			data.bestWorstPicks = Array.from(bestWorstResult).map((pick: any) => ({
				seasonYear: pick.seasonYear || pick.season_year,
				overallPick: pick.overallPick || pick.overall_pick,
				roundNumber: pick.roundNumber || pick.round_number,
				seasonPoints: pick.seasonPoints || pick.season_points,
				managerName: pick.managerName || pick.manager_name,
				playerName: pick.playerName || pick.player_name,
				primaryPosition: pick.primaryPosition || pick.primary_position,
				pickType: pick.pickType || pick.pick_type
			}));

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
					dp.nfl_team,
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
				numTeams: pick.numTeams || pick.num_teams
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
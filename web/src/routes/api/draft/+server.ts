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
		data.drafts = Array.from(draftResult);

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
			data.season_stats = Array.from(seasonStatsResult);

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
			data.manager_performance = Array.from(managerStatsResult);

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
			data.best_worst_picks = Array.from(bestWorstResult);

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
			data.position_trends = Array.from(positionTrendsResult);
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
			data.draft_board = Array.from(boardResult);
		}

		// Available seasons
		const seasonsQuery = `
			SELECT DISTINCT season_year
			FROM edw.fact_draft
			ORDER BY season_year DESC
		`;

		const seasonsResult = await db.execute(sql.raw(seasonsQuery));
		data.available_seasons = Array.from(seasonsResult).map(row => row.season_year);

		// Meta information
		data.meta = {
			total_drafts: data.drafts?.length || 0,
			seasons_available: data.available_seasons?.length || 0,
			requested_season: season,
			analysis_level: analysis
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
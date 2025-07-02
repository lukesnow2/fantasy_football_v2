import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const manager = url.searchParams.get('manager');
		const analysis = url.searchParams.get('analysis') || 'overview';

		let data: any = {};

		// Use the actual mart_manager_performance table with correct column names
		const managerStatsQuery = `
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
			${manager ? `AND manager_name ILIKE '%${manager}%'` : ''}
			ORDER BY career_win_percentage DESC, championships_won DESC
		`;

		const statsResult = await db.execute(sql.raw(managerStatsQuery));
		const managerStats = Array.from(statsResult);

		// Add computed rankings and tier classification
		const enrichedStats = managerStats.map((mgr: any, index: number) => ({
			...mgr,
			// Overall ranking based on combined factors
			overall_rank: index + 1,
			// Championship ranking
			championship_rank: [...managerStats].sort((a: any, b: any) => (Number(b.championships_won) || 0) - (Number(a.championships_won) || 0)).findIndex((m: any) => m.manager_name === mgr.manager_name) + 1,
			// Win percentage ranking
			win_pct_rank: [...managerStats].sort((a: any, b: any) => (Number(b.career_win_percentage) || 0) - (Number(a.career_win_percentage) || 0)).findIndex((m: any) => m.manager_name === mgr.manager_name) + 1,
			// Scoring ranking
			scoring_rank: [...managerStats].sort((a: any, b: any) => (Number(b.avg_points_per_season) || 0) - (Number(a.avg_points_per_season) || 0)).findIndex((m: any) => m.manager_name === mgr.manager_name) + 1,
			// Playoff ranking
			playoff_rank: [...managerStats].sort((a: any, b: any) => (Number(b.playoff_win_percentage) || 0) - (Number(a.playoff_win_percentage) || 0)).findIndex((m: any) => m.manager_name === mgr.manager_name) + 1,
			// Tier classification based on performance
			tier_classification: mgr.manager_name === 'Bobby' ? 'League Alum' : (() => {
				const championships = parseInt(mgr.total_championships) || 0;
				const winPct = parseFloat(mgr.win_percentage) || 0;
				
				// Debug logging
				console.log('Tier Debug:', {
					manager: mgr.manager_name,
					raw_championships: mgr.total_championships,
					parsed_championships: championships,
					raw_winpct: mgr.win_percentage,
					parsed_winpct: winPct
				});
				
				if (championships >= 3) return 'League Legend';
				if (championships >= 2) return 'Dynasty Builder';
				if (championships >= 1 && winPct >= 0.60) return 'Championship Elite';
				if (championships >= 1) return 'Champion';
				if (winPct >= 0.65) return 'Elite Competitor';
				if (winPct >= 0.55) return 'Playoff Contender';
				if (winPct >= 0.45) return 'League Regular';
				return 'Rebuilding';
			})()
		}));

		data.performance = enrichedStats;
		data.rankings = enrichedStats;

		// Get achievements based on computed stats
		const achievements = enrichedStats.map((mgr: any) => ({
			manager_name: mgr.manager_name,
			championship_achievement: (mgr.total_championships || 0) >= 3 ? 'Dynasty Builder' : 
										(mgr.total_championships || 0) >= 2 ? 'Repeat Champion' : 
										(mgr.total_championships || 0) >= 1 ? 'Champion' : null,
			consistency_achievement: (mgr.win_percentage || 0) >= 0.70 ? 'Dominance' : 
									(mgr.win_percentage || 0) >= 0.60 ? 'Consistent Winner' : 
									(mgr.win_percentage || 0) >= 0.50 ? 'Above Average' : null,
			scoring_achievement: (mgr.avg_points_for || 0) >= 130 ? 'Offensive Powerhouse' : 
								(mgr.avg_points_for || 0) >= 120 ? 'High Scorer' : null,
			longevity_achievement: (mgr.total_seasons || 0) >= 15 ? 'League Veteran' : 
								  (mgr.total_seasons || 0) >= 10 ? 'Long Timer' : 
								  (mgr.total_seasons || 0) >= 5 ? 'Established' : 'Newcomer',
			all_achievements: [
				(mgr.total_championships || 0) >= 1 ? 'Champion' : null,
				(mgr.win_percentage || 0) >= 0.60 ? 'Consistent Winner' : null,
				(mgr.avg_points_for || 0) >= 120 ? 'High Scorer' : null,
				(mgr.total_seasons || 0) >= 10 ? 'Long Timer' : 
				(mgr.total_seasons || 0) >= 5 ? 'Established' : 'Newcomer'
			].filter(a => a !== null)
		}));

		data.achievements = achievements;

		// Get detailed season-by-season breakdown
		if (analysis === 'seasons' || analysis === 'all') {
			try {
				const seasonsQuery = `
					SELECT 
						tp.season_year,
						dm.manager_name,
						tp.wins,
						tp.losses,
						tp.ties,
						tp.win_percentage,
						tp.points_for,
						tp.points_against
					FROM edw.fact_team_performance tp
					JOIN edw.dim_manager dm ON tp.manager_key = dm.manager_key
					WHERE dm.include_in_analysis = true
					${manager ? `AND dm.manager_name ILIKE '%${manager}%'` : ''}
					ORDER BY tp.season_year DESC
				`;

				const seasonsResult = await db.execute(sql.raw(seasonsQuery));
				data.seasons = Array.from(seasonsResult);
			} catch (seasonsError) {
				console.log('Error getting seasons data, skipping:', seasonsError);
				data.seasons = [];
			}
		}

		// Get manager head-to-head dominance (if table exists)
		if (analysis === 'h2h' || analysis === 'all') {
			try {
				const h2hQuery = `
					SELECT 
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_a_name
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_b_name
							ELSE manager_a_name
						END as manager,
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_b_name
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_a_name
							ELSE manager_b_name
						END as opponent,
						total_matchups,
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_a_wins
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_b_wins
							ELSE manager_a_wins
						END as wins,
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_a_losses
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_b_losses
							ELSE manager_a_losses
						END as losses,
						CASE 
							WHEN manager_a_name ILIKE '%${manager || ''}%' THEN manager_a_win_percentage
							WHEN manager_b_name ILIKE '%${manager || ''}%' THEN manager_b_win_percentage
							ELSE manager_a_win_percentage
						END as win_percentage
					FROM edw.mart_manager_h2h
					WHERE total_matchups >= 5
					${manager ? `AND (manager_a_name ILIKE '%${manager}%' OR manager_b_name ILIKE '%${manager}%')` : ''}
					ORDER BY win_percentage DESC, total_matchups DESC
				`;

				const h2hResult = await db.execute(sql.raw(h2hQuery));
				data.head_to_head = Array.from(h2hResult);
			} catch (h2hError) {
				console.log('H2H mart table not available, skipping');
				data.head_to_head = [];
			}
		}

		// Available managers list
		data.available_managers = managerStats.map((mgr: any) => mgr.manager_name);

		// Meta information
		data.meta = {
			total_managers: data.available_managers?.length || 0,
			requested_manager: manager,
			analysis_level: analysis
		};

		return json({
			data,
			meta: data.meta
		});

	} catch (error) {
		console.error('Manager performance API error:', error);
		return json(
			{ error: 'Failed to fetch manager performance data', details: error instanceof Error ? error.message : 'Unknown error' },
			{ status: 500 }
		);
	}
}; 
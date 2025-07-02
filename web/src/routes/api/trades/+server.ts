import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const season = url.searchParams.get('season') || 'all';
		const manager = url.searchParams.get('manager');
		const limit = parseInt(url.searchParams.get('limit') || '100');
		const analysis = url.searchParams.get('analysis') || 'overview';

		// Base trade analysis query with proper SQL syntax
		let query = `
			SELECT 
				league_name,
				season_year,
				transaction_date,
				transaction_week,
				team_a_name,
				team_a_manager,
				team_a_gives,
				team_b_name, 
				team_b_manager,
				team_b_gives,
				trade_type,
				total_players,
				team_a_production,
				team_b_production,
				production_differential,
				team_a_pre_trade_avg,
				team_b_pre_trade_avg,
				opportunity_differential,
				team_a_made_playoffs,
				team_b_made_playoffs,
				team_a_champion,
				team_b_champion,
				team_a_production_score,
				team_a_playoff_score,
				team_a_opportunity_score,
				team_a_context_score,
				team_a_final_score,
				team_b_final_score,
				trade_winner,
				trade_analysis,
				evaluation_status,
				trade_group_id
			FROM edw.vw_trade_analysis
			WHERE 1=1
		`;

		// Add filters
		if (season && season !== 'all') {
			query += ` AND season_year = ${parseInt(season)}`;
		}

		if (manager) {
			query += ` AND (team_a_manager ILIKE '%${manager}%' OR team_b_manager ILIKE '%${manager}%')`;
		}

		query += ` ORDER BY transaction_date DESC LIMIT ${limit}`;

		console.log('Executing trade query:', query);
		const trades = await db.execute(sql.raw(query));

		// Generate analytics based on request type
		let analytics: any = {};

		if (analysis === 'overview' || analysis === 'all') {
			// Overview analytics
			const analyticsQuery = `
				SELECT 
					COUNT(*) as total_trades,
					AVG(ABS(production_differential)) as avg_production_impact,
					COUNT(*) FILTER (WHERE ABS(team_a_final_score - team_b_final_score) >= 8) as decisive_trades,
					COUNT(*) FILTER (WHERE trade_winner = 'Even Trade') as even_trades,
					COUNT(*) FILTER (WHERE team_a_champion = 1 OR team_b_champion = 1) as championship_impact_trades,
					AVG(total_players) as avg_players_per_trade,
					MAX(total_players) as biggest_trade_players,
					COUNT(DISTINCT team_a_manager) + COUNT(DISTINCT team_b_manager) as active_traders
				FROM edw.vw_trade_analysis
				${season && season !== 'all' ? `WHERE season_year = ${parseInt(season)}` : ''}
			`;

			console.log('Executing analytics query:', analyticsQuery);
			const analyticsResult = await db.execute(sql.raw(analyticsQuery));
			analytics = {
				...analytics,
				overview: Array.from(analyticsResult)[0]
			};
		}

		if (analysis === 'managers' || analysis === 'all') {
			// Manager performance analytics
			const managerQuery = `
				WITH manager_trades AS (
					SELECT 
						CASE 
							WHEN team_a_final_score > team_b_final_score THEN team_a_manager
							WHEN team_b_final_score > team_a_final_score THEN team_b_manager
							ELSE NULL 
						END as winner,
						CASE 
							WHEN team_a_final_score < team_b_final_score THEN team_a_manager
							WHEN team_b_final_score < team_a_final_score THEN team_b_manager
							ELSE NULL 
						END as loser,
						team_a_manager,
						team_b_manager,
						ABS(team_a_final_score - team_b_final_score) as score_differential,
						team_a_final_score,
						team_b_final_score,
						CASE WHEN team_a_champion = 1 THEN team_a_manager 
							 WHEN team_b_champion = 1 THEN team_b_manager 
							 ELSE NULL END as championship_impact,
						trade_winner
					FROM edw.vw_trade_analysis
					${season && season !== 'all' ? `WHERE season_year = ${parseInt(season)}` : ''}
				),
				all_managers AS (
					SELECT team_a_manager as manager_name FROM manager_trades
					UNION 
					SELECT team_b_manager as manager_name FROM manager_trades
				)
				SELECT 
					m.manager_name,
					COUNT(*) as total_trades,
					COUNT(*) FILTER (WHERE mt.winner = m.manager_name) as wins,
					COUNT(*) FILTER (WHERE mt.loser = m.manager_name) as losses,
					COUNT(*) FILTER (WHERE mt.trade_winner = 'Even Trade' AND (mt.team_a_manager = m.manager_name OR mt.team_b_manager = m.manager_name)) as even_trades,
					ROUND(
						COUNT(*) FILTER (WHERE mt.winner = m.manager_name) * 100.0 / 
						NULLIF(COUNT(*) FILTER (WHERE mt.winner = m.manager_name OR mt.loser = m.manager_name), 0), 1
					) as win_percentage,
					COUNT(*) FILTER (WHERE mt.championship_impact = m.manager_name) as championship_trades,
					ROUND(AVG(
						CASE WHEN mt.team_a_manager = m.manager_name THEN mt.team_a_final_score
							 WHEN mt.team_b_manager = m.manager_name THEN mt.team_b_final_score
							 ELSE NULL END
					), 1) as avg_trade_score
				FROM all_managers m
				LEFT JOIN manager_trades mt ON (mt.team_a_manager = m.manager_name OR mt.team_b_manager = m.manager_name)
				WHERE m.manager_name IS NOT NULL
				GROUP BY m.manager_name
				HAVING COUNT(*) > 0
				ORDER BY win_percentage DESC NULLS LAST, total_trades DESC
			`;

			console.log('Executing manager query');
			const managerResult = await db.execute(sql.raw(managerQuery));
			analytics = {
				...analytics,
				managers: Array.from(managerResult)
			};
		}

		if (analysis === 'trends' || analysis === 'all') {
			// Trade trends analytics
			const trendsQuery = `
				SELECT 
					season_year,
					COUNT(*) as total_trades,
					AVG(total_players) as avg_players_per_trade,
					COUNT(*) FILTER (WHERE trade_winner != 'Even Trade') as decisive_trades,
					COUNT(*) FILTER (WHERE team_a_champion = 1 OR team_b_champion = 1) as championship_impact,
					AVG(ABS(production_differential)) as avg_production_impact,
					COUNT(*) FILTER (WHERE transaction_week >= 10) as late_season_trades
				FROM edw.vw_trade_analysis
				GROUP BY season_year
				ORDER BY season_year DESC
			`;

			console.log('Executing trends query');
			const trendsResult = await db.execute(sql.raw(trendsQuery));
			analytics = {
				...analytics,
				trends: Array.from(trendsResult)
			};
		}

		if (analysis === 'best_worst' || analysis === 'all') {
			// Best and worst trades
			const bestWorstQuery = `
				WITH ranked_trades AS (
					SELECT *,
						ABS(team_a_final_score - team_b_final_score) as score_gap,
						CASE 
							WHEN team_a_final_score > team_b_final_score THEN 
								CONCAT(team_a_manager, ' (', team_a_final_score, ' vs ', team_b_final_score, ')')
							WHEN team_b_final_score > team_a_final_score THEN 
								CONCAT(team_b_manager, ' (', team_b_final_score, ' vs ', team_a_final_score, ')')
							ELSE 'Even Trade'
						END as trade_result
					FROM edw.vw_trade_analysis
					${season && season !== 'all' ? `WHERE season_year = ${parseInt(season)}` : ''}
				)
				(
					SELECT 'best' as category, * FROM ranked_trades 
					WHERE trade_winner != 'Even Trade'
					ORDER BY score_gap DESC, ABS(production_differential) DESC
					LIMIT 5
				)
				UNION ALL
				(
					SELECT 'championship' as category, * FROM ranked_trades 
					WHERE team_a_champion = 1 OR team_b_champion = 1
					ORDER BY season_year DESC
					LIMIT 5
				)
			`;

			console.log('Executing best/worst query');
			const bestWorstResult = await db.execute(sql.raw(bestWorstQuery));
			const bestWorstArray = Array.from(bestWorstResult);
			
			const bestTrades = bestWorstArray.filter((t: any) => t.category === 'best');
			const championshipTrades = bestWorstArray.filter((t: any) => t.category === 'championship');

			analytics = {
				...analytics,
				best_trades: bestTrades,
				championship_trades: championshipTrades
			};
		}

		const tradesArray = Array.from(trades);
		console.log(`Returning ${tradesArray.length} trades and analytics:`, Object.keys(analytics));

		return json({
			trades: tradesArray,
			analytics,
			meta: {
				season,
				manager,
				limit,
				analysis,
				total_returned: tradesArray.length
			}
		});

	} catch (error) {
		console.error('Error fetching trade analysis:', error);
		return json({ 
			error: 'Failed to fetch trade analysis data',
			details: error instanceof Error ? error.message : String(error)
		}, { status: 500 });
	}
}; 
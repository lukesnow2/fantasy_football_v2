import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const season = url.searchParams.get('season') || 'all';
		const manager = url.searchParams.get('manager');
		const limit = parseInt(url.searchParams.get('limit') || '1000');
		const analysis = url.searchParams.get('analysis') || 'overview';

		// Test if the view exists and has data
		try {
			const testQuery = `SELECT COUNT(*) as count FROM edw.vw_trade_analysis`;
			const testResult = await db.execute(sql.raw(testQuery));
			console.log('Trade analysis view count:', testResult[0]);
			
			// DEBUG: Check raw championship data from view
			const championshipViewQuery = `
				SELECT 
					season_year,
					team_a_manager,
					team_b_manager,
					team_a_champion,
					team_b_champion,
					trade_winner,
					production_differential
				FROM edw.vw_trade_analysis
				WHERE team_a_champion = 1 OR team_b_champion = 1
				ORDER BY season_year DESC
				LIMIT 10
			`;
			const championshipViewResult = await db.execute(sql.raw(championshipViewQuery));
			console.log('=== DATABASE VIEW CHAMPIONSHIP DEBUG ===');
			console.log('Championship trades in view:', championshipViewResult.length);
			Array.from(championshipViewResult).forEach((trade: any) => {
				console.log(`Season ${trade.season_year}: ${trade.team_a_manager} vs ${trade.team_b_manager}`);
				console.log(`  Team A Champion: ${trade.team_a_champion}, Team B Champion: ${trade.team_b_champion}`);
				console.log(`  Trade Winner: ${trade.trade_winner}, Production Diff: ${trade.production_differential}`);
			});
			console.log('=== END DATABASE VIEW CHAMPIONSHIP DEBUG ===\n');
			
			// DEBUG: Check if any trades have champion flags at all
			const anyChampionQuery = `
				SELECT 
					COUNT(*) as total_trades,
					COUNT(*) FILTER (WHERE team_a_champion IS NOT NULL) as team_a_champion_count,
					COUNT(*) FILTER (WHERE team_b_champion IS NOT NULL) as team_b_champion_count,
					COUNT(*) FILTER (WHERE team_a_champion = 1) as team_a_champion_true,
					COUNT(*) FILTER (WHERE team_b_champion = 1) as team_b_champion_true
				FROM edw.vw_trade_analysis
			`;
			const anyChampionResult = await db.execute(sql.raw(anyChampionQuery));
			console.log('=== CHAMPION FLAGS SUMMARY ===');
			console.log('Champion flags summary:', Array.from(anyChampionResult)[0]);
			console.log('=== END CHAMPION FLAGS SUMMARY ===\n');
			
		} catch (error) {
			console.error('Trade analysis view error:', error);
		}

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
		console.log('Trade query result count:', trades.length);
		if (trades.length > 0) {
			console.log('First trade record:', trades[0]);
		}

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
			console.log('Analytics result:', Array.from(analyticsResult));
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
			
			// DEBUG: Add comprehensive logging for championship trades
			console.log('=== CHAMPIONSHIP TRADES DEBUG ===');
			console.log('Best/worst query result count:', bestWorstArray.length);
			
			// Log all championship trades with detailed information
			const championshipTrades = bestWorstArray.filter((t: any) => t.category === 'championship');
			console.log('Championship trades found:', championshipTrades.length);
			
			championshipTrades.forEach((trade: any, index: number) => {
				console.log(`\n--- Championship Trade ${index + 1} ---`);
				console.log('Season:', trade.season_year);
				console.log('Team A Manager:', trade.team_a_manager);
				console.log('Team B Manager:', trade.team_b_manager);
				console.log('Team A Champion Flag:', trade.team_a_champion);
				console.log('Team B Champion Flag:', trade.team_b_champion);
				console.log('Trade Winner:', trade.trade_winner);
				console.log('Production Differential:', trade.production_differential);
				console.log('Team A Final Score:', trade.team_a_final_score);
				console.log('Team B Final Score:', trade.team_b_final_score);
				console.log('Trade Analysis:', trade.trade_analysis);
			});
			
			// DEBUG: Check raw championship data in fact_matchup
			try {
				const championshipDataQuery = `
					SELECT 
						fm.season_year,
						fm.winner_team_key,
						fm.is_championship,
						dt.team_name,
						dm.manager_name
					FROM edw.fact_matchup fm
					LEFT JOIN edw.dim_team dt ON fm.winner_team_key = dt.team_key
					LEFT JOIN edw.dim_manager dm ON dt.manager_key = dm.manager_key
					WHERE fm.is_championship = 1
					ORDER BY fm.season_year DESC
				`;
				const championshipData = await db.execute(sql.raw(championshipDataQuery));
				console.log('\n=== RAW CHAMPIONSHIP DATA ===');
				console.log('Championship matchups found:', championshipData.length);
				Array.from(championshipData).forEach((champ: any) => {
					console.log(`Season ${champ.season_year}: ${champ.manager_name} (${champ.team_name}) - Team Key: ${champ.winner_team_key}`);
				});
			} catch (error) {
				console.error('Error fetching championship data:', error);
			}
			
			// DEBUG: Check team-manager mappings
			try {
				const teamManagerQuery = `
					SELECT DISTINCT
						dt.team_key,
						dt.team_name,
						dm.manager_name,
						dm.manager_key
					FROM edw.dim_team dt
					LEFT JOIN edw.dim_manager dm ON dt.manager_key = dm.manager_key
					WHERE dm.manager_name ILIKE '%omar%' OR dm.manager_name ILIKE '%luke%'
					ORDER BY dm.manager_name
				`;
				const teamManagerData = await db.execute(sql.raw(teamManagerQuery));
				console.log('\n=== TEAM-MANAGER MAPPINGS ===');
				console.log('Omar/Luke team mappings:', Array.from(teamManagerData));
			} catch (error) {
				console.error('Error fetching team-manager mappings:', error);
			}
			
			// DEBUG: Check trade analysis view for Omar's trades
			try {
				const omarTradesQuery = `
					SELECT 
						season_year,
						team_a_manager,
						team_b_manager,
						team_a_champion,
						team_b_champion,
						trade_winner,
						production_differential
					FROM edw.vw_trade_analysis
					WHERE team_a_manager ILIKE '%omar%' OR team_b_manager ILIKE '%omar%'
					ORDER BY season_year DESC, transaction_date DESC
				`;
				const omarTrades = await db.execute(sql.raw(omarTradesQuery));
				console.log('\n=== OMAR TRADES DEBUG ===');
				console.log('Omar trades found:', omarTrades.length);
				Array.from(omarTrades).forEach((trade: any) => {
					console.log(`Season ${trade.season_year}: ${trade.team_a_manager} vs ${trade.team_b_manager}`);
					console.log(`  Team A Champion: ${trade.team_a_champion}, Team B Champion: ${trade.team_b_champion}`);
					console.log(`  Trade Winner: ${trade.trade_winner}, Production Diff: ${trade.production_differential}`);
				});
			} catch (error) {
				console.error('Error fetching Omar trades:', error);
			}
			
			console.log('=== END CHAMPIONSHIP TRADES DEBUG ===\n');
			
			const bestTrades = bestWorstArray.filter((t: any) => t.category === 'best');
			
			console.log('Best trades count:', bestTrades.length);
			console.log('Championship trades count:', championshipTrades.length);
			if (championshipTrades.length > 0) {
				console.log('First championship trade:', championshipTrades[0]);
			}

			analytics = {
				...analytics,
				best_trades: bestTrades,
				championship_trades: championshipTrades
			};
		}

		// Convert snake_case to camelCase for frontend compatibility
		const tradesArray = Array.from(trades).map((trade: any) => {
			// DEBUG: Log raw trade data before conversion
			console.log('=== TRADE DATA CONVERSION DEBUG ===');
			console.log('Raw trade data keys:', Object.keys(trade));
			console.log('Raw team_a_champion:', trade.team_a_champion);
			console.log('Raw team_b_champion:', trade.team_b_champion);
			console.log('Raw team_a_manager:', trade.team_a_manager);
			console.log('Raw team_b_manager:', trade.team_b_manager);
			
			const convertedTrade = {
				leagueName: trade.leagueName || trade.league_name,
				seasonYear: trade.seasonYear || trade.season_year,
				transactionDate: trade.transactionDate || trade.transaction_date,
				transactionWeek: trade.transactionWeek || trade.transaction_week,
				teamAName: trade.teamAName || trade.team_a_name,
				teamAManager: trade.teamAManager || trade.team_a_manager,
				teamAGives: trade.teamAGives || trade.team_a_gives,
				teamBName: trade.teamBName || trade.team_b_name,
				teamBManager: trade.teamBManager || trade.team_b_manager,
				teamBGives: trade.teamBGives || trade.team_b_gives,
				tradeType: trade.tradeType || trade.trade_type,
				totalPlayers: trade.totalPlayers || trade.total_players,
				teamAProduction: trade.teamAProduction || trade.team_a_production,
				teamBProduction: trade.teamBProduction || trade.team_b_production,
				productionDifferential: trade.productionDifferential || trade.production_differential,
				teamAPreTradeAvg: trade.teamAPreTradeAvg || trade.team_a_pre_trade_avg,
				teamBPreTradeAvg: trade.teamBPreTradeAvg || trade.team_b_pre_trade_avg,
				opportunityDifferential: trade.opportunityDifferential || trade.opportunity_differential,
				teamAMadePlayoffs: trade.teamAMadePlayoffs || trade.team_a_made_playoffs,
				teamBMadePlayoffs: trade.teamBMadePlayoffs || trade.team_b_made_playoffs,
				teamAChampion: trade.teamAChampion || trade.team_a_champion,
				teamBChampion: trade.teamBChampion || trade.team_b_champion,
				teamAProductionScore: trade.teamAProductionScore || trade.team_a_production_score,
				teamAPlayoffScore: trade.teamAPlayoffScore || trade.team_a_playoff_score,
				teamAOpportunityScore: trade.teamAOpportunityScore || trade.team_a_opportunity_score,
				teamAContextScore: trade.teamAContextScore || trade.team_a_context_score,
				teamAFinalScore: trade.teamAFinalScore || trade.team_a_final_score,
				teamBFinalScore: trade.teamBFinalScore || trade.team_b_final_score,
				tradeWinner: trade.tradeWinner || trade.trade_winner,
				tradeAnalysis: trade.tradeAnalysis || trade.trade_analysis,
				evaluationStatus: trade.evaluationStatus || trade.evaluation_status,
				tradeGroupId: trade.tradeGroupId || trade.trade_group_id
			};
			
			// DEBUG: Log converted trade data
			console.log('Converted teamAChampion:', convertedTrade.teamAChampion);
			console.log('Converted teamBChampion:', convertedTrade.teamBChampion);
			console.log('Converted teamAManager:', convertedTrade.teamAManager);
			console.log('Converted teamBManager:', convertedTrade.teamBManager);
			console.log('=== END TRADE DATA CONVERSION DEBUG ===\n');
			
			return convertedTrade;
		});

		// Convert analytics to camelCase
		const camelCaseAnalytics: any = {};
		if (analytics.overview) {
			camelCaseAnalytics.overview = {
				totalTrades: analytics.overview.totalTrades || analytics.overview.total_trades,
				avgProductionImpact: analytics.overview.avgProductionImpact || analytics.overview.avg_production_impact,
				decisiveTrades: analytics.overview.decisiveTrades || analytics.overview.decisive_trades,
				evenTrades: analytics.overview.evenTrades || analytics.overview.even_trades,
				championshipImpactTrades: analytics.overview.championshipImpactTrades || analytics.overview.championship_impact_trades,
				avgPlayersPerTrade: analytics.overview.avgPlayersPerTrade || analytics.overview.avg_players_per_trade,
				biggestTradePlayers: analytics.overview.biggestTradePlayers || analytics.overview.biggest_trade_players,
				activeTraders: analytics.overview.activeTraders || analytics.overview.active_traders
			};
		}

		if (analytics.managers) {
			camelCaseAnalytics.managers = analytics.managers.map((manager: any) => ({
				managerName: manager.managerName || manager.manager_name,
				totalTrades: manager.totalTrades || manager.total_trades,
				wins: manager.wins,
				losses: manager.losses,
				evenTrades: manager.evenTrades || manager.even_trades,
				winPercentage: manager.winPercentage || manager.win_percentage,
				championshipTrades: manager.championshipTrades || manager.championship_trades,
				avgTradeScore: manager.avgTradeScore || manager.avg_trade_score
			}));
		}

		if (analytics.trends) {
			camelCaseAnalytics.trends = analytics.trends.map((trend: any) => ({
				seasonYear: trend.seasonYear || trend.season_year,
				totalTrades: trend.totalTrades || trend.total_trades,
				avgPlayersPerTrade: trend.avgPlayersPerTrade || trend.avg_players_per_trade,
				decisiveTrades: trend.decisiveTrades || trend.decisive_trades,
				championshipImpact: trend.championshipImpact || trend.championship_impact,
				avgProductionImpact: trend.avgProductionImpact || trend.avg_production_impact,
				lateSeasonTrades: trend.lateSeasonTrades || trend.late_season_trades
			}));
		}

		if (analytics.best_trades) {
			camelCaseAnalytics.bestTrades = analytics.best_trades.map((trade: any) => ({
				category: trade.category,
				leagueName: trade.leagueName || trade.league_name,
				seasonYear: trade.seasonYear || trade.season_year,
				transactionDate: trade.transactionDate || trade.transaction_date,
				teamAName: trade.teamAName || trade.team_a_name,
				teamAManager: trade.teamAManager || trade.team_a_manager,
				teamBName: trade.teamBName || trade.team_b_name,
				teamBManager: trade.teamBManager || trade.team_b_manager,
				tradeWinner: trade.tradeWinner || trade.trade_winner,
				teamAFinalScore: trade.teamAFinalScore || trade.team_a_final_score,
				teamBFinalScore: trade.teamBFinalScore || trade.team_b_final_score,
				productionDifferential: trade.productionDifferential || trade.production_differential,
				tradeResult: trade.tradeResult || trade.trade_result
			}));
		}

		if (analytics.championship_trades) {
			camelCaseAnalytics.championshipTrades = analytics.championship_trades.map((trade: any) => {
				// DEBUG: Log raw championship trade data before conversion
				console.log('=== CHAMPIONSHIP TRADE CONVERSION DEBUG ===');
				console.log('Raw championship trade keys:', Object.keys(trade));
				console.log('Raw team_a_champion:', trade.team_a_champion);
				console.log('Raw team_b_champion:', trade.team_b_champion);
				console.log('Raw team_a_manager:', trade.team_a_manager);
				console.log('Raw team_b_manager:', trade.team_b_manager);
				
				const convertedChampionshipTrade = {
					category: trade.category,
					leagueName: trade.leagueName || trade.league_name,
					seasonYear: trade.seasonYear || trade.season_year,
					transactionDate: trade.transactionDate || trade.transaction_date,
					teamAName: trade.teamAName || trade.team_a_name,
					teamAManager: trade.teamAManager || trade.team_a_manager,
					teamBName: trade.teamBName || trade.team_b_name,
					teamBManager: trade.teamBManager || trade.team_b_manager,
					tradeWinner: trade.tradeWinner || trade.trade_winner,
					teamAFinalScore: trade.teamAFinalScore || trade.team_a_final_score,
					teamBFinalScore: trade.teamBFinalScore || trade.team_b_final_score,
					productionDifferential: trade.productionDifferential || trade.production_differential,
					tradeResult: trade.tradeResult || trade.trade_result,
					teamAChampion: trade.teamAChampion || trade.team_a_champion,
					teamBChampion: trade.teamBChampion || trade.team_b_champion
				};
				
				// DEBUG: Log converted championship trade data
				console.log('Converted teamAChampion:', convertedChampionshipTrade.teamAChampion);
				console.log('Converted teamBChampion:', convertedChampionshipTrade.teamBChampion);
				console.log('Converted teamAManager:', convertedChampionshipTrade.teamAManager);
				console.log('Converted teamBManager:', convertedChampionshipTrade.teamBManager);
				console.log('=== END CHAMPIONSHIP TRADE CONVERSION DEBUG ===\n');
				
				return convertedChampionshipTrade;
			});
		}

		console.log(`Returning ${tradesArray.length} trades and analytics:`, Object.keys(camelCaseAnalytics));
		
		// Debug analytics structure
		console.log('Analytics overview:', camelCaseAnalytics.overview);
		console.log('Analytics championshipTrades:', camelCaseAnalytics.championshipTrades);
		console.log('Analytics championshipTrades length:', camelCaseAnalytics.championshipTrades?.length);

		return json({
			trades: tradesArray,
			analytics: camelCaseAnalytics,
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
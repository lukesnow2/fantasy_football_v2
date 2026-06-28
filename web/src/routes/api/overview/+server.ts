import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

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
			const scoringQuery = `
				SELECT 
					season_year,
					AVG(weekly_points) as avg_weekly_score,
					MAX(weekly_points) as max_weekly_score,
					MIN(weekly_points) as min_weekly_score,
					STDDEV(weekly_points) as score_volatility,
					COUNT(*) as total_team_weeks,
					PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY weekly_points) as p90_score,
					PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY weekly_points) as p10_score
				FROM edw.fact_team_performance
				WHERE weekly_points > 0
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
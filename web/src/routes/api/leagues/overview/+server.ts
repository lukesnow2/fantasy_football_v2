import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { dimLeague, dimManager, factTransaction, factMatchup, factTeamPerformance } from '$lib/server/db/schema';
import { count, countDistinct, max, min, eq } from 'drizzle-orm';
import type { RequestHandler } from './$types';
import { sql } from 'drizzle-orm';

export const GET: RequestHandler = async () => {
	try {
		// Get basic league counts
		const leagueStats = await db
			.select({
				totalLeagues: countDistinct(dimLeague.leagueId),
				totalSeasons: countDistinct(dimLeague.seasonYear),
				firstSeason: min(dimLeague.seasonYear),
				lastSeason: max(dimLeague.seasonYear)
			})
			.from(dimLeague)
			.where(eq(dimLeague.isActive, true));

		// Get manager counts
		const managerStats = await db
			.select({
				totalManagers: count(dimManager.managerKey)
			})
			.from(dimManager)
			.where(eq(dimManager.isActive, true));

		// Get transaction counts
		const transactionStats = await db
			.select({
				totalTransactions: count(factTransaction.transactionKey)
			})
			.from(factTransaction);

		// Get matchup counts
		const matchupStats = await db
			.select({
				totalMatchups: count(factMatchup.matchupKey)
			})
			.from(factMatchup);

		const championshipStats = await db
			.select({
				championshipGames: count(factMatchup.matchupKey)
			})
			.from(factMatchup)
			.where(eq(factMatchup.isChampionship, true));

		// Get team performance counts (represents teams across all seasons)
		const teamStats = await db
			.select({
				totalTeamSeasons: count(factTeamPerformance.performanceKey)
			})
			.from(factTeamPerformance);

		// Draft pick count (fact_draft isn't modeled in Drizzle, so query it directly)
		const draftPickResult = await db.execute(sql.raw(`SELECT COUNT(*) AS total FROM edw.fact_draft`));
		const totalDraftPicks = Number(Array.from(draftPickResult)[0]?.total) || 0;

		// Query for all-time leader (Hall of Fame #1)
		const hallOfFameLeaderResult = await db.execute(sql.raw(`
			SELECT manager_name
			FROM vw_manager_hall_of_fame
			WHERE hall_of_fame_rank = 1
			ORDER BY hall_of_fame_rank ASC
			LIMIT 1
		`));
		
		// Debug logging for Hall of Fame query
		
		const allTimeLeader = Array.from(hallOfFameLeaderResult)[0]?.managerName || 'TBD';

		// Query for biggest rivalry (most games played between two managers)
		const biggestRivalryResult = await db.execute(sql.raw(`
			SELECT manager_a_name, manager_b_name, total_matchups
			FROM edw.mart_manager_h2h
			ORDER BY total_matchups DESC
			LIMIT 1
		`));
		
		// Debug logging for Rivalry query
		
		const rivalryRow = Array.from(biggestRivalryResult)[0];
		const biggestRivalry = rivalryRow ? `${rivalryRow.managerAName} vs ${rivalryRow.managerBName}` : 'Analyzing...';

		// Get current season data directly from database
		let currentSeasonData = {
			currentWeek: 1,
			isSeasonComplete: false,
			playoffStatus: 'Regular Season',
			tradeDeadlineStatus: 'Active'
		};

		try {
			// Get the latest season and its current week
			const latestSeasonResult = await db.execute(sql.raw(`
				SELECT MAX(season_year) as latest_season 
				FROM edw.fact_draft
			`));
			const latestSeason = parseInt(latestSeasonResult[0]?.latestSeason?.toString() || '2024');
			
			// Get current week for the latest season
			const currentWeekResult = await db.execute(sql.raw(`
				SELECT MAX(week_number) as current_week 
				FROM edw.dim_week dw
				JOIN edw.fact_team_performance ftp ON dw.week_key = ftp.week_key
				JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
				WHERE dl.season_year = ${latestSeason}
			`));
			const currentWeek = parseInt(currentWeekResult[0]?.currentWeek?.toString() || '1');
			
			// Check if season is complete (has championship game)
			const championshipResult = await db.execute(sql.raw(`
				SELECT COUNT(*) as championship_count
				FROM edw.fact_matchup fm
				JOIN edw.dim_league dl ON fm.league_key = dl.league_key
				WHERE dl.season_year = ${latestSeason} AND fm.is_championship = true
			`));
			const championshipCount = parseInt(championshipResult[0]?.championshipCount?.toString() || '0');
			const isSeasonComplete = championshipCount > 0;
			
			currentSeasonData = {
				currentWeek: currentWeek,
				isSeasonComplete: isSeasonComplete,
				playoffStatus: isSeasonComplete ? 'Championship Decided' : (currentWeek >= 14 ? 'Playoffs' : 'Regular Season'),
				tradeDeadlineStatus: currentWeek >= 10 ? 'Passed' : 'Active'
			};
		} catch (error) {
			console.warn('Could not fetch current season data from database:', error);
		}

		const overview = {
			totalSeasons: leagueStats[0]?.totalSeasons || 0,
			totalLeagues: leagueStats[0]?.totalLeagues || 0,
			firstSeason: leagueStats[0]?.firstSeason?.toString() || 'Unknown',
			lastSeason: leagueStats[0]?.lastSeason?.toString() || 'Unknown',
			totalManagers: managerStats[0]?.totalManagers || 0,
			totalTeams: teamStats[0]?.totalTeamSeasons || 0,
			totalTransactions: transactionStats[0]?.totalTransactions || 0,
			totalDraftPicks,
			totalMatchups: matchupStats[0]?.totalMatchups || 0,
			championshipGames: championshipStats[0]?.championshipGames || 0,
			dataPoints: (teamStats[0]?.totalTeamSeasons || 0) + 
						(transactionStats[0]?.totalTransactions || 0) + 
						(matchupStats[0]?.totalMatchups || 0),
			allTimeLeader,
			biggestRivalry,
			// Current season data
			currentWeek: currentSeasonData.currentWeek,
			playoffStatus: currentSeasonData.playoffStatus,
			tradeDeadlineStatus: currentSeasonData.tradeDeadlineStatus
		};

		return json({
			overview,
			lastUpdated: new Date().toISOString()
		});

	} catch (error) {
		console.error('Error fetching league overview:', error);
		return json({ error: 'Failed to fetch league overview' }, { status: 500 });
	}
}; 
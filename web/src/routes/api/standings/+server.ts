import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { vwCurrentSeasonDashboard } from '$lib/server/db/schema';
import { eq, desc } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const requestedSeason = url.searchParams.get('season') || '2024';
		
		// Get current season standings from the dashboard view
		// Note: This view is hardcoded to current year and current week
		const standings = await db
			.select()
			.from(vwCurrentSeasonDashboard)
			.orderBy(vwCurrentSeasonDashboard.seasonRank);

		// Format data for frontend
		const standingsWithStats = standings.map((team, index) => {
			const winPercentage = team.winPercentage ? 
				(parseFloat(team.winPercentage.toString()) * 100).toFixed(1) : '0.0';
			const pointDifferential = team.pointDifferential ? 
				parseFloat(team.pointDifferential.toString()).toFixed(1) : '0.0';
			
			return {
				teamId: team.seasonRank || (index + 1), // Use actual season rank
				teamName: team.teamName,
				managerName: team.managerName,
				wins: team.wins || 0,
				losses: team.losses || 0,
				ties: team.ties || 0,
				pointsFor: team.pointsFor ? parseFloat(team.pointsFor.toString()) : 0,
				pointsAgainst: team.pointsAgainst ? parseFloat(team.pointsAgainst.toString()) : 0,
				playoffSeed: team.playoffSeed,
				rank: team.seasonRank || (index + 1), // Use actual season rank from database
				winPercentage,
				pointDifferential,
				leagueName: team.leagueName,
				isPlayoffTeam: team.isPlayoffTeam,
				playoffProbability: team.playoffProbability ? 
					(parseFloat(team.playoffProbability.toString()) * 100).toFixed(1) : '0.0',
				// Mock streak for now - would need to calculate from recent matchups
				streak: index % 3 === 0 ? `W${Math.floor(Math.random() * 3) + 1}` : `L${Math.floor(Math.random() * 2) + 1}`
			};
		});

		// Get current week from the database
		let currentWeek = 1;
		try {
			const weekResult = await db.execute(`
				SELECT MAX(week_number) as current_week 
				FROM edw.dim_week dw
				JOIN edw.fact_team_performance ftp ON dw.week_key = ftp.week_key
				JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
				WHERE dl.season_year = ${requestedSeason}
			`);
			if (weekResult && weekResult.length > 0) {
				currentWeek = (weekResult[0] as any).current_week || 1;
			}
		} catch (error) {
			console.warn('Could not determine current week, using default:', error);
		}

		return json({
			standings: standingsWithStats,
			season: standings.length > 0 ? standings[0].seasonYear : requestedSeason,
			currentWeek,
			lastUpdated: new Date().toISOString()
		});

	} catch (error) {
		console.error('Error fetching standings:', error);
		return json({ error: 'Failed to fetch standings' }, { status: 500 });
	}
}; 
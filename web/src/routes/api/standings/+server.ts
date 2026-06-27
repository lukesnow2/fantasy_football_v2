import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { vwCurrentSeasonDashboard } from '$lib/server/db/schema';
import { eq, desc, sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

// Utility to convert snake_case keys to camelCase
function toCamelCase(obj: any): any {
	if (Array.isArray(obj)) {
		return obj.map(toCamelCase);
	}
	if (obj !== null && typeof obj === 'object') {
		const newObj: any = {};
		for (const key in obj) {
			const camelKey = key.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
			newObj[camelKey] = toCamelCase(obj[key]);
		}
		return newObj;
	}
	return obj;
}

export const GET: RequestHandler = async ({ url }) => {
	try {
		const requestedSeason = url.searchParams.get('season') || '2025';
		console.log('Standings API called with season:', requestedSeason);
		
		// Simple test response first
		if (requestedSeason === 'test') {
			return json({ 
				test: true, 
				message: 'Standings API is working',
				timestamp: new Date().toISOString()
			});
		}
		
		// Test database connection
		try {
			const testQuery = `SELECT COUNT(*) as count FROM edw.vw_current_season_dashboard`;
			const testResult = await db.execute(sql.raw(testQuery));
			console.log('Database connection test result:', testResult[0]);
		} catch (dbError) {
			console.error('Database connection error:', dbError);
			return json({ error: 'Database connection failed' }, { status: 500 });
		}
		
		// Get current season standings from the dashboard view
		// Note: This view is hardcoded to current year and current week
		console.log('Fetching standings from dashboard view...');
		let rawStandings;
		try {
			rawStandings = await db
				.select()
				.from(vwCurrentSeasonDashboard)
				.orderBy(vwCurrentSeasonDashboard.seasonRank);
			console.log('Raw standings count:', rawStandings.length);
		} catch (standingsError) {
			console.error('Error fetching standings:', standingsError);
			return json({ error: 'Failed to fetch standings data' }, { status: 500 });
		}

		// Aggregate data by team since view may return multiple rows per team (one per week)
		const teamStandings = new Map();
		
		rawStandings.forEach((row: any) => {
			const teamId = `${row.teamName}_${row.managerName}`; // Use combination as unique key
			
			if (teamStandings.has(teamId)) {
				// Aggregate stats for existing team
				const existing = teamStandings.get(teamId);
				existing.wins += (row.wins || 0);
				existing.losses += (row.losses || 0);
				existing.ties += (row.ties || 0);
				existing.pointsFor += parseFloat(row.pointsFor?.toString() || '0');
				existing.pointsAgainst += parseFloat(row.pointsAgainst?.toString() || '0');
			} else {
				// First occurrence of this team
				teamStandings.set(teamId, {
					teamName: row.teamName,
					managerName: row.managerName,
					wins: row.wins || 0,
					losses: row.losses || 0,
					ties: row.ties || 0,
					pointsFor: parseFloat(row.pointsFor?.toString() || '0'),
					pointsAgainst: parseFloat(row.pointsAgainst?.toString() || '0'),
					playoffSeed: row.playoffSeed,
					leagueName: row.leagueName,
					isPlayoffTeam: row.isPlayoffTeam,
					playoffProbability: row.playoffProbability,
					seasonYear: row.seasonYear
				});
			}
		});

		// Convert to array and calculate derived stats
		const standings = Array.from(teamStandings.values()).map(team => {
			const totalGames = team.wins + team.losses + team.ties;
			const winPercentage = totalGames > 0 ? (team.wins + 0.5 * team.ties) / totalGames : 0;
			const pointDifferential = team.pointsFor - team.pointsAgainst;
			
			return {
				...team,
				winPercentage,
				pointDifferential
			};
		});
		
		// Sort standings by wins first, then by point differential for regular season
		standings.sort((a, b) => {
			if (a.wins !== b.wins) return b.wins - a.wins;
			if (a.losses !== b.losses) return a.losses - b.losses; // Fewer losses is better
			return b.pointDifferential - a.pointDifferential;
		});
		
		// Add season rank based on sorted order
		standings.forEach((team, index) => {
			team.seasonRank = index + 1;
		});

		// Check if season is complete (championship played) and get final playoff standings
		const championshipQuery = `
			SELECT 
				fm.winner_team_key as champion_team_key,
				CASE 
					WHEN fm.team1_key = fm.winner_team_key THEN fm.team2_key
					ELSE fm.team1_key
				END as runner_up_team_key,
				fm.season_year
			FROM edw.fact_matchup fm
			JOIN edw.dim_league dl ON fm.league_key = dl.league_key
			WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
			  AND fm.is_championship = true
			LIMIT 1
		`;
		
		console.log('Executing championship query...');
		const championshipResult = await db.execute(sql.raw(championshipQuery));
		const championshipData = championshipResult.length > 0 ? championshipResult[0] : null;
		console.log('Championship result:', championshipData);
		console.log('Championship query found games:', championshipResult.length);
		
		// Also check if there are any championship games at all
		const allChampionshipQuery = `
			SELECT COUNT(*) as count 
			FROM edw.fact_matchup fm
			JOIN edw.dim_league dl ON fm.league_key = dl.league_key
			WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
			  AND fm.is_championship = true
		`;
		const allChampionshipResult = await db.execute(sql.raw(allChampionshipQuery));
		console.log('Total championship games in database:', allChampionshipResult[0]);

		// Get last place game loser information
		let lastPlaceGameLoser = null;
		if (championshipData) {
			const lastPlaceQuery = `
				SELECT 
					CASE 
						WHEN fm.team1_key = fm.winner_team_key THEN fm.team2_key
						ELSE fm.team1_key
					END as loser_team_key,
					dt_loser.team_name as loser_team_name,
					dt_loser.manager_name as loser_manager_name
				FROM edw.fact_matchup fm
				JOIN edw.dim_league dl ON fm.league_key = dl.league_key
				JOIN edw.dim_team dt_loser ON (
					CASE 
						WHEN fm.team1_key = fm.winner_team_key THEN fm.team2_key
						ELSE fm.team1_key
					END
				) = dt_loser.team_key
				WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
				  AND fm.is_last_place_game = true
				LIMIT 1
			`;
			
			const lastPlaceResult = await db.execute(sql.raw(lastPlaceQuery));
			if (lastPlaceResult.length > 0) {
				const loserData = lastPlaceResult[0] as any;
				lastPlaceGameLoser = {
					teamKey: loserData.loserTeamKey,
					teamName: loserData.loserTeamName,
					managerName: loserData.loserManagerName
				};
			}
		}

		// Debug: Check what playoff games exist (moved outside championshipData check)
		console.log('=== DEBUGGING PLAYOFF QUERIES ===');
		console.log('Championship data found:', !!championshipData);
		console.log('Checking playoff games for season 2024...');
		
		// Simple test query to see if we can find any playoff games
		const testQuery = `SELECT COUNT(*) as count FROM edw.fact_matchup fm JOIN edw.dim_league dl ON fm.league_key = dl.league_key WHERE dl.season_year = 2024 AND fm.is_playoffs = true`;
		const testResult = await db.execute(sql.raw(testQuery));
		console.log('Test playoff count:', testResult[0]);
		
		// Test the exact playoff query that's failing
		const exactTestQuery = `SELECT COUNT(*) as count FROM edw.fact_matchup fm JOIN edw.dim_league dl ON fm.league_key = dl.league_key JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft) AND fm.is_playoffs = true AND fm.is_consolation = false`;
		const exactTestResult = await db.execute(sql.raw(exactTestQuery));
		console.log('Exact playoff query count:', exactTestResult[0]);
		
		// Also test the championship query to see what season it finds
		const championshipTestQuery = `SELECT MAX(season_year) as max_season FROM edw.fact_draft`;
		const championshipTestResult = await db.execute(sql.raw(championshipTestQuery));
		console.log('Championship test - max season:', championshipTestResult[0]);
		console.log('=== END DEBUGGING ===');

		let finalStandings = standings;
		let playoffGames: any[] = [];
		let playoffStandings = new Map();
		let consolationGames: any[] = [];

		if (championshipData) {
			// Season is complete - build final standings based on playoff results
			console.log('Championship completed, building final playoff standings...');
			console.log('Original standings count:', standings.length);
			console.log('Championship data found, entering playoff logic');
			
			// Get playoff bracket results - simplified query
			const playoffQuery = `
				SELECT 
					fm.team1_key,
					fm.team2_key,
					fm.winner_team_key,
					fm.is_championship,
					fm.is_semifinal,
					fm.is_quarterfinal,
					dt1.team_name as team1_name,
					dt1.manager_name as team1_manager,
					dt2.team_name as team2_name,
					dt2.manager_name as team2_manager
				FROM edw.fact_matchup fm
				JOIN edw.dim_league dl ON fm.league_key = dl.league_key
				JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key
				JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key
				WHERE dl.season_year = 2024
				  AND fm.is_playoffs = true
				ORDER BY fm.is_championship DESC, fm.is_semifinal DESC, fm.is_quarterfinal DESC
			`;
			
			// Get consolation bracket results for the 4 non-playoff teams
			const consolationQuery = `
				SELECT 
					fm.team1_key,
					fm.team2_key,
					fm.winner_team_key,
					fm.team1_points,
					fm.team2_points,
					fm.week_key,
					fm.is_last_place_game,
					fm.is_consolation,
					dt1.team_name as team1_name,
					dt1.manager_name as team1_manager,
					dt2.team_name as team2_name,
					dt2.manager_name as team2_manager
				FROM edw.fact_matchup fm
				JOIN edw.dim_league dl ON fm.league_key = dl.league_key
				JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key
				JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key
				WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
				  AND fm.is_consolation = true
				ORDER BY fm.week_key DESC
			`;
			
			console.log('Executing playoff query...');
			console.log('Playoff query SQL:', playoffQuery);
			try {
				const playoffResult = await db.execute(sql.raw(playoffQuery));
				playoffGames = Array.from(playoffResult);
				console.log('Playoff games found:', playoffGames.length);
				console.log('Raw playoff result:', playoffResult);
				console.log('First playoff game:', playoffGames[0]);
				console.log('All playoff games:', playoffGames);
			} catch (error) {
				console.error('Error executing playoff query:', error);
				playoffGames = [];
			}
			
			// Get consolation bracket data
			let consolationGames: any[] = [];
			try {
				console.log('=== EXECUTING CONSOLATION QUERY ===');
				console.log('Consolation query SQL:', consolationQuery);
				
				// First, let's debug what's in the fact_matchup table
				console.log('=== DEBUGGING CONSOLATION QUERY ===');
				const debugQuery = `
					SELECT 
						fm.team1_key,
						fm.team2_key,
						fm.winner_team_key,
						fm.is_consolation,
						fm.is_playoffs,
						fm.is_last_place_game,
						fm.week_key,
						dt1.team_name as team1_name,
						dt2.team_name as team2_name
					FROM edw.fact_matchup fm
					JOIN edw.dim_league dl ON fm.league_key = dl.league_key
					JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key
					JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key
					WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
					  AND fm.week_key >= 14
					ORDER BY fm.week_key DESC
				`;
				
				// Also check all games to see what flags exist
				const allGamesQuery = `
					SELECT 
						fm.team1_key,
						fm.team2_key,
						fm.winner_team_key,
						fm.is_consolation,
						fm.is_playoffs,
						fm.is_last_place_game,
						fm.week_key,
						dt1.team_name as team1_name,
						dt2.team_name as team2_name
					FROM edw.fact_matchup fm
					JOIN edw.dim_league dl ON fm.league_key = dl.league_key
					JOIN edw.dim_team dt1 ON fm.team1_key = dt1.team_key
					JOIN edw.dim_team dt2 ON fm.team2_key = dt2.team_key
					WHERE dl.season_year = (SELECT MAX(season_year) FROM edw.fact_draft)
					ORDER BY fm.week_key DESC
				`;
				
				const debugResult = await db.execute(sql.raw(debugQuery));
				const debugGames = Array.from(debugResult);
				console.log('All playoff/consolation games (week 14+):', debugGames.length);
				console.log('Games with is_consolation flag:', debugGames.filter((g: any) => g.is_consolation));
				console.log('Games with is_playoffs flag:', debugGames.filter((g: any) => g.is_playoffs));
				console.log('All games:', debugGames.map((g: any) => ({
					week: g.week_key,
					team1: g.team1_name,
					team2: g.team2_name,
					is_consolation: g.is_consolation,
					is_playoffs: g.is_playoffs,
					is_last_place_game: g.is_last_place_game,
					winner: g.winner_team_key
				})));
				
				// Check all games to see what flags exist
				const allGamesResult = await db.execute(sql.raw(allGamesQuery));
				const allGames = Array.from(allGamesResult);
				console.log('All games in season 2024:', allGames.length);
				console.log('Games with is_consolation = true:', allGames.filter((g: any) => g.is_consolation === true));
				console.log('Games with is_consolation = false:', allGames.filter((g: any) => g.is_consolation === false));
				console.log('Games with is_consolation = null:', allGames.filter((g: any) => g.is_consolation === null));
				console.log('Sample of all games:', allGames.slice(0, 5).map((g: any) => ({
					week: g.week_number,
					team1: g.team1_name,
					team2: g.team2_name,
					is_consolation: g.is_consolation,
					is_playoffs: g.is_playoffs,
					is_last_place_game: g.is_last_place_game
				})));
				
				// Now try the actual consolation query
				console.log('About to execute consolation query...');
				const consolationResult = await db.execute(sql.raw(consolationQuery));
				console.log('Consolation query executed, raw result:', consolationResult);
				consolationGames = Array.from(consolationResult);
				console.log('Consolation games found:', consolationGames.length);
				console.log('Consolation games:', consolationGames);
				
				// After fetching consolationGames, convert to camelCase
				consolationGames = toCamelCase(Array.from(consolationResult));
				
				// Debug: Check the exact field names in the first consolation game
				if (consolationGames.length > 0) {
					console.log('First consolation game field names:', Object.keys(consolationGames[0]));
					console.log('First consolation game data:', consolationGames[0]);
				}
				console.log('=== END DEBUGGING CONSOLATION QUERY ===');
			} catch (error) {
				console.error('Error executing consolation query:', error);
				consolationGames = [];
			}
			
			// Build final standings based on playoff results
			playoffStandings.clear();
			
			// 1st and 2nd place from championship
			const championshipGame = playoffGames.find(game => game.isChampionship);
			console.log('=== CHAMPIONSHIP GAME DEBUG ===');
			console.log('All playoff games:', playoffGames.map(g => ({ 
				team1: g.team1Name, 
				team2: g.team2Name, 
				is_championship: g.isChampionship,
				winner_team_key: g.winnerTeamKey,
				team1_key: g.team1Key,
				team2_key: g.team2Key
			})));
			console.log('Championship game found:', championshipGame);
			if (championshipGame) {
				console.log('Found championship game:', championshipGame);
				console.log('Winner team key:', championshipGame.winnerTeamKey);
				console.log('Team1 key:', championshipGame.team1Key);
				console.log('Team2 key:', championshipGame.team2Key);
				playoffStandings.set(championshipGame.winnerTeamKey, { rank: 1, tier: 'Champion' });
				const runnerUpKey = championshipGame.team1Key === championshipGame.winnerTeamKey 
					? championshipGame.team2Key : championshipGame.team1Key;
				playoffStandings.set(runnerUpKey, { rank: 2, tier: 'Runner-up' });
				console.log('Set champion and runner-up:', { champion: championshipGame.winnerTeamKey, runnerUp: runnerUpKey });
				console.log('Playoff standings map after championship:', Array.from(playoffStandings.entries()));
			} else {
				console.log('No championship game found in playoff games');
			}
			console.log('=== END CHAMPIONSHIP GAME DEBUG ===');
			
			// 3rd and 4th place from semifinals losers
			const semifinalGames = playoffGames.filter(game => game.isSemifinal);
			let rank = 3;
			for (const game of semifinalGames) {
				if (game.winnerTeamKey) {
					const loserKey = game.team1Key === game.winnerTeamKey 
						? game.team2Key : game.team1Key;
					if (!playoffStandings.has(loserKey)) {
						playoffStandings.set(loserKey, { rank, tier: 'Semifinalist' });
						rank++;
					}
				}
			}
			
			// 5th and 6th place from quarterfinals losers
			const quarterfinalGames = playoffGames.filter(game => game.isQuarterfinal);
			for (const game of quarterfinalGames) {
				if (game.winnerTeamKey) {
					const loserKey = game.team1Key === game.winnerTeamKey 
						? game.team2Key : game.team1Key;
					if (!playoffStandings.has(loserKey)) {
						playoffStandings.set(loserKey, { rank, tier: 'Quarterfinalist' });
						rank++;
					}
				}
			}
			
			// Build consolation standings for the 4 non-playoff teams (ranks 7-10)
			const consolationStandings = new Map();
			if (consolationGames.length > 0) {
				console.log('=== CONSOLATION BRACKET ANALYSIS ===');
				console.log('Found consolation games:', consolationGames.length);
				console.log('Consolation games raw data:', consolationGames);
				console.log('Consolation games:', consolationGames.map((g: any) => ({
					week: g.weekKey,
					team1: g.team1Name,
					team2: g.team2Name,
					winner: g.winnerTeamKey ? (g.team1Key === g.winnerTeamKey ? g.team1Name : g.team2Name) : 'Tie',
					is_last_place_game: g.isLastPlaceGame
				})));
				
				// Find the last place game (should be the game with is_last_place_game = true)
				const lastPlaceGame = consolationGames.find((g: any) => g.isLastPlaceGame);
				console.log('Last place game:', lastPlaceGame);
				
				if (lastPlaceGame) {
					// The loser of the last place game gets rank 10
					const lastPlaceLoserKey = lastPlaceGame.team1Key === lastPlaceGame.winnerTeamKey 
						? lastPlaceGame.team2Key : lastPlaceGame.team1Key;
					const lastPlaceWinnerKey = lastPlaceGame.winnerTeamKey;
					
					console.log('Last place winner:', lastPlaceWinnerKey);
					console.log('Last place loser:', lastPlaceLoserKey);
					
					// Assign rank 10 to the last place loser
					consolationStandings.set(lastPlaceLoserKey, { rank: 10, tier: 'Consolation 4th (Last Place)' });
					
					// The winner of the last place game gets rank 9
					consolationStandings.set(lastPlaceWinnerKey, { rank: 9, tier: 'Consolation 3rd' });
					
					// Find the consolation championship game (should be the other game in the final week)
					const finalWeek = Math.max(...consolationGames.map((g: any) => g.weekKey));
					const finalWeekGames = consolationGames.filter((g: any) => g.weekKey === finalWeek);
					const consolationChampionshipGame = finalWeekGames.find((g: any) => !g.isLastPlaceGame);
					
					if (consolationChampionshipGame) {
						const consolationWinnerKey = consolationChampionshipGame.winnerTeamKey;
						const consolationLoserKey = consolationChampionshipGame.team1Key === consolationWinnerKey 
							? consolationChampionshipGame.team2Key : consolationChampionshipGame.team1Key;
						
						console.log('Consolation winner:', consolationWinnerKey);
						console.log('Consolation runner-up:', consolationLoserKey);
						
						// Assign ranks 7-8 based on consolation championship
						consolationStandings.set(consolationWinnerKey, { rank: 7, tier: 'Consolation Winner' });
						consolationStandings.set(consolationLoserKey, { rank: 8, tier: 'Consolation Runner-up' });
					}
				}
				
				console.log('Final consolation standings:', Array.from(consolationStandings.entries()));
				console.log('=== END CONSOLATION BRACKET ANALYSIS ===');
			}
			
			// Apply playoff rankings to standings
			console.log('Playoff games found:', playoffGames.length);
			console.log('Playoff standings map:', Array.from(playoffStandings.entries()));
			console.log('Consolation standings map:', Array.from(consolationStandings.entries()));
			console.log('Available teams in standings:', standings.map(s => `${s.teamName} (${s.managerName})`));
			console.log('Teams in playoff games:', playoffGames.map(g => `${g.team1_name} (${g.team1_manager}) vs ${g.team2_name} (${g.team2_manager})`));
			
			// Debug: Check what's in the playoff standings map
			console.log('=== PLAYOFF STANDINGS MAP DEBUG ===');
			console.log('Playoff standings map size:', playoffStandings.size);
			playoffStandings.forEach((rank, key) => {
				console.log(`Key: ${key}, Rank: ${rank.rank}, Tier: ${rank.tier}`);
			});
			console.log('=== END PLAYOFF STANDINGS MAP DEBUG ===');
			
			finalStandings = standings.map((team, index) => {
				// Find team key by matching name and manager
				let teamKey = null;
				console.log(`\n--- Matching team: "${team.teamName}" (${team.managerName}) ---`);
				
				// First try to find in playoff games
				for (const game of playoffGames) {
					console.log(`Checking playoff game: "${game.team1Name}" (${game.team1Manager}) vs "${game.team2Name}" (${game.team2Manager})`);
					console.log(`Game object keys:`, Object.keys(game));
					console.log(`Game raw data:`, game);
					
					// Normalize strings for comparison (trim whitespace, lowercase)
					const normalizeStr = (str: any) => String(str || '').trim().toLowerCase();
					
					const team1NameMatch = normalizeStr(game.team1Name) === normalizeStr(team.teamName);
					const team1ManagerMatch = normalizeStr(game.team1Manager) === normalizeStr(team.managerName);
					const team2NameMatch = normalizeStr(game.team2Name) === normalizeStr(team.teamName);
					const team2ManagerMatch = normalizeStr(game.team2Manager) === normalizeStr(team.managerName);
					
					const team1Match = team1NameMatch && team1ManagerMatch;
					const team2Match = team2NameMatch && team2ManagerMatch;
					
					console.log(`Team1 match: ${team1Match} (name: ${team1NameMatch}, manager: ${team1ManagerMatch})`);
					console.log(`Team2 match: ${team2Match} (name: ${team2NameMatch}, manager: ${team2ManagerMatch})`);
					
					if (team1Match || team2Match) {
						teamKey = team1Match ? game.team1Key : game.team2Key;
						console.log(`✅ Found team key ${teamKey} for ${team.teamName} (${team.managerName})`);
						break;
					}
				}
				
				// If not found in playoff games, try consolation games
				if (!teamKey) {
					console.log(`Team not found in playoff games, checking consolation games...`);
					for (const game of consolationGames) {
						console.log(`Checking consolation game: "${game.team1Name}" (${game.team1Manager}) vs "${game.team2Name}" (${game.team2Manager})`);
						
						// Normalize strings for comparison (trim whitespace, lowercase)
						const normalizeStr = (str: any) => String(str || '').trim().toLowerCase();
						
						const team1NameMatch = normalizeStr(game.team1Name) === normalizeStr(team.teamName);
						const team1ManagerMatch = normalizeStr(game.team1Manager) === normalizeStr(team.managerName);
						const team2NameMatch = normalizeStr(game.team2Name) === normalizeStr(team.teamName);
						const team2ManagerMatch = normalizeStr(game.team2Manager) === normalizeStr(team.managerName);
						
						const team1Match = team1NameMatch && team1ManagerMatch;
						const team2Match = team2NameMatch && team2ManagerMatch;
						
						console.log(`Consolation Team1 match: ${team1Match} (name: ${team1NameMatch}, manager: ${team1ManagerMatch})`);
						console.log(`Consolation Team2 match: ${team2Match} (name: ${team2NameMatch}, manager: ${team2ManagerMatch})`);
						
						if (team1Match || team2Match) {
							teamKey = team1Match ? game.team1Key : game.team2Key;
							console.log(`✅ Found consolation team key ${teamKey} for ${team.teamName} (${team.managerName})`);
							break;
						}
					}
				}
				
				if (!teamKey) {
					console.log(`❌ No team key found for ${team.teamName} (${team.managerName})`);
					console.log(`Available playoff teams:`);
					playoffGames.forEach(game => {
						console.log(`  - "${game.team1Name}" (${game.team1Manager})`);
						console.log(`  - "${game.team2Name}" (${game.team2Manager})`);
					});
					console.log(`Available consolation teams:`);
					consolationGames.forEach(game => {
						console.log(`  - "${game.team1Name}" (${game.team1Manager})`);
						console.log(`  - "${game.team2Name}" (${game.team2Manager})`);
					});
				}
				
				const playoffRank = playoffStandings.get(teamKey);
				const consolationRank = consolationStandings.get(teamKey);
				console.log(`Playoff standings map has ${playoffStandings.size} entries:`);
				playoffStandings.forEach((rank, key) => {
					console.log(`  Key: ${key}, Rank: ${rank.rank}, Tier: ${rank.tier}`);
				});
				console.log(`Consolation standings map has ${consolationStandings.size} entries:`);
				consolationStandings.forEach((rank, key) => {
					console.log(`  Key: ${key}, Rank: ${rank.rank}, Tier: ${rank.tier}`);
				});
				
				if (playoffRank) {
					console.log(`✅ Team ${team.teamName} (${team.managerName}) gets playoff rank ${playoffRank.rank} (${playoffRank.tier})`);
					return {
						...team,
						finalRank: playoffRank.rank,
						playoffTier: playoffRank.tier
					};
				} else if (consolationRank) {
					console.log(`✅ Team ${team.teamName} (${team.managerName}) gets consolation rank ${consolationRank.rank} (${consolationRank.tier})`);
					return {
						...team,
						finalRank: consolationRank.rank,
						playoffTier: consolationRank.tier
					};
				} else {
					// Non-playoff team - rank by regular season record
					console.log(`❌ Team ${team.teamName} (${team.managerName}) gets regular season rank (no playoff/consolation rank found)`);
					return {
						...team,
						finalRank: team.seasonRank || (index + 1),
						playoffTier: 'Regular Season'
					};
				}
			});
			
			console.log('Final standings count:', finalStandings.length);
			console.log('Final standings:', finalStandings.map(s => `${s.finalRank}: ${s.teamName} (${s.managerName})`));
			
			// Sort by final playoff rank, then by regular season record for non-playoff teams
			finalStandings.sort((a, b) => {
				if (a.finalRank && b.finalRank) return a.finalRank - b.finalRank;
				if (a.finalRank && !b.finalRank) return -1;
				if (!a.finalRank && b.finalRank) return 1;
				// Both non-playoff - sort by regular season
				if (a.wins !== b.wins) return b.wins - a.wins;
				return b.pointDifferential - a.pointDifferential;
			});
		} else {
			// Regular season still ongoing - standings are already sorted above
			console.log('No championship data found - using regular season standings');
			finalStandings = standings;
		}

		// Format data for frontend
		const standingsWithStats = finalStandings.map((team, index) => {
			const winPercentage = team.winPercentage ? 
				(parseFloat(team.winPercentage.toString()) * 100).toFixed(1) : '0.0';
			const pointDifferential = team.pointDifferential ? 
				parseFloat(team.pointDifferential.toString()).toFixed(1) : '0.0';
			
			// Check if this team is the last place game loser
			const isLastPlaceLoser = lastPlaceGameLoser && 
				team.teamName === lastPlaceGameLoser.teamName && 
				team.managerName === lastPlaceGameLoser.managerName;
			
			// Determine the correct rank
			const rank = team.finalRank || team.seasonRank || (index + 1);
			
			return {
				teamId: rank, // Use the correct rank
				teamName: team.teamName,
				managerName: team.managerName,
				wins: team.wins || 0,
				losses: team.losses || 0,
				ties: team.ties || 0,
				pointsFor: team.pointsFor ? parseFloat(team.pointsFor.toString()) : 0,
				pointsAgainst: team.pointsAgainst ? parseFloat(team.pointsAgainst.toString()) : 0,
				playoffSeed: team.playoffSeed,
				rank: rank, // Use the correct rank
				winPercentage,
				pointDifferential,
				leagueName: team.leagueName,
				isPlayoffTeam: team.isPlayoffTeam,
				playoffProbability: team.playoffProbability ? 
					(parseFloat(team.playoffProbability.toString()) * 100).toFixed(1) : '0.0',
				playoffTier: team.playoffTier || null, // Championship tier (Champion, Runner-up, etc.)
				isFinalRank: !!team.finalRank, // Whether this is final playoff ranking
				isLastPlaceLoser: isLastPlaceLoser, // Whether this team lost the last place game
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

		const responseData = {
			standings: standingsWithStats,
			season: finalStandings.length > 0 ? finalStandings[0].seasonYear : requestedSeason,
			currentWeek,
			isSeasonComplete: !!championshipData,
			isFinalStandings: !!championshipData,
			lastPlaceGameLoser, // Include the last place game loser info
			lastUpdated: new Date().toISOString(),
			// Debug info for frontend
			debug: {
				championshipDataFound: !!championshipData,
				playoffGamesCount: playoffGames?.length || 0,
				playoffStandingsMapSize: playoffStandings?.size || 0,
				teamsWithPlayoffTiers: standingsWithStats.filter(s => s.playoffTier && s.playoffTier !== 'Regular Season').length,
				// Additional debugging
				playoffGames: playoffGames?.map(g => {
					console.log('Mapping playoff game:', g);
					console.log('Game keys:', Object.keys(g));
					return {
						team1: g.team1Name,
						team2: g.team2Name,
						is_championship: g.isChampionship,
						winner_team_key: g.winnerTeamKey,
						team1_key: g.team1Key,
						team2_key: g.team2Key,
						raw_game: g, // Include raw game data for debugging
						game_keys: Object.keys(g), // Include keys for debugging
						team1_name_prop: g.team1Name,
						team1_manager_prop: g.team1Manager,
						team2_name_prop: g.team2Name,
						team2_manager_prop: g.team2Manager
					};
				}) || [],
				championshipGameFound: !!playoffGames?.find(g => g.isChampionship),
				playoffStandingsEntries: Array.from(playoffStandings?.entries() || []),
				consolationGames: consolationGames?.map((g: any) => ({
					team1: g.team1Name,
					team2: g.team2Name,
					winner_team_key: g.winnerTeamKey,
					team1_key: g.team1Key,
					team2_key: g.team2Key,
					raw_game: g, // Include raw game data for debugging
					game_keys: Object.keys(g), // Include keys for debugging
					team1_name_prop: g.team1Name,
					team1_manager_prop: g.team1Manager,
					team2_name_prop: g.team2Name,
					team2_manager_prop: g.team2Manager
				})) || []
			}
		};
		
		console.log('Returning standings response:', {
			standingsCount: standingsWithStats.length,
			isSeasonComplete: !!championshipData,
			isFinalStandings: !!championshipData,
			firstFewStandings: standingsWithStats.slice(0, 3).map(s => `${s.rank}: ${s.teamName} (${s.managerName})`),
			allRanks: standingsWithStats.map(s => s.rank).sort((a, b) => a - b)
		});
		
		return json(responseData);

	} catch (error) {
		console.error('Error fetching standings:', error);
		return json({ error: 'Failed to fetch standings' }, { status: 500 });
	}
}; 
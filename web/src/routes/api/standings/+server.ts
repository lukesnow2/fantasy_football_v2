import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

// Standings for a single season. Resolves ONE target season (the validated `season`
// query param, else the current/most-recent season) and uses it for every query —
// season totals, championship detection, and the playoff/consolation bracket. Teams
// are matched to bracket games by team_key (not by name), so it can't silently drop
// the top half of the table when flags or names differ.
export const GET: RequestHandler = async ({ url }) => {
	try {
		// 1. Resolve the target season.
		const maxRow = await db.execute(sql`SELECT MAX(season_year) AS season FROM edw.dim_league`);
		const currentSeason = Number((maxRow[0] as any)?.season) || new Date().getFullYear();

		const seasonParam = url.searchParams.get('season');
		let targetSeason = currentSeason;
		if (seasonParam !== null) {
			const parsed = Number(seasonParam);
			if (!Number.isInteger(parsed) || parsed < 2005 || parsed > currentSeason + 1) {
				return json({ error: 'Invalid season parameter' }, { status: 400 });
			}
			targetSeason = parsed;
		}

		// 2. Season totals per team (fact_team_performance stores per-week rows; sum them).
		const standingsRows = await db.execute(sql`
			SELECT
				dt.team_key                  AS team_key,
				dt.team_name                 AS team_name,
				dt.manager_name              AS manager_name,
				dl.league_name               AS league_name,
				SUM(ftp.wins)                AS wins,
				SUM(ftp.losses)              AS losses,
				SUM(ftp.ties)                AS ties,
				SUM(ftp.points_for)          AS points_for,
				SUM(ftp.points_against)      AS points_against,
				bool_or(ftp.is_playoff_team) AS is_playoff_team
			FROM edw.fact_team_performance ftp
			JOIN edw.dim_team dt   ON ftp.team_key = dt.team_key
			JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
			WHERE ftp.season_year = ${targetSeason}
			GROUP BY dt.team_key, dt.team_name, dt.manager_name, dl.league_name
		`);

		const standings = standingsRows.map((r: any) => {
			const wins = Number(r.wins) || 0;
			const losses = Number(r.losses) || 0;
			const ties = Number(r.ties) || 0;
			const pointsFor = parseFloat(r.pointsFor ?? '0') || 0;
			const pointsAgainst = parseFloat(r.pointsAgainst ?? '0') || 0;
			const totalGames = wins + losses + ties;
			return {
				teamKey: Number(r.teamKey),
				teamName: r.teamName,
				managerName: r.managerName,
				leagueName: r.leagueName,
				wins,
				losses,
				ties,
				pointsFor,
				pointsAgainst,
				playoffSeed: null as number | null,
				isPlayoffTeam: r.isPlayoffTeam,
				winPercentage: totalGames > 0 ? (wins + 0.5 * ties) / totalGames : 0,
				pointDifferential: pointsFor - pointsAgainst,
				seasonRank: 0
			};
		});

		// Regular-season ordering (used as-is for in-progress seasons, and as the
		// fallback rank for any team not placed in a bracket).
		standings.sort(
			(a, b) => b.wins - a.wins || a.losses - b.losses || b.pointDifferential - a.pointDifferential
		);
		standings.forEach((t, i) => (t.seasonRank = i + 1));

		if (standings.length === 0) {
			return json({
				standings: [],
				season: targetSeason,
				currentWeek: 0,
				isSeasonComplete: false,
				isFinalStandings: false,
				lastPlaceGameLoser: null,
				lastUpdated: new Date().toISOString()
			});
		}

		// 3. Is the season complete? (a championship game has been played)
		const champRows = await db.execute(sql`
			SELECT 1
			FROM edw.fact_matchup fm
			JOIN edw.dim_league dl ON fm.league_key = dl.league_key
			WHERE dl.season_year = ${targetSeason} AND fm.is_championship = true
			LIMIT 1
		`);
		const seasonComplete = champRows.length > 0;

		// 4. Build final placements from the playoff + consolation brackets (keyed by team_key).
		const placements = new Map<number, { rank: number; tier: string }>();
		let lastPlaceGameLoser: { teamKey: number; teamName?: string; managerName?: string } | null = null;

		if (seasonComplete) {
			const playoffGames = await db.execute(sql`
				SELECT team1_key, team2_key, winner_team_key, is_championship, is_semifinal, is_quarterfinal
				FROM edw.fact_matchup fm
				JOIN edw.dim_league dl ON fm.league_key = dl.league_key
				WHERE dl.season_year = ${targetSeason} AND fm.is_playoffs = true
			`);
			const consolationGames = await db.execute(sql`
				SELECT team1_key, team2_key, winner_team_key, week_key, is_last_place_game
				FROM edw.fact_matchup fm
				JOIN edw.dim_league dl ON fm.league_key = dl.league_key
				WHERE dl.season_year = ${targetSeason} AND fm.is_consolation = true
				ORDER BY week_key DESC
			`);

			const loserOf = (g: any) =>
				Number(g.team1Key) === Number(g.winnerTeamKey) ? Number(g.team2Key) : Number(g.team1Key);

			// Champion (1) & runner-up (2)
			const champ = playoffGames.find((g: any) => g.isChampionship);
			if (champ?.winnerTeamKey) {
				placements.set(Number(champ.winnerTeamKey), { rank: 1, tier: 'Champion' });
				placements.set(loserOf(champ), { rank: 2, tier: 'Runner-up' });
			}
			// Semifinal losers (3-4)
			let rank = 3;
			for (const g of playoffGames.filter((g: any) => g.isSemifinal && g.winnerTeamKey)) {
				const loser = loserOf(g);
				if (!placements.has(loser)) placements.set(loser, { rank: rank++, tier: 'Semifinalist' });
			}
			// Quarterfinal losers (5-6)
			for (const g of playoffGames.filter((g: any) => g.isQuarterfinal && g.winnerTeamKey)) {
				const loser = loserOf(g);
				if (!placements.has(loser)) placements.set(loser, { rank: rank++, tier: 'Quarterfinalist' });
			}
			// Consolation bracket (7-10)
			const lastPlace = consolationGames.find((g: any) => g.isLastPlaceGame);
			if (lastPlace?.winnerTeamKey) {
				const loser = loserOf(lastPlace);
				placements.set(loser, { rank: 10, tier: 'Consolation 4th (Last Place)' });
				placements.set(Number(lastPlace.winnerTeamKey), { rank: 9, tier: 'Consolation 3rd' });
				lastPlaceGameLoser = { teamKey: loser };

				const finalWeek = Math.max(...consolationGames.map((g: any) => Number(g.weekKey)));
				const consChamp = consolationGames.find(
					(g: any) => Number(g.weekKey) === finalWeek && !g.isLastPlaceGame
				);
				if (consChamp?.winnerTeamKey) {
					placements.set(Number(consChamp.winnerTeamKey), { rank: 7, tier: 'Consolation Winner' });
					placements.set(loserOf(consChamp), { rank: 8, tier: 'Consolation Runner-up' });
				}
			}
		}

		// 5. Final standings: a bracket placement if there is one, else the season rank.
		const finalStandings = standings.map((t) => {
			const placed = placements.get(t.teamKey);
			return {
				...t,
				finalRank: placed ? placed.rank : seasonComplete ? t.seasonRank : null,
				playoffTier: placed ? placed.tier : seasonComplete ? 'Regular Season' : null,
				inBracket: !!placed
			};
		});
		if (seasonComplete) {
			finalStandings.sort(
				(a, b) =>
					(a.finalRank ?? 99) - (b.finalRank ?? 99) ||
					b.wins - a.wins ||
					b.pointDifferential - a.pointDifferential
			);
		}

		// 6. Current week for the target season.
		const weekRows = await db.execute(sql`
			SELECT MAX(dw.week_number) AS current_week
			FROM edw.fact_team_performance ftp
			JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
			WHERE ftp.season_year = ${targetSeason}
		`);
		const currentWeek = Number((weekRows[0] as any)?.currentWeek) || 1;

		if (lastPlaceGameLoser) {
			const loserTeam = standings.find((t) => t.teamKey === lastPlaceGameLoser!.teamKey);
			if (loserTeam) {
				lastPlaceGameLoser.teamName = loserTeam.teamName;
				lastPlaceGameLoser.managerName = loserTeam.managerName;
			}
		}

		// 7. Shape for the frontend.
		const standingsWithStats = finalStandings.map((t, i) => {
			const rank = t.finalRank ?? t.seasonRank ?? i + 1;
			return {
				teamId: rank,
				teamName: t.teamName,
				managerName: t.managerName,
				wins: t.wins,
				losses: t.losses,
				ties: t.ties,
				pointsFor: t.pointsFor,
				pointsAgainst: t.pointsAgainst,
				playoffSeed: t.playoffSeed,
				rank,
				winPercentage: (t.winPercentage * 100).toFixed(1),
				pointDifferential: t.pointDifferential.toFixed(1),
				leagueName: t.leagueName,
				isPlayoffTeam: t.isPlayoffTeam,
				playoffProbability: '0.0',
				playoffTier: t.playoffTier,
				isFinalRank: seasonComplete && t.inBracket,
				isLastPlaceLoser: !!(lastPlaceGameLoser && lastPlaceGameLoser.teamKey === t.teamKey),
				streak: null
			};
		});

		return json({
			standings: standingsWithStats,
			season: targetSeason,
			currentWeek,
			isSeasonComplete: seasonComplete,
			isFinalStandings: seasonComplete,
			lastPlaceGameLoser,
			lastUpdated: new Date().toISOString()
		});
	} catch (error) {
		console.error('Error fetching standings:', error);
		return json({ error: 'Failed to fetch standings' }, { status: 500 });
	}
};

import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const limit = Math.min(parseInt(url.searchParams.get('limit') || '20', 10) || 20, 100);

		// Resolve the season: validated param, else the current/most-recent season
		// (matches the standings API so the page shows the same season everywhere).
		const seasonParam = url.searchParams.get('season');
		let season: number;
		if (seasonParam !== null) {
			const parsed = Number(seasonParam);
			if (!Number.isInteger(parsed)) {
				return json({ error: 'Invalid season parameter' }, { status: 400 });
			}
			season = parsed;
		} else {
			const maxRow = await db.execute(sql`SELECT MAX(season_year) AS season FROM edw.dim_league`);
			season = Number((maxRow[0] as any)?.season) || new Date().getFullYear();
		}

		// Resolve player and the acting team/manager. The acting team is the destination
		// for adds (free-agent pickups have no from-team) and the source for drops, so
		// COALESCE(to, from) gives the right side for each transaction type.
		const rows = await db.execute(sql`
			SELECT
				ft.transaction_key                          AS transaction_key,
				ft.transaction_type                         AS transaction_type,
				ft.transaction_date                         AS transaction_date,
				ft.faab_bid                                 AS faab_bid,
				dp.player_name                              AS player_name,
				COALESCE(to_t.team_name, from_t.team_name)  AS team_name,
				COALESCE(to_m.manager_name, from_m.manager_name) AS manager_name,
				ft.season_year                              AS season_year
			FROM edw.fact_transaction ft
			LEFT JOIN edw.dim_player  dp     ON ft.player_key       = dp.player_key
			LEFT JOIN edw.dim_team    to_t   ON ft.to_team_key      = to_t.team_key
			LEFT JOIN edw.dim_team    from_t ON ft.from_team_key    = from_t.team_key
			LEFT JOIN edw.dim_manager to_m   ON ft.to_manager_key   = to_m.manager_key
			LEFT JOIN edw.dim_manager from_m ON ft.from_manager_key = from_m.manager_key
			WHERE ft.season_year = ${season}
			ORDER BY ft.transaction_date DESC
			LIMIT ${limit}
		`);

		const formattedTransactions = rows.map((tx: any) => ({
			id: tx.transactionKey,
			type: tx.transactionType,
			timestamp: tx.transactionDate,
			playerName: tx.playerName || 'Unknown Player',
			teamName: tx.teamName || 'Free Agent',
			managerName: tx.managerName || null,
			faabBid: tx.faabBid ? parseFloat(tx.faabBid.toString()) : null,
			status: 'completed',
			timeAgo: getTimeAgo(tx.transactionDate)
		}));

		return json({
			transactions: formattedTransactions,
			count: formattedTransactions.length,
			season
		});
	} catch (error) {
		console.error('Error fetching transactions:', error);
		return json({ error: 'Failed to fetch transactions' }, { status: 500 });
	}
};

function getTimeAgo(date: Date | string): string {
	const now = new Date();
	const transactionDate = new Date(date);
	const diffMs = now.getTime() - transactionDate.getTime();
	const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
	const diffDays = Math.floor(diffHours / 24);

	if (diffDays > 0) {
		return `${diffDays} day${diffDays > 1 ? 's' : ''} ago`;
	} else if (diffHours > 0) {
		return `${diffHours} hour${diffHours > 1 ? 's' : ''} ago`;
	} else {
		return 'Less than an hour ago';
	}
}

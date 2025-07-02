import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { factTransaction, dimTeam, dimManager } from '$lib/server/db/schema';
import { desc, eq, and } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	try {
		const limit = parseInt(url.searchParams.get('limit') || '20');
		const season = parseInt(url.searchParams.get('season') || '2024');

		// Get recent transactions with team and manager information
		const recentTransactions = await db
			.select({
				transactionKey: factTransaction.transactionKey,
				transactionType: factTransaction.transactionType,
				transactionDate: factTransaction.transactionDate,
				faabBid: factTransaction.faabBid,
				fromTeamName: dimTeam.teamName,
				fromManagerName: dimManager.managerName,
				seasonYear: factTransaction.seasonYear
			})
			.from(factTransaction)
			.leftJoin(dimTeam, eq(factTransaction.fromTeamKey, dimTeam.teamKey))
			.leftJoin(dimManager, eq(factTransaction.fromManagerKey, dimManager.managerKey))
			.where(and(
				eq(factTransaction.seasonYear, season),
				eq(factTransaction.isSuccessful, true)
			))
			.orderBy(desc(factTransaction.transactionDate))
			.limit(limit);

		// Format transactions for frontend
		const formattedTransactions = recentTransactions.map(tx => ({
			id: tx.transactionKey,
			type: tx.transactionType,
			timestamp: tx.transactionDate,
			playerName: 'Player', // Player info would need to be joined from dim_player
			teamName: tx.fromTeamName,
			managerName: tx.fromManagerName,
			faabBid: tx.faabBid ? parseFloat(tx.faabBid.toString()) : null,
			status: 'completed',
			timeAgo: getTimeAgo(tx.transactionDate)
		}));

		return json({
			transactions: formattedTransactions,
			count: formattedTransactions.length
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
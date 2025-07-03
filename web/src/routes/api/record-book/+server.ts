import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	try {
		const recordBookQuery = `
			SELECT 
				record_name,
				record_value,
				team_name,
				manager_name,
				season_year
			FROM edw.vw_league_record_book
			ORDER BY record_name, season_year DESC
		`;

		const recordBookResult = await db.execute(sql.raw(recordBookQuery));
		const records = Array.from(recordBookResult);

		// Categorize records for better organization
		const categorizeRecord = (recordName: string) => {
			const name = recordName.toLowerCase();
			if (name.includes('point') || name.includes('score') || name.includes('scoring')) {
				return 'Scoring Records';
			}
			if (name.includes('championship') || name.includes('title') || name.includes('playoff')) {
				return 'Championship Records';
			}
			if (name.includes('win') || name.includes('loss') || name.includes('streak')) {
				return 'Win/Loss Records';
			}
			if (name.includes('trade') || name.includes('transaction') || name.includes('waiver') || name.includes('pickup')) {
				return 'Transaction Records';
			}
			if (name.includes('draft') || name.includes('rookie')) {
				return 'Draft Records';
			}
			if (name.includes('season') || name.includes('year')) {
				return 'Season Records';
			}
			return 'Other Records';
		};

		// Format data for frontend
		const formattedRecords = records.map(record => ({
			record: record.record_name,
			value: record.record_value,
			holder: record.team_name ? `${record.manager_name} (${record.team_name})` : record.manager_name,
			year: record.season_year?.toString() || '',
			manager: record.manager_name,
			team: record.team_name,
			category: categorizeRecord(String(record.record_name || ''))
		}));

		// Group records by category
		const recordsByCategory = formattedRecords.reduce((acc, record) => {
			if (!acc[record.category]) {
				acc[record.category] = [];
			}
			acc[record.category].push(record);
			return acc;
		}, {} as Record<string, typeof formattedRecords>);

		return json({
			records: formattedRecords,
			recordsByCategory: recordsByCategory,
			categories: Object.keys(recordsByCategory),
			totalRecords: formattedRecords.length,
			lastUpdated: new Date().toISOString()
		});

	} catch (error) {
		console.error('Error fetching record book data:', error);
		return json({ error: 'Failed to fetch record book data' }, { status: 500 });
	}
}; 
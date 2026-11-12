import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { lastPipelineLoad } from '$lib/server/pipeline-load';
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

		// The ledger read never throws, so it runs alongside the records.
		const [recordBookResult, lastUpdated] = await Promise.all([
			db.execute(sql.raw(recordBookQuery)),
			lastPipelineLoad()
		]);
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

		// Format data for frontend with camelCase fallback
		const formattedRecords = records.map((record: any) => ({
			record: record.recordName || record.record_name,
			value: record.recordValue || record.record_value,
			holder: (record.teamName || record.team_name) ? `${record.managerName || record.manager_name} (${record.teamName || record.team_name})` : (record.managerName || record.manager_name),
			year: (record.seasonYear || record.season_year)?.toString() || '',
			manager: record.managerName || record.manager_name,
			team: record.teamName || record.team_name,
			category: categorizeRecord(String(record.recordName || record.record_name || ''))
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
			// The pipeline's last load, not this request: null when there's none.
			lastUpdated
		});

	} catch (error) {
		console.error('Error fetching record book data:', error);
		return json({ error: 'Failed to fetch record book data' }, { status: 500 });
	}
}; 
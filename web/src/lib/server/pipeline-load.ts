import { sql } from 'drizzle-orm';
import { db } from '$lib/server/db';

/**
 * When the pipeline last loaded data into the warehouse: the latest
 * successful run in its ledger (public.pipeline_runs, src/pipeline/state.py)
 * that loaded weeks or republished stored ones. A run that found nothing to
 * load records itself as a successful no-op, and that isn't a load.
 */
export const LAST_LOAD_SQL = `
	SELECT max(finished_at) AS last_load
	FROM public.pipeline_runs
	WHERE status = 'success'
	  AND (cardinality(weeks_loaded) > 0 OR row_counts ? 'repair')
`;

/** The ledger's timestamp as an ISO string, or null for none. Pure, for testing. */
export function toIso(value: unknown): string | null {
	if (value == null || value === '') return null;
	const d = value instanceof Date ? value : new Date(String(value));
	return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

/**
 * The last load's time, or null when the ledger has none or isn't there (a
 * warehouse the pipeline hasn't run against). Never throws: a page that shows
 * the date can still show everything else.
 */
export async function lastPipelineLoad(): Promise<string | null> {
	try {
		const rows = Array.from(await db.execute(sql.raw(LAST_LOAD_SQL))) as Record<string, unknown>[];
		const row = rows[0] ?? {};
		return toIso(row.lastLoad ?? row.last_load);
	} catch (error) {
		console.error('Error reading the pipeline run ledger:', error);
		return null;
	}
}

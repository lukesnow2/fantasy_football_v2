import { describe, expect, it, vi } from 'vitest';

vi.mock('$lib/server/db', () => ({ db: {} }));

const { LAST_LOAD_SQL, toIso } = await import('./pipeline-load');

describe('the pipeline last load', () => {
	it('counts successful runs that loaded or republished, not no-ops', () => {
		expect(LAST_LOAD_SQL).toContain("status = 'success'");
		expect(LAST_LOAD_SQL).toContain('cardinality(weeks_loaded) > 0');
		expect(LAST_LOAD_SQL).toContain("row_counts ? 'repair'");
	});

	it('reads the ledger timestamp as ISO, and nothing as null', () => {
		expect(toIso(new Date('2026-11-07T21:10:39Z'))).toBe('2026-11-07T21:10:39.000Z');
		expect(toIso('2026-11-07 14:10:39.188572-07')).toBe('2026-11-07T21:10:39.188Z');
		expect(toIso(null)).toBeNull();
		expect(toIso('not a date')).toBeNull();
	});
});

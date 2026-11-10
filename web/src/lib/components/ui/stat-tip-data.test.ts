import { afterEach, describe, expect, it, vi } from 'vitest';
import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';
import { dictionaryHref, loadDefinition, resetStatTipData } from './stat-tip-data';

const LUCK = {
	metricId: 'luck_factor',
	metricName: 'Luck Factor',
	categoryName: 'Head to head',
	shortDescription: 'Measures whether a record was better or worse than its points suggest.',
	calculationFormula: 'Regular-season wins - Pythagorean wins',
	interpretationGuide: null
};

function respond(status: number, body: unknown) {
	return vi.fn(async () => new Response(JSON.stringify(body), { status }));
}

afterEach(() => {
	resetStatTipData();
	metricDefinitionsStore.clearCache();
	vi.restoreAllMocks();
});

describe('loadDefinition', () => {
	it('fetches a definition and caches it in the store', async () => {
		const fetchFn = respond(200, { metric: LUCK, related_metrics: [] });
		await expect(loadDefinition('luck_factor', fetchFn)).resolves.toEqual(LUCK);
		expect(fetchFn).toHaveBeenCalledWith(
			'/api/meta-data?metric_id=luck_factor&include_related=false'
		);
		expect(metricDefinitionsStore.getMetric('luck_factor')?.metric).toEqual(LUCK);
	});

	it('serves a definition the store already holds without fetching', async () => {
		metricDefinitionsStore.setMetric('luck_factor', { metric: LUCK as never });
		const fetchFn = respond(200, {});
		await expect(loadDefinition('luck_factor', fetchFn)).resolves.toEqual(LUCK);
		expect(fetchFn).not.toHaveBeenCalled();
	});

	it('shares one request between labels asking at once', async () => {
		const fetchFn = respond(200, { metric: LUCK });
		await Promise.all([
			loadDefinition('luck_factor', fetchFn),
			loadDefinition('luck_factor', fetchFn),
			loadDefinition('luck_factor', fetchFn)
		]);
		expect(fetchFn).toHaveBeenCalledTimes(1);
	});

	it('returns null for an unknown metric, warns once and does not ask again', async () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const fetchFn = respond(404, { error: 'Metric not found' });
		await expect(loadDefinition('nope', fetchFn)).resolves.toBeNull();
		await expect(loadDefinition('nope', fetchFn)).resolves.toBeNull();
		expect(fetchFn).toHaveBeenCalledTimes(1);
		expect(warn).toHaveBeenCalledTimes(1);
	});

	it('returns null when the request fails, with one warning', async () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const fetchFn = respond(500, { error: 'Failed to fetch meta-data' });
		await expect(loadDefinition('luck_factor', fetchFn)).resolves.toBeNull();
		await expect(loadDefinition('luck_factor', fetchFn)).resolves.toBeNull();
		expect(warn).toHaveBeenCalledTimes(1);
	});
});

describe('dictionaryHref', () => {
	it('points at the entry on the Data Dictionary', () => {
		expect(dictionaryHref('luck_factor')).toBe('/data-dictionary#luck_factor');
	});
});

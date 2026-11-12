/**
 * Where StatTip gets its copy: the same path MetricTooltip used, so the card
 * and the Data Dictionary never disagree. A definition comes out of
 * metricDefinitionsStore when it's cached there (the Data Dictionary seeds it),
 * and otherwise from /api/meta-data, which then caches it.
 *
 * Every StatTip on a page asks on mount, so it can drop its ⓘ when a definition
 * is missing. Requests for the same metric share one fetch, and a metric the API
 * doesn't know is remembered, so a table with ten "Luck" cells costs one request.
 */
import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';

/** The fields the card shows. */
export interface StatTipDefinition {
	metricId: string;
	metricName: string;
	categoryName?: string | null;
	shortDescription: string;
	calculationFormula?: string | null;
	interpretationGuide?: string | null;
}

type Fetch = (input: string) => Promise<Response>;

const inFlight = new Map<string, Promise<StatTipDefinition | null>>();
const missing = new Set<string>();
const warned = new Set<string>();

/** The Data Dictionary entry for a metric, from the Dictionary's own helper so the two can't drift. */
export { dictionaryHref } from '$lib/metrics/dictionary';

/** One console warning per metric, however many labels ask for it. */
export function warnOnce(metricId: string, reason: string): void {
	if (warned.has(metricId)) return;
	warned.add(metricId);
	console.warn(
		`StatTip: no definition for "${metricId}" (${reason}), so the label shows without an ⓘ.`
	);
}

/** A cached definition, if there is one, without fetching. */
export function cachedDefinition(metricId: string): StatTipDefinition | null {
	return metricDefinitionsStore.getMetric(metricId)?.metric ?? null;
}

/**
 * The definition for a metric, or null when there isn't one. Null covers both
 * a metric the API doesn't know and a request that failed: either way the card
 * would be empty, and StatTip never shows an empty card.
 */
export function loadDefinition(
	metricId: string,
	fetchFn: Fetch = (input) => fetch(input)
): Promise<StatTipDefinition | null> {
	const cached = cachedDefinition(metricId);
	if (cached) return Promise.resolve(cached);
	if (missing.has(metricId)) return Promise.resolve(null);

	const pending = inFlight.get(metricId);
	if (pending) return pending;

	const request = (async () => {
		try {
			const response = await fetchFn(
				`/api/meta-data?metric_id=${encodeURIComponent(metricId)}&include_related=false`
			);
			if (response.status === 404) {
				missing.add(metricId);
				warnOnce(metricId, 'not in the Data Dictionary');
				return null;
			}
			if (!response.ok) throw new Error(`HTTP ${response.status}`);
			const data = await response.json();
			if (!data?.metric) {
				missing.add(metricId);
				warnOnce(metricId, 'the API returned no metric');
				return null;
			}
			// Cached without a related_metrics key, as the store expects for a
			// definition-only record.
			metricDefinitionsStore.setMetric(metricId, { metric: data.metric });
			return data.metric as StatTipDefinition;
		} catch (error) {
			warnOnce(metricId, error instanceof Error ? error.message : 'the request failed');
			return null;
		} finally {
			inFlight.delete(metricId);
		}
	})();

	inFlight.set(metricId, request);
	return request;
}

/** Test hook: forget every miss and warning. */
export function resetStatTipData(): void {
	inFlight.clear();
	missing.clear();
	warned.clear();
}

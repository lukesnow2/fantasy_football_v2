/**
 * How the Data Dictionary shows the meta_data rows (WP11 S11.8, S11.9). Pure,
 * and the rows themselves never change here (G3): a short name or a blurb is
 * a display choice, and an unknown id falls back to what's stored.
 */
import { sentenceCase } from '$lib/constitution/display';

/** A metric as /api/meta-data serves it. Every field may be missing. */
export interface DictionaryMetric {
	metricId: string;
	metricName?: string | null;
	metricCategory?: string | null;
	categoryName?: string | null;
	shortDescription?: string | null;
	calculationFormula?: string | null;
	exampleCalculation?: string | null;
	interpretationGuide?: string | null;
	limitations?: string | null;
	dataType?: string | null;
	unitOfMeasure?: string | null;
	typicalRange?: string | null;
	goodValueThreshold?: number | null;
	excellentValueThreshold?: number | null;
	displayFormat?: string | null;
}

export interface DictionaryCategory {
	categoryId: string;
	categoryName?: string | null;
	categoryDescription?: string | null;
	metrics?: DictionaryMetric[] | null;
}

const LABELS: Record<string, string> = {
	manager_performance: 'Manager performance',
	team_performance: 'Team performance',
	competitiveness: 'League competitiveness',
	head_to_head: 'Head to head',
	trade_analysis: 'Trades',
	draft_analysis: 'Draft',
	player_value: 'Player value',
	playoff_analysis: 'Playoffs',
	consistency: 'Consistency',
	efficiency: 'Efficiency',
	record_book: 'Record book',
	league_analysis: 'League analysis'
};

const BLURBS: Record<string, string> = {
	manager_performance: 'Career statistics for each manager',
	team_performance: 'Season and weekly team strength',
	head_to_head: 'Matchups and expected results',
	competitiveness: 'How balanced a season was',
	trade_analysis: 'Who won each trade, and by how much',
	draft_analysis: 'How each pick turned out',
	player_value: 'What a player is worth over a fill-in',
	playoff_analysis: 'Chances of making the playoffs',
	consistency: "How steady a team's scoring was",
	efficiency: 'What each FAAB dollar bought',
	record_book: "The league's biggest single games",
	league_analysis: 'League-wide activity and trends'
};

/** The short name a chip, the select and a card title use. */
export function categoryLabel(id: string, storedName?: string | null): string {
	if (LABELS[id]) return LABELS[id];
	if (storedName) return sentenceCase(storedName);
	return sentenceCase(id.replace(/_/g, ' '));
}

/** The line beside a category's title. */
export function categoryBlurb(id: string, storedDescription?: string | null): string {
	return BLURBS[id] ?? storedDescription ?? '';
}

const UNITS: Record<string, string | null> = {
	games: 'Games',
	fantasy_points: 'Fantasy points',
	win_percentage: 'Win %',
	ratio: 'Ratio',
	points_per_week: 'Points per week',
	percentage_points: 'Percentage points',
	points: 'Points',
	points_per_dollar: 'Points per dollar',
	team_name: 'Manager',
	// The type and range already say these.
	index_score: null,
	score: null,
	percentage: null
};

/** The Unit tile's value, or null for no tile. */
export function unitLabel(unit: string | null | undefined): string | null {
	if (!unit) return null;
	if (unit in UNITS) return UNITS[unit];
	return sentenceCase(unit.replace(/_/g, ' '));
}

/** "0.000-1.000" reads "0.000 to 1.000"; "-5 to +5" stays as it is. */
export function formatRange(range: string | null | undefined): string | null {
	if (!range) return null;
	return range.replace(/(\d)\s*-\s*(?=\d)/g, '$1 to ');
}

/**
 * Standard deviations are stored with the excellent threshold below the good
 * one, which is the only signal in the data that says lower is better.
 */
export function lowerIsBetter(metric: DictionaryMetric): boolean {
	return (
		metric.goodValueThreshold != null &&
		metric.excellentValueThreshold != null &&
		metric.excellentValueThreshold < metric.goodValueThreshold
	);
}

export function formatValue(value: number, format: string | null | undefined): string {
	switch (format) {
		case 'percentage_1':
			return `${(value * 100).toFixed(1)}%`;
		case 'percentage_2':
			return `${(value * 100).toFixed(2)}%`;
		case 'decimal_1':
			return value.toFixed(1);
		case 'decimal_2':
			return value.toFixed(2);
		case 'decimal_3':
			return value.toFixed(3);
		case 'integer':
			return Math.round(value).toString();
		case 'currency':
			return `$${value.toFixed(2)}`;
		default:
			return value.toString();
	}
}

/** "0.600+", or "≤0.120" when lower is better. */
export function formatThreshold(
	value: number,
	format: string | null | undefined,
	lower: boolean
): string {
	return lower ? `≤${formatValue(value, format)}` : `${formatValue(value, format)}+`;
}

export interface FactTile {
	key: 'Type' | 'Unit' | 'Range' | 'Good' | 'Excellent';
	value: string;
}

/** The tiles an open entry shows, in order, each only when it has a value. */
export function factTiles(metric: DictionaryMetric): FactTile[] {
	const tiles: FactTile[] = [];
	const lower = lowerIsBetter(metric);
	if (metric.dataType) tiles.push({ key: 'Type', value: sentenceCase(metric.dataType.replace(/_/g, ' ')) });
	const unit = unitLabel(metric.unitOfMeasure);
	if (unit) tiles.push({ key: 'Unit', value: unit });
	const range = formatRange(metric.typicalRange);
	if (range) tiles.push({ key: 'Range', value: range });
	if (metric.goodValueThreshold != null) {
		tiles.push({ key: 'Good', value: formatThreshold(metric.goodValueThreshold, metric.displayFormat, lower) });
	}
	if (metric.excellentValueThreshold != null) {
		tiles.push({
			key: 'Excellent',
			value: formatThreshold(metric.excellentValueThreshold, metric.displayFormat, lower)
		});
	}
	return tiles;
}

/**
 * The categories with the metrics that match, empty categories dropped. A
 * search matches the name, the short description, the category id and the
 * category's display name. `?? ''` so a metric missing a field drops out of the
 * results instead of throwing.
 */
export function filterMetrics(
	categories: DictionaryCategory[],
	search: string,
	selectedCategory = 'all'
): Array<DictionaryCategory & { metrics: DictionaryMetric[] }> {
	const term = search.trim().toLowerCase();
	return categories
		.filter((c) => selectedCategory === 'all' || c.categoryId === selectedCategory)
		.map((c) => {
			const label = categoryLabel(c.categoryId, c.categoryName).toLowerCase();
			const metrics = (c.metrics ?? []).filter((m) => {
				if (!term) return true;
				return (
					(m.metricName ?? '').toLowerCase().includes(term) ||
					(m.shortDescription ?? '').toLowerCase().includes(term) ||
					(m.metricCategory ?? '').toLowerCase().includes(term) ||
					label.includes(term)
				);
			});
			return { ...c, metrics };
		})
		.filter((c) => c.metrics.length > 0);
}

/** The kicker: "League · 21 metrics in 12 categories", or "League" before the data loads. */
export function dictionarySummary(categories: DictionaryCategory[] | null | undefined): string {
	if (!categories || categories.length === 0) return 'League';
	const metrics = categories.reduce((n, c) => n + (c.metrics?.length ?? 0), 0);
	const m = `${metrics} metric${metrics === 1 ? '' : 's'}`;
	const c = `${categories.length} categor${categories.length === 1 ? 'y' : 'ies'}`;
	return `League · ${m} in ${c}`;
}

export interface CategoryChip {
	value: string;
	label: string;
	count: number;
}

/** "All {total}" first, then each category with its count, in the API's order. */
export function categoryChips(categories: DictionaryCategory[]): CategoryChip[] {
	const total = categories.reduce((n, c) => n + (c.metrics?.length ?? 0), 0);
	return [
		{ value: 'all', label: 'All', count: total },
		...categories.map((c) => ({
			value: c.categoryId,
			label: categoryLabel(c.categoryId, c.categoryName),
			count: c.metrics?.length ?? 0
		}))
	];
}

/** How many category chips show before "+ n more". */
export const VISIBLE_CATEGORY_CHIPS = 7;

/** The Data Dictionary entry for a metric. StatTip links here. */
export function dictionaryHref(metricId: string): string {
	return `/data-dictionary#${encodeURIComponent(metricId)}`;
}

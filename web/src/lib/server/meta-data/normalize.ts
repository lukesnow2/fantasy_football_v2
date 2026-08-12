/**
 * Row mappers for the meta-data (data dictionary) API.
 *
 * The postgres client is configured with `transform: postgres.camel`, so every
 * row that comes back from `meta_data.vw_active_metrics` already has camelCase
 * keys — `metricId`, not `metric_id`. These mappers name the fields explicitly
 * rather than passing the raw row through, so the JSON contract is written down
 * in one place instead of being whatever `SELECT *` happened to return.
 *
 * That distinction is not academic: the Data Dictionary page read snake_case
 * for its whole life, got `undefined` for every field, and rendered an empty
 * shell without anything failing loudly.
 */

/** A row off `meta_data.vw_active_metrics`, after the camelCase transform. */
export interface MetricRow {
	metricId: string;
	metricName: string;
	metricCategory: string;
	categoryName?: string | null;
	categoryDescription?: string | null;
	shortDescription: string;
	detailedDescription?: string | null;
	calculationFormula?: string | null;
	exampleCalculation?: string | null;
	interpretationGuide?: string | null;
	dataType: string;
	unitOfMeasure?: string | null;
	typicalRange?: string | null;
	goodValueThreshold?: number | string | null;
	excellentValueThreshold?: number | string | null;
	displayFormat: string;
	sortOrder?: number | string | null;
	categoryOrder?: number | string | null;
	categoryIcon?: string | null;
	categoryColor?: string | null;
}

/** A metric as served to the browser. */
export interface Metric {
	metricId: string;
	metricName: string;
	metricCategory: string;
	categoryName: string | null;
	shortDescription: string;
	detailedDescription: string | null;
	calculationFormula: string | null;
	exampleCalculation: string | null;
	interpretationGuide: string | null;
	dataType: string;
	unitOfMeasure: string | null;
	typicalRange: string | null;
	goodValueThreshold: number | null;
	excellentValueThreshold: number | null;
	displayFormat: string;
	sortOrder: number | null;
}

/** A category with its metrics, as served to the browser. */
export interface CategoryGroup {
	categoryId: string;
	categoryName: string;
	categoryDescription: string | null;
	iconName: string | null;
	colorScheme: string | null;
	metrics: Metric[];
}

/**
 * Numerics arrive from postgres as strings (they exceed JS number precision in
 * the general case), so thresholds need coercing or the page formats "0.6500"
 * as a string and `formatValue` returns it untouched.
 */
function toNumber(value: number | string | null | undefined): number | null {
	if (value === null || value === undefined || value === '') return null;
	const parsed = typeof value === 'number' ? value : Number(value);
	return Number.isFinite(parsed) ? parsed : null;
}

function orNull(value: string | null | undefined): string | null {
	return value ?? null;
}

export function toMetric(row: MetricRow): Metric {
	return {
		metricId: row.metricId,
		metricName: row.metricName,
		metricCategory: row.metricCategory,
		categoryName: orNull(row.categoryName),
		shortDescription: row.shortDescription,
		detailedDescription: orNull(row.detailedDescription),
		calculationFormula: orNull(row.calculationFormula),
		exampleCalculation: orNull(row.exampleCalculation),
		interpretationGuide: orNull(row.interpretationGuide),
		dataType: row.dataType,
		unitOfMeasure: orNull(row.unitOfMeasure),
		typicalRange: orNull(row.typicalRange),
		goodValueThreshold: toNumber(row.goodValueThreshold),
		excellentValueThreshold: toNumber(row.excellentValueThreshold),
		displayFormat: row.displayFormat,
		sortOrder: toNumber(row.sortOrder)
	};
}

/**
 * Group rows into category cards, preserving the order the query returned them
 * in (the view orders by category_order, sort_order, metric_name).
 *
 * The icon is emitted as `iconName` — the same spelling the `include_categories`
 * response uses, since both ultimately come from `metric_categories.icon_name`.
 * One name for one thing keeps consumers from having to guess which endpoint
 * they were handed.
 */
export function groupByCategory(rows: MetricRow[]): CategoryGroup[] {
	const groups = new Map<string, CategoryGroup>();

	for (const row of rows) {
		const categoryId = row.metricCategory;
		let group = groups.get(categoryId);

		if (!group) {
			group = {
				categoryId,
				categoryName: row.categoryName ?? categoryId,
				categoryDescription: orNull(row.categoryDescription),
				iconName: orNull(row.categoryIcon),
				colorScheme: orNull(row.categoryColor),
				metrics: []
			};
			groups.set(categoryId, group);
		}

		group.metrics.push(toMetric(row));
	}

	return Array.from(groups.values());
}

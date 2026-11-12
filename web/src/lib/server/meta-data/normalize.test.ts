import { describe, expect, it } from 'vitest';
import { groupByCategory, toMetric, type MetricRow } from './normalize';

// Shaped like a real row off `meta_data.vw_active_metrics` *after* the client's
// `transform: postgres.camel` has rewritten the column names. Getting this
// fixture wrong is the whole bug, so it is written out in full once.
function row(overrides: Partial<MetricRow> = {}): MetricRow {
	return {
		metricId: 'hall_of_fame_index',
		metricName: 'Hall of Fame Index',
		metricCategory: 'manager_performance',
		categoryName: 'Manager Performance',
		categoryDescription: 'Metrics that rank managers across their careers',
		shortDescription: 'Composite score of a manager career achievements',
		detailedDescription: 'Weighted blend of titles, win rate and longevity',
		calculationFormula: '(championships * 40) + (win_pct * 60)',
		exampleCalculation: '2 titles, .580 win rate -> 114.8',
		interpretationGuide: 'Above 100 is a first-ballot career',
		limitations: 'Only managers with three or more seasons are ranked',
		dataType: 'decimal',
		unitOfMeasure: 'points',
		typicalRange: '0 - 150',
		goodValueThreshold: '75.0000',
		excellentValueThreshold: '100.0000',
		displayFormat: 'decimal_1',
		sortOrder: '1',
		categoryOrder: '1',
		categoryIcon: 'trophy',
		categoryColor: 'amber',
		...overrides
	};
}

describe('toMetric', () => {
	it('emits the camelCase contract the page reads', () => {
		const metric = toMetric(row());

		expect(metric.metricId).toBe('hall_of_fame_index');
		expect(metric.metricName).toBe('Hall of Fame Index');
		expect(metric.metricCategory).toBe('manager_performance');
		expect(metric.shortDescription).toBe('Composite score of a manager career achievements');
		expect(metric.dataType).toBe('decimal');
		expect(metric.unitOfMeasure).toBe('points');
		expect(metric.typicalRange).toBe('0 - 150');
		expect(metric.displayFormat).toBe('decimal_1');
	});

	it('carries the long-form fields the dictionary page expands into', () => {
		// These live in the DB but were never surfaced anywhere except the hover
		// tooltip, which is why the page read as a list of vague one-liners.
		const metric = toMetric(row());

		expect(metric.detailedDescription).toBe('Weighted blend of titles, win rate and longevity');
		expect(metric.calculationFormula).toBe('(championships * 40) + (win_pct * 60)');
		expect(metric.exampleCalculation).toBe('2 titles, .580 win rate -> 114.8');
		expect(metric.interpretationGuide).toBe('Above 100 is a first-ballot career');
		expect(metric.limitations).toBe('Only managers with three or more seasons are ranked');
	});

	it('leaves no field undefined', () => {
		// The failure mode this whole change exists to prevent: Svelte renders
		// `undefined` as an empty string, so a missing key is invisible in the
		// markup — you get a bare "Type:" label and no error anywhere.
		const metric = toMetric(row());

		for (const [key, value] of Object.entries(metric)) {
			expect(value, `${key} should be a value or null, never undefined`).not.toBeUndefined();
		}
	});

	it('coerces numeric thresholds out of the strings postgres returns', () => {
		// pg hands back NUMERIC as a string. Left alone, `formatValue` hits its
		// default branch and prints "75.0000" where "75.0+" belongs.
		const metric = toMetric(row());

		expect(metric.goodValueThreshold).toBe(75);
		expect(metric.excellentValueThreshold).toBe(100);
		expect(metric.sortOrder).toBe(1);
	});

	it('maps absent optional columns to null, not undefined', () => {
		const metric = toMetric(
			row({
				unitOfMeasure: null,
				typicalRange: null,
				goodValueThreshold: null,
				excellentValueThreshold: null,
				detailedDescription: null
			})
		);

		expect(metric.unitOfMeasure).toBeNull();
		expect(metric.typicalRange).toBeNull();
		expect(metric.goodValueThreshold).toBeNull();
		expect(metric.excellentValueThreshold).toBeNull();
		expect(metric.detailedDescription).toBeNull();
	});
});

describe('groupByCategory', () => {
	it('splits rows into one group per category', () => {
		// The headline symptom was "21 total metrics across 1 categories" — every
		// row collapsing into a single bucket keyed "undefined".
		const rows = [
			row({ metricId: 'a', metricCategory: 'manager_performance' }),
			row({ metricId: 'b', metricCategory: 'manager_performance' }),
			row({
				metricId: 'c',
				metricCategory: 'competitiveness',
				categoryName: 'League Competitiveness',
				categoryIcon: 'target'
			}),
			row({ metricId: 'd', metricCategory: 'draft_analysis', categoryName: 'Draft Analysis' })
		];

		const groups = groupByCategory(rows);

		expect(groups).toHaveLength(3);
		expect(groups.map((g) => g.categoryId)).toEqual([
			'manager_performance',
			'competitiveness',
			'draft_analysis'
		]);
		expect(groups[0].metrics.map((m) => m.metricId)).toEqual(['a', 'b']);
		expect(groups.reduce((total, g) => total + g.metrics.length, 0)).toBe(4);
	});

	it('carries the category name, description and icon onto the group', () => {
		const [group] = groupByCategory([row()]);

		expect(group.categoryName).toBe('Manager Performance');
		expect(group.categoryDescription).toBe('Metrics that rank managers across their careers');
		// `iconName`, matching the include_categories response — the page looked up
		// `icon_name` against a payload that said `category_icon`, so every card
		// fell through to the default emoji.
		expect(group.iconName).toBe('trophy');
		expect(group.colorScheme).toBe('amber');
	});

	it('preserves the order the view returned rows in', () => {
		// The query orders by category_order, so the first category out of the DB
		// must be the first card on the page.
		const groups = groupByCategory([
			row({ metricCategory: 'record_book', categoryName: 'Record Book' }),
			row({ metricCategory: 'manager_performance' }),
			row({ metricCategory: 'record_book', categoryName: 'Record Book' })
		]);

		expect(groups.map((g) => g.categoryName)).toEqual(['Record Book', 'Manager Performance']);
	});

	it('falls back to the category id when the join found no category row', () => {
		// vw_active_metrics LEFT JOINs metric_categories, so an unseeded category
		// yields a null name. Better a raw id than a blank heading.
		const [group] = groupByCategory([row({ categoryName: null, categoryDescription: null })]);

		expect(group.categoryName).toBe('manager_performance');
		expect(group.categoryDescription).toBeNull();
	});

	it('returns an empty list for no rows', () => {
		expect(groupByCategory([])).toEqual([]);
	});
});

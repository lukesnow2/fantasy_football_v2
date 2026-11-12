import { describe, expect, it } from 'vitest';
import {
	categoryBlurb,
	categoryChips,
	categoryLabel,
	dictionaryHref,
	dictionarySummary,
	factTiles,
	filterMetrics,
	formatRange,
	formatThreshold,
	unitLabel,
	type DictionaryCategory
} from './dictionary';

describe('categoryLabel', () => {
	it('gives the board short names', () => {
		expect(categoryLabel('competitiveness', 'League Competitiveness')).toBe('League competitiveness');
		expect(categoryLabel('head_to_head', 'Head-to-Head Analysis')).toBe('Head to head');
		expect(categoryLabel('trade_analysis')).toBe('Trades');
		expect(categoryLabel('playoff_analysis')).toBe('Playoffs');
	});

	it('falls back to the stored name, sentence-cased', () => {
		expect(categoryLabel('weather', 'Weather Effects')).toBe('Weather effects');
		expect(categoryLabel('weather_effects')).toBe('Weather effects');
	});
});

describe('categoryBlurb', () => {
	it("gives the board's four and the eight others", () => {
		expect(categoryBlurb('manager_performance')).toBe('Career statistics for each manager');
		expect(categoryBlurb('competitiveness')).toBe('How balanced a season was');
		expect(categoryBlurb('efficiency')).toBe('What each FAAB dollar bought');
	});

	it('falls back to the stored description', () => {
		expect(categoryBlurb('weather', 'How the weather changed scoring')).toBe(
			'How the weather changed scoring'
		);
		expect(categoryBlurb('weather')).toBe('');
	});
});

describe('unitLabel', () => {
	it('names the known units', () => {
		expect(unitLabel('games')).toBe('Games');
		expect(unitLabel('win_percentage')).toBe('Win %');
		expect(unitLabel('team_name')).toBe('Manager');
		expect(unitLabel('points_per_dollar')).toBe('Points per dollar');
	});

	it('hides the units the type already says', () => {
		expect(unitLabel('index_score')).toBeNull();
		expect(unitLabel('score')).toBeNull();
		expect(unitLabel('percentage')).toBeNull();
		expect(unitLabel(null)).toBeNull();
	});

	it('sentence-cases anything else', () => expect(unitLabel('average_picks')).toBe('Average picks'));
});

describe('formatRange', () => {
	it('reads a hyphen between numbers as "to"', () => {
		expect(formatRange('0.000-1.000')).toBe('0.000 to 1.000');
		expect(formatRange('0-100%')).toBe('0 to 100%');
	});

	it('leaves a range already in words', () => {
		expect(formatRange('-5 to +5')).toBe('-5 to +5');
	});

	it('is null with no range', () => expect(formatRange(null)).toBeNull());
});

describe('formatThreshold', () => {
	it('reads higher is better as a floor', () => {
		expect(formatThreshold(0.6, 'decimal_3', false)).toBe('0.600+');
		expect(formatThreshold(0.55, 'percentage_1', false)).toBe('55.0%+');
	});

	it('reads lower is better as a ceiling', () => {
		expect(formatThreshold(0.12, 'decimal_3', true)).toBe('≤0.120');
	});
});

describe('factTiles', () => {
	it('lists only the tiles with values, in order', () => {
		expect(
			factTiles({
				metricId: 'luck_factor',
				dataType: 'decimal',
				unitOfMeasure: 'games',
				typicalRange: '-5 to +5',
				displayFormat: 'decimal_1'
			})
		).toEqual([
			{ key: 'Type', value: 'Decimal' },
			{ key: 'Unit', value: 'Games' },
			{ key: 'Range', value: '-5 to +5' }
		]);
	});

	it('flips both thresholds when excellent sits under good', () => {
		const tiles = factTiles({
			metricId: 'season_consistency_score',
			dataType: 'decimal',
			goodValueThreshold: 0.14,
			excellentValueThreshold: 0.12,
			displayFormat: 'decimal_3'
		});
		expect(tiles.slice(-2)).toEqual([
			{ key: 'Good', value: '≤0.140' },
			{ key: 'Excellent', value: '≤0.120' }
		]);
	});
});

const categories: DictionaryCategory[] = [
	{
		categoryId: 'manager_performance',
		categoryName: 'Manager Performance',
		metrics: [
			{ metricId: 'hall_of_fame_index', metricName: 'Hall of Fame Index', shortDescription: 'Composite score', metricCategory: 'manager_performance' },
			{ metricId: 'career_win_percentage', metricName: 'Career Win Percentage', shortDescription: 'Share of games won', metricCategory: 'manager_performance' }
		]
	},
	{
		categoryId: 'head_to_head',
		categoryName: 'Head-to-Head Analysis',
		metrics: [
			{ metricId: 'luck_factor', metricName: 'Luck Factor', shortDescription: 'Actual wins minus Pythagorean wins.', metricCategory: 'head_to_head' },
			{ metricId: 'broken', metricName: null, shortDescription: null, metricCategory: null }
		]
	}
];

describe('filterMetrics', () => {
	it('matches the name', () => {
		const out = filterMetrics(categories, 'hall of');
		expect(out.map((c) => c.categoryId)).toEqual(['manager_performance']);
		expect(out[0].metrics.map((m) => m.metricId)).toEqual(['hall_of_fame_index']);
	});

	it("matches the category's display name", () => {
		const out = filterMetrics(categories, 'head to head');
		expect(out[0].metrics.map((m) => m.metricId)).toEqual(['luck_factor', 'broken']);
	});

	it('drops a metric missing its fields instead of throwing', () => {
		const out = filterMetrics(categories, 'pythag');
		expect(out[0].metrics.map((m) => m.metricId)).toEqual(['luck_factor']);
	});

	it('narrows to one category', () => {
		expect(filterMetrics(categories, '', 'head_to_head').map((c) => c.categoryId)).toEqual(['head_to_head']);
	});
});

describe('dictionarySummary', () => {
	it('counts metrics and categories', () => {
		expect(dictionarySummary(categories)).toBe('League · 4 metrics in 2 categories');
	});

	it('reads just League while loading', () => {
		expect(dictionarySummary([])).toBe('League');
		expect(dictionarySummary(null)).toBe('League');
	});
});

describe('categoryChips', () => {
	it('starts with All and its total', () => {
		expect(categoryChips(categories)).toEqual([
			{ value: 'all', label: 'All', count: 4 },
			{ value: 'manager_performance', label: 'Manager performance', count: 2 },
			{ value: 'head_to_head', label: 'Head to head', count: 2 }
		]);
	});
});

describe('dictionaryHref', () => {
	it('links to the entry', () => expect(dictionaryHref('luck_factor')).toBe('/data-dictionary#luck_factor'));
});

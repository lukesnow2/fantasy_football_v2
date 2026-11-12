/**
 * Sample data for the WP1 stories. The copy is the System board's and Power
 * Rankings', so a story reads the same as the board it's checked against.
 * Storybook has no /api/meta-data, so the StatTip stories seed the store.
 */
import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';

export const LUCK_FACTOR = {
	metricId: 'luck_factor',
	metricName: 'Luck Factor',
	metricCategory: 'head_to_head',
	categoryName: 'Head to head',
	shortDescription:
		'Measures whether a record was better or worse than its points suggest. Positive is lucky.',
	calculationFormula: 'Regular-season wins - Pythagorean wins',
	interpretationGuide:
		"One season's spread here is about 1.1 wins, so ±1.5 is a notable year and ±2.5 is rare.",
	dataType: 'decimal',
	displayFormat: 'decimal_1'
};

const OTHERS = [
	['pythagorean_wins', 'Pyth W', 'Wins a team would expect from its points for and against.'],
	['power_score', 'Power score', 'One number for how strong a team is right now.'],
	['hall_of_fame_index', 'HoF index', 'A career score built from titles, wins and playoff runs.'],
	['draft_grade', 'Grade', 'How a draft turned out, from A to F.']
] as const;

export function seedDefinitions() {
	metricDefinitionsStore.setMetric(LUCK_FACTOR.metricId, { metric: LUCK_FACTOR as never });
	for (const [metricId, metricName, shortDescription] of OTHERS) {
		metricDefinitionsStore.setMetric(metricId, {
			metric: { metricId, metricName, shortDescription, categoryName: null } as never
		});
	}
}

/** This Season's standings and strip, as the board draws them (2026, week 3). */
export const STANDINGS = [
	['Israel', '3-0', 424.1, 380.5, 85, 79],
	['Craig', '2-1', 465.5, 402.2, 86, 89],
	['Gabe Flores', '2-1', 368.35, 346.3, 63, 46],
	['Trevor', '2-1', 368.2, 359.95, 61, 55],
	['Nick', '1-2', 450.8, 432.65, 75, 83],
	['Omar', '1-2', 372.45, 377.9, 51, 58],
	['Erik Snow', '1-2', 363.15, 357.6, 48, 59],
	['Gabe the Younger', '1-2', 357.4, 361.8, 45, 54],
	['Luke S', '1-2', 353.25, 381.35, 45, 40],
	['Troy Colvin', '1-2', 337.05, 460.0, 40, 39]
].map(([name, wl, pf, pa, odds, lastWeek], index) => ({
	rank: index + 1,
	name: name as string,
	wl: wl as string,
	pf: (pf as number).toFixed(1),
	pa: (pa as number).toFixed(1),
	odds: odds as number,
	move: (odds as number) - (lastWeek as number)
}));

export const SEASON_STATS = [
	{ label: 'Current week', value: '3 of 14', sub: 'Playoffs start week 15' },
	{ label: 'Most points for', value: '465.5', sub: 'Craig' },
	{ label: 'Only unbeaten', value: '3-0', sub: 'Israel' },
	{
		label: 'Points leaders who won the title',
		value: '8 of 21',
		sub: 'Since 2005. All 21 made the playoffs.'
	}
];

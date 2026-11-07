import { describe, expect, it } from 'vitest';
import { topBusts, topSteals, valueDraftPicks, type DraftPickInput } from './draft-value';

type Pick = DraftPickInput & { name: string };

/** A finished season shaped like this league: QBs score most, K/DEF go late. */
function season(year: number, overrides: Partial<Record<string, number>> = {}): Pick[] {
	const picks: Pick[] = [];
	for (let round = 1; round <= 15; round++) {
		const late = round >= 12;
		const slots: [string, number][] = late
			? [
					['K', 110],
					['DEF', 100],
					['RB', 220 - round * 12],
					['WR', 220 - round * 12]
				]
			: [
					['QB', 420 - round * 10],
					['RB', 300 - round * 14],
					['WR', 300 - round * 14],
					['TE', 240 - round * 10]
				];
		for (const [position, points] of slots) {
			const name = `${year}-${position}-${round}`;
			picks.push({
				seasonYear: year,
				roundNumber: round,
				position,
				seasonPoints: overrides[name] ?? points,
				name
			});
		}
	}
	return picks;
}

const history = [season(2022), season(2023), season(2024)].flat();
const done = new Set([2022, 2023, 2024]);

describe('valueDraftPicks', () => {
	it('expects more from a QB than an RB in the same round', () => {
		const v = valueDraftPicks(history, done);
		const qb = v.find((p) => p.name === '2024-QB-3')!;
		const rb = v.find((p) => p.name === '2024-RB-3')!;
		expect(qb.expectedPoints).toBeGreaterThan(rb.expectedPoints);
	});

	it('a late kicker who was cut is not a bust; an early first-rounder who flopped is', () => {
		const flops = season(2025, { '2025-K-14': 5, '2025-RB-1': 60 });
		const v = valueDraftPicks([...history, ...flops], done).filter((p) => p.seasonYear === 2025);
		const kicker = v.find((p) => p.name === '2025-K-14')!;
		expect(kicker.highExpectation).toBe(false);
		const busts = topBusts(v, 3).map((p) => p.name);
		expect(busts).toContain('2025-RB-1');
		expect(busts).not.toContain('2025-K-14');
	});

	it('a kicker reached for in an early round carries that round expectation', () => {
		const reach: Pick = {
			seasonYear: 2025,
			roundNumber: 4,
			position: 'K',
			seasonPoints: 110,
			name: 'reach'
		};
		const v = valueDraftPicks([...history, ...season(2025), reach], done);
		const k = v.find((p) => p.name === 'reach')!;
		expect(k.highExpectation).toBe(true);
		expect(
			topBusts(
				v.filter((p) => p.seasonYear === 2025),
				1
			)[0].name
		).toBe('reach');
	});

	it('a late breakout is the top steal', () => {
		const v = valueDraftPicks([...history, ...season(2025, { '2025-WR-13': 330 })], done);
		expect(
			topSteals(
				v.filter((p) => p.seasonYear === 2025),
				1
			)[0].name
		).toBe('2025-WR-13');
	});

	it('judges a partial season on its own scale', () => {
		// The same draft three weeks in (a third of the points) must read exactly
		// like the full season, relative to expectation: small totals are not busts.
		const full = season(2025);
		const partial = season(2026).map((p) => ({ ...p, seasonPoints: p.seasonPoints / 3 }));
		const v = valueDraftPicks([...history, ...full, ...partial], done);
		const ratio = (name: string) => {
			const p = v.find((x) => x.name === name)!;
			return p.valueOverExpected / p.expectedPoints;
		};
		for (const pick of full) {
			expect(ratio(pick.name.replace('2025', '2026'))).toBeCloseTo(ratio(pick.name), 9);
		}
	});

	it('a late-round pick is never a bust, even a QB the league expects a lot from', () => {
		// QBs out-score an average pick deep into the draft, but a round-11 flier
		// isn't a pick the draft counted on.
		const flier: Pick = {
			seasonYear: 2025,
			roundNumber: 11,
			position: 'QB',
			seasonPoints: 0,
			name: 'flier'
		};
		const v = valueDraftPicks([...history, ...season(2025), flier], done);
		const qb = v.find((p) => p.name === 'flier')!;
		expect(qb.expectedRelative).toBeGreaterThan(1);
		expect(qb.highExpectation).toBe(false);
		expect(topBusts(v, 50).map((p) => p.name)).not.toContain('flier');
	});
});

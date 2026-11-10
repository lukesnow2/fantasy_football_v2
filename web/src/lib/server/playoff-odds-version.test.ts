import { describe, expect, it } from 'vitest';
import { DEFAULT_PARAMS, simulatePlayoffOdds } from '$lib/playoff-odds';
import { modelVersion } from './playoff-odds-version';

const site = {
	sims: 10000,
	seed: (season: number, weeks: number) => season * 100 + weeks,
	params: DEFAULT_PARAMS
};

describe('modelVersion', () => {
	it('is stable and short', () => {
		const v = modelVersion(site);
		expect(v).toMatch(/^[0-9a-f]{12}$/);
		expect(modelVersion(site)).toBe(v);
	});

	it('ignores how the model is called, only what it returns', () => {
		const wrapped: typeof simulatePlayoffOdds = (opts) => simulatePlayoffOdds({ ...opts });
		expect(modelVersion(site, wrapped)).toBe(modelVersion(site));
	});

	it('changes when a model parameter changes', () => {
		const withParams = (p: Partial<typeof DEFAULT_PARAMS>) =>
			modelVersion({ ...site, params: { ...DEFAULT_PARAMS, ...p } });
		expect(withParams({ shrinkage: 4 })).not.toBe(modelVersion(site));
		expect(withParams({ drift: 0.25 })).not.toBe(modelVersion(site));
		expect(withParams({})).toBe(modelVersion(site));
	});

	it('changes when the engine returns different numbers', () => {
		const nudged: typeof simulatePlayoffOdds = (opts) => {
			const res = simulatePlayoffOdds(opts);
			return {
				...res,
				teams: res.teams.map((t, i) => (i ? t : { ...t, titleOdds: t.titleOdds + 0.0005 }))
			};
		};
		expect(modelVersion(site, nudged)).not.toBe(modelVersion(site));
	});

	it('changes with the site simulation count or seed rule', () => {
		expect(modelVersion({ ...site, sims: 20000 })).not.toBe(modelVersion(site));
		expect(modelVersion({ ...site, seed: (s, w) => s * 1000 + w })).not.toBe(modelVersion(site));
	});
});

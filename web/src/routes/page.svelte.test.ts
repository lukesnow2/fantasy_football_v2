import { page } from '@vitest/browser/context';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { render } from 'vitest-browser-svelte';
import Page from './+page.svelte';

const STANDINGS = [
	{ managerName: 'Israel', wins: 3, losses: 0, ties: 0, playoffTier: 'Champion' },
	{ managerName: 'Craig', wins: 2, losses: 1, ties: 0, playoffTier: 'Runner-up' },
	{ managerName: 'Gabe Flores', wins: 2, losses: 1, ties: 0, playoffTier: 'Semifinalist' },
	{ managerName: 'Trevor', wins: 2, losses: 1, ties: 1, playoffTier: 'Semifinalist' },
	{ managerName: 'Nick', wins: 1, losses: 2, ties: 0, playoffTier: 'Quarterfinalist' }
];

const ODDS = [
	{ managerName: 'Israel', playoffOdds: 0.85, clinched: false, eliminated: false },
	{ managerName: 'Craig', playoffOdds: 0.857, clinched: false, eliminated: false },
	{ managerName: 'Gabe Flores', playoffOdds: 0.63, clinched: false, eliminated: false },
	{ managerName: 'Trevor', playoffOdds: 0.996, clinched: false, eliminated: false }
];

/** Stub every API Home reads; anything not given answers 500. */
function stubApis(responses: Record<string, unknown>) {
	vi.stubGlobal(
		'fetch',
		vi.fn(async (url: string) => {
			const path = String(url).split('?')[0];
			return path in responses
				? new Response(JSON.stringify(responses[path]), { status: 200 })
				: new Response('{}', { status: 500 });
		})
	);
}

function season(isSeasonComplete: boolean, phase: string) {
	return {
		'/api/standings': {
			standings: STANDINGS,
			season: 2025,
			currentWeek: 3,
			regularSeasonWeeks: 14,
			isSeasonComplete
		},
		'/api/playoff-odds': { available: true, phase, teams: ODDS }
	};
}

afterEach(() => {
	vi.unstubAllGlobals();
	vi.restoreAllMocks();
});

describe('/+page.svelte', () => {
	it('renders one h1, "The League"', async () => {
		stubApis({});
		vi.spyOn(console, 'error').mockImplementation(() => {});
		render(Page);

		const heading = page.getByRole('heading', { level: 1 });
		await expect.element(heading).toHaveTextContent('The League');
		expect(document.querySelectorAll('h1')).toHaveLength(1);
	});

	it('gives each card its own failed state with a way to try again', async () => {
		stubApis({});
		vi.spyOn(console, 'error').mockImplementation(() => {});
		render(Page);

		await expect.element(page.getByText("Couldn't load the standings.")).toBeInTheDocument();
		await expect.element(page.getByText("Couldn't load the latest trades.")).toBeInTheDocument();
		expect(page.getByRole('button', { name: 'Try again' }).elements().length).toBeGreaterThan(4);
	});

	it('draws the top four with playoff odds in the regular season', async () => {
		stubApis(season(false, 'regular'));
		vi.spyOn(console, 'error').mockImplementation(() => {});
		render(Page);

		await expect.element(page.getByRole('heading', { name: 'This week' })).toBeInTheDocument();
		await expect.element(page.getByText('Wk 3 of 14')).toBeInTheDocument();
		await expect.element(page.getByText('86%')).toBeInTheDocument();
		await expect.element(page.getByText('>99%')).toBeInTheDocument();
		await expect.element(page.getByText('2-1-1')).toBeInTheDocument();
		await expect.element(page.getByText('Record · playoff odds')).toBeInTheDocument();
		expect(page.getByRole('link', { name: 'Nick' }).elements()).toHaveLength(0);
	});

	it('reads None for a manager missing from the odds', async () => {
		const responses = season(false, 'regular');
		responses['/api/playoff-odds'].teams = ODDS.filter((t) => t.managerName !== 'Trevor');
		stubApis(responses);
		vi.spyOn(console, 'error').mockImplementation(() => {});
		render(Page);

		await expect.element(page.getByText('None', { exact: true })).toBeInTheDocument();
		await expect.element(page.getByText('86%')).toBeInTheDocument();
	});

	it('hides the odds during the playoffs (D4)', async () => {
		stubApis(season(false, 'postseason'));
		vi.spyOn(console, 'error').mockImplementation(() => {});
		render(Page);

		await expect.element(page.getByText('Playoffs', { exact: true })).toBeInTheDocument();
		await expect.element(page.getByText('Record', { exact: true })).toBeInTheDocument();
		expect(page.getByText('86%').elements()).toHaveLength(0);
	});

	it('turns into the final standings with Finish tags after the final (D4)', async () => {
		stubApis(season(true, 'postseason'));
		vi.spyOn(console, 'error').mockImplementation(() => {});
		render(Page);

		await expect
			.element(page.getByRole('heading', { name: 'Final standings' }))
			.toBeInTheDocument();
		await expect.element(page.getByText('2025 · Final')).toBeInTheDocument();
		await expect.element(page.getByText('Champion', { exact: true })).toBeInTheDocument();
		await expect.element(page.getByText('Runner-up', { exact: true })).toBeInTheDocument();
		await expect.element(page.getByText('Record · finish')).toBeInTheDocument();
		await expect.element(page.getByRole('link', { name: 'Final standings →' })).toBeInTheDocument();
		expect(page.getByText('86%').elements()).toHaveLength(0);
	});
});

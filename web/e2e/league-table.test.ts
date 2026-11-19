import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';

/**
 * The League Table (WP14 S14.8) at 1280, on dev-database data from
 * 2026-11-16: /api/managers/performance, /api/draft and /api/hall-of-fame are
 * stubbed from e2e/fixtures/league-table.json.
 */
const f = JSON.parse(
	readFileSync(new URL('./fixtures/league-table.json', import.meta.url), 'utf8')
);

async function stub(page: Page) {
	const json = (body: unknown) => ({ contentType: 'application/json', body: JSON.stringify(body) });
	await page.route(/\/api\/managers\/performance/, (r) => r.fulfill(json(f.all)));
	await page.route(/\/api\/hall-of-fame/, (r) => r.fulfill(json(f.hallOfFame)));
	await page.route(/\/api\/draft/, (r) => r.fulfill(json(f.draft)));
}

test.use({ viewport: { width: 1280, height: 900 } });

test.beforeEach(async ({ page }) => {
	await stub(page);
});

const table = (page: Page) => page.getByRole('table');
const firstName = (page: Page) =>
	table(page).locator('tbody tr').first().locator('td').nth(1).getByRole('link');

test('opens on Overview, sorted by Titles', async ({ page }) => {
	await page.goto('/league-table');
	await expect(page.getByRole('heading', { level: 2, name: 'Overview' })).toBeVisible();
	await expect(page.getByRole('radio', { name: 'Overview' })).toHaveAttribute(
		'aria-checked',
		'true'
	);
	await expect(page.getByText('11 managers · sorted by titles, highest first')).toBeVisible();
	await expect(table(page).locator('th[aria-sort="descending"]')).toContainText('Titles');
	await expect(firstName(page)).toContainText('Trevor');
});

test('clicking Points against in Scoring sorts lowest first', async ({ page }) => {
	await page.goto('/league-table');
	await page.getByRole('radio', { name: 'Scoring' }).click();
	await table(page).getByRole('button', { name: 'Points against' }).click();
	const header = table(page).locator('th', { hasText: 'Points against' });
	await expect(header).toHaveAttribute('aria-sort', 'ascending');
	await expect(page).toHaveURL(/\/league-table\?group=scoring&sort=pa$/);

	const lowest = f.all.data.performance.reduce(
		(a: { pointsAgainst: number }, b: { pointsAgainst: number }) =>
			b.pointsAgainst < a.pointsAgainst ? b : a
	);
	await expect(firstName(page)).toContainText(lowest.managerName);

	// A second click flips it.
	await table(page).getByRole('button', { name: 'Points against' }).click();
	await expect(header).toHaveAttribute('aria-sort', 'descending');
	await expect(page).toHaveURL(/\/league-table\?group=scoring&sort=pa&dir=desc$/);
});

test('Active only removes former managers', async ({ page }) => {
	await page.goto('/league-table');
	await expect(table(page).getByRole('link', { name: /Bobby/ })).toBeVisible();
	await page.getByRole('radio', { name: 'Active only' }).click();
	await expect(table(page).getByRole('link', { name: /Bobby/ })).toHaveCount(0);
	await expect(page.getByText('10 managers · sorted by titles, highest first')).toBeVisible();
	await expect(page).toHaveURL(/\/league-table\?who=active$/);
});

test('a link opens the same view', async ({ page }) => {
	await page.goto('/league-table?group=scoring&sort=pa&who=active');
	await expect(page.getByRole('heading', { level: 2, name: 'Scoring' })).toBeVisible();
	await expect(page.getByRole('radio', { name: 'Active only' })).toHaveAttribute(
		'aria-checked',
		'true'
	);
	await expect(table(page).locator('th', { hasText: 'Points against' })).toHaveAttribute(
		'aria-sort',
		'ascending'
	);
	await expect(
		page.getByText('10 managers · sorted by points against, lowest first')
	).toBeVisible();

	// An unknown value falls back to the default.
	await page.goto('/league-table?group=nope');
	await expect(page.getByRole('heading', { level: 2, name: 'Overview' })).toBeVisible();
});

test('a Hall of Fame that fails to load reads "None", not "Under 3 seasons"', async ({ page }) => {
	await page.route(/\/api\/hall-of-fame/, (r) => r.fulfill({ status: 500, body: '' }));
	await page.goto('/league-table?group=legacy');
	await expect(page.getByRole('heading', { level: 2, name: 'Legacy' })).toBeVisible();
	await expect(table(page).getByText('Under 3 seasons')).toHaveCount(0);
	const trevor = table(page).locator('tbody tr', { hasText: 'Trevor' });
	await expect(trevor.getByText('None')).toHaveCount(2);
});

test('the header ⓘ and the sort button have different names', async ({ page }) => {
	await page.goto('/league-table');
	await expect(table(page).getByRole('button', { name: 'Luck', exact: true })).toHaveCount(1);
	await expect(table(page).getByRole('button', { name: 'About Luck' })).toHaveCount(1);
});

test('the Rankings menu links to it', async ({ page }) => {
	await page.goto('/hall-of-fame');
	await page.getByRole('button', { name: /Rankings/ }).click();
	await page.getByRole('menuitem', { name: /League Table/ }).click();
	await expect(page).toHaveURL(/\/league-table$/);
	await expect(page.getByRole('heading', { level: 1, name: 'League Table' })).toBeVisible();
});

test.describe('a span of seasons', () => {
	/** Dev-database data from 2026-11-19, the seasons API with the three it adds up from. */
	const s = JSON.parse(
		readFileSync(new URL('./fixtures/league-table-seasons.json', import.meta.url), 'utf8')
	);
	const seasonsRequests: string[] = [];

	test.beforeEach(async ({ page }) => {
		seasonsRequests.length = 0;
		const json = (body: unknown) => ({
			contentType: 'application/json',
			body: JSON.stringify(body)
		});
		// Registered after the file's stubs, so these answer first.
		await page.route(/\/api\/managers\/performance/, (r) => r.fulfill(json(s.all)));
		await page.route(/\/api\/hall-of-fame/, (r) => r.fulfill(json(s.hallOfFame)));
		await page.route(/\/api\/draft/, (r) => r.fulfill(json(s.draft)));
		await page.route(/\/api\/managers\/seasons/, (r) => {
			seasonsRequests.push(r.request().url());
			return r.fulfill(json(s.seasons));
		});
	});

	test('picking seasons counts only those seasons', async ({ page }) => {
		await page.goto('/league-table');
		await expect(firstName(page)).toBeVisible();
		// All time needs no seasons.
		expect(seasonsRequests).toHaveLength(0);

		await page.getByRole('combobox', { name: 'First season' }).selectOption('2010');
		await page.getByRole('combobox', { name: 'Last season' }).selectOption('2020');
		await expect(page).toHaveURL(/\/league-table\?from=2010&to=2020$/);
		await expect(page.locator('main')).toContainText('Rankings · 2010 to 2020');

		const titles = (name: string) =>
			s.seasons.data.teamSeasons
				.filter(
					(t: { managerName: string; seasonYear: number }) =>
						t.managerName === name && t.seasonYear >= 2010 && t.seasonYear <= 2020
				)
				.reduce((a: number, t: { titles: number }) => a + t.titles, 0);
		const trevor = table(page).locator('tbody tr', { hasText: 'Trevor' });
		await expect(trevor.locator('td').nth(3)).toHaveText(String(titles('Trevor')));
		await expect(trevor).toContainText('2010 to 2020');
		await expect(trevor.getByText('All time only')).toBeVisible();
		expect(seasonsRequests).toHaveLength(1);
	});

	test('a last season before the first moves both ends, and a link opens the span', async ({
		page
	}) => {
		await page.goto('/league-table?from=2010&to=2020');
		await expect(page.getByRole('combobox', { name: 'First season' })).toHaveValue('2010');
		await page.getByRole('combobox', { name: 'Last season' }).selectOption('2008');
		await expect(page).toHaveURL(/\/league-table\?from=2008&to=2008$/);
		await expect(page.getByRole('combobox', { name: 'First season' })).toHaveValue('2008');

		// Back to the league's first and latest seasons is all time again.
		await page.getByRole('combobox', { name: 'First season' }).selectOption('2005');
		await page.getByRole('combobox', { name: 'Last season' }).selectOption('2026');
		await expect(page).toHaveURL(/\/league-table$/);
		await expect(table(page).getByText('All time only')).toHaveCount(0);
	});
});

import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';

/**
 * Managers and the manager profile at 1280 (WP6 S6.10), on dev-database
 * data from 2026 week 8: /api/managers/performance, /api/hall-of-fame and
 * /api/draft are stubbed from e2e/fixtures/managers.json.
 */
const f = JSON.parse(readFileSync(new URL('./fixtures/managers.json', import.meta.url), 'utf8'));

async function stub(page: Page) {
	const json = (body: unknown) => ({ contentType: 'application/json', body: JSON.stringify(body) });
	await page.route(/\/api\/managers\/performance/, (r) => {
		const manager = new URL(r.request().url()).searchParams.get('manager');
		return r.fulfill(json(manager ? f.trevor : f.all));
	});
	await page.route(/\/api\/hall-of-fame/, (r) => r.fulfill(json(f.hallOfFame)));
	await page.route(/\/api\/draft/, (r) => r.fulfill(json(f.draft)));
}

test.use({ viewport: { width: 1280, height: 900 } });

test.beforeEach(async ({ page }) => {
	await stub(page);
});

const tabs = (page: Page) => page.getByRole('navigation', { name: 'Profile sections' });

test('a ?tab= link opens on that tab', async ({ page }) => {
	await page.goto('/managers/Trevor?tab=head-to-head');
	await expect(tabs(page).getByRole('link', { name: 'Head to head' })).toHaveAttribute(
		'aria-current',
		'page'
	);
	await expect(page.getByRole('heading', { name: 'Against every manager' })).toBeVisible();
});

test('a tab click changes the URL in place, and Back leaves the profile', async ({ page }) => {
	await page.goto('/managers');
	await expect(page.getByRole('navigation', { name: 'Pick a manager' })).toBeVisible();
	await page.goto('/managers/Trevor?tab=head-to-head');
	await expect(page.getByRole('heading', { name: 'Against every manager' })).toBeVisible();

	let fetches = 0;
	page.on('request', (r) => {
		if (r.url().includes('/api/managers/performance')) fetches++;
	});
	const scroll = await page.evaluate(() => window.scrollY);
	await tabs(page).getByRole('link', { name: 'Achievements' }).click();
	await expect(page).toHaveURL(/\/managers\/Trevor\?tab=achievements$/);
	// Shallow: no refetch and no scroll jump.
	expect(fetches).toBe(0);
	expect(await page.evaluate(() => window.scrollY)).toBe(scroll);
	await expect(page.getByRole('heading', { name: 'Title games' })).toBeVisible();
	await expect(tabs(page).getByRole('link', { name: 'Achievements' })).toHaveAttribute(
		'aria-current',
		'page'
	);

	await page.goBack();
	await expect(page).toHaveURL(/\/managers$/);
});

test("the hero's Head to head link opens that tab", async ({ page }) => {
	await page.goto('/managers?manager=Trevor');
	const hero = page.getByRole('navigation', { name: "Trevor's full profile" });
	await hero.getByRole('link', { name: 'Head to head' }).click();
	await expect(page).toHaveURL(/\/managers\/Trevor\?tab=head-to-head$/);
	await expect(page.getByRole('heading', { name: 'Against every manager' })).toBeVisible();
});

test('a rail click selects in place without a history entry', async ({ page }) => {
	await page.goto('/');
	await page.goto('/managers?manager=Trevor');
	const rail = page.getByRole('navigation', { name: 'Pick a manager' });
	await rail.getByRole('link', { name: /Omar/ }).click();
	await expect(page).toHaveURL(/\/managers\?manager=Omar$/);
	await expect(rail.getByRole('link', { name: /Omar/ })).toHaveAttribute('aria-current', 'page');
	await expect(page.getByRole('heading', { level: 2, name: 'Omar' })).toBeVisible();
	await page.goBack();
	await expect(page).toHaveURL(/\/$/);
});

test('every Rivalry link names the tab and both managers', async ({ page }) => {
	await page.goto('/managers/Trevor?tab=head-to-head');
	const links = page.getByRole('link', { name: 'Rivalry →' });
	await expect(links.first()).toBeVisible();
	const hrefs = await links.evaluateAll((els) => els.map((e) => e.getAttribute('href') ?? ''));
	expect(hrefs.length).toBe(f.trevor.data.head_to_head.length);
	for (const href of hrefs) {
		const url = new URL(href, 'http://x');
		expect(url.pathname).toBe('/historical');
		expect(url.searchParams.get('tab')).toBe('head-to-head');
		expect(url.searchParams.get('a')).toBe('Trevor');
		expect(url.searchParams.get('b')).toBeTruthy();
	}
});

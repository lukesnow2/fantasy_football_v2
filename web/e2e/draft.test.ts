import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';

/**
 * Draft Central's 2025 board at 1280 (WP8 S8.11), on dev-database data from
 * 2026 week 3: /api/draft is stubbed from e2e/fixtures/draft.json, the
 * all-seasons response (its picks trimmed to 2025) and the 2025 season.
 */
const f = JSON.parse(readFileSync(new URL('./fixtures/draft.json', import.meta.url), 'utf8'));

async function stub(page: Page) {
	await page.route(/\/api\/draft/, (r) => {
		const season = new URL(r.request().url()).searchParams.get('season');
		return r.fulfill({
			contentType: 'application/json',
			body: JSON.stringify(season === '2025' ? f.season2025 : f.all)
		});
	});
}

test.use({ viewport: { width: 1280, height: 900 } });

test('the 2025 board tags no kicker or defense, and a click shows that pick', async ({ page }) => {
	await stub(page);
	await page.goto('/draft');
	await page.getByLabel('Season').selectOption('2025');
	await expect(page.getByRole('heading', { name: 'The 2025 draft', exact: true })).toBeVisible();

	const cells = page.locator('button[aria-label^="Pick "]');
	await expect(cells).toHaveCount(150);

	// Kickers and defenses: graded, never tagged.
	const special = page.locator(
		'button[aria-label^="Pick "][aria-label*=", K, grade"], button[aria-label^="Pick "][aria-label*=", DEF, grade"]'
	);
	expect(await special.count()).toBeGreaterThan(0);
	for (const cell of await special.all()) {
		await expect(cell.locator('.tag')).toHaveCount(0);
	}
	// Other cells do carry tags, so the check above isn't vacuous.
	expect(await cells.locator('.tag').count()).toBeGreaterThan(0);

	// Click a pick: the Selected pick card shows it, and only it is pressed.
	const card = page.getByRole('region', { name: 'Selected pick' });
	const target = page.locator('button[aria-label^="Pick 1,"]');
	const label = (await target.getAttribute('aria-label')) ?? '';
	const player = label.split(', ')[1];
	await target.click();
	await expect(card).toContainText(player);
	await expect(card).toContainText('Pick 1 · Round 1');
	await expect(target).toHaveAttribute('aria-pressed', 'true');
	await expect(page.locator('button[aria-label^="Pick "][aria-pressed="true"]')).toHaveCount(1);
});

test('a failed season shows its own card, and Try again loads it', async ({ page }) => {
	let fail = true;
	await page.route(/\/api\/draft/, (r) => {
		const season = new URL(r.request().url()).searchParams.get('season');
		if (season === '2025' && fail) return r.fulfill({ status: 500, body: '{}' });
		return r.fulfill({
			contentType: 'application/json',
			body: JSON.stringify(season === '2025' ? f.season2025 : f.all)
		});
	});
	await page.goto('/draft');
	await page.getByLabel('Season').selectOption('2025');
	await expect(page.getByRole('heading', { name: "The 2025 draft didn't load" })).toBeVisible();
	await expect(page.getByText('No draft for')).toHaveCount(0);

	fail = false;
	await page.getByRole('button', { name: 'Try again' }).click();
	await expect(page.getByRole('region', { name: 'Selected pick' })).toBeVisible();
});

test('a slow earlier season never replaces the one picked last', async ({ page }) => {
	await page.route(/\/api\/draft/, async (r) => {
		const season = new URL(r.request().url()).searchParams.get('season');
		// 2024 answers late with the 2025 fixture relabeled, so a stale landing shows.
		if (season === '2024') await new Promise((done) => setTimeout(done, 1500));
		const body = season ? structuredClone(f.season2025) : f.all;
		if (season === '2024') body.data.draftBoard[0].playerName = 'Stale 2024 pick';
		return r.fulfill({ contentType: 'application/json', body: JSON.stringify(body) });
	});
	await page.goto('/draft');
	await page.getByLabel('Season').selectOption('2024');
	await page.getByLabel('Season').selectOption('2025');
	await expect(page.getByRole('region', { name: 'Selected pick' })).toBeVisible();
	await page.waitForTimeout(2000);
	await expect(page.getByRole('heading', { name: 'The 2025 draft', exact: true })).toBeVisible();
	await expect(page.getByText('Stale 2024 pick')).toHaveCount(0);
});

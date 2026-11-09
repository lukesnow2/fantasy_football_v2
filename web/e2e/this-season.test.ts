import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';

/**
 * This Season's two toggles (WP4 S4.11), at 1280, on the boards' data: the
 * live board's week 3 and the after-the-final board's 2025 season. The APIs
 * are stubbed from e2e/fixtures/this-season.json.
 */
const fixtures = JSON.parse(readFileSync(new URL('./fixtures/this-season.json', import.meta.url), 'utf8'));

async function stub(page: Page, set: 'live' | 'final') {
	const f = fixtures[set];
	const route = (pattern: RegExp, body: unknown) =>
		page.route(pattern, (r) => r.fulfill({ contentType: 'application/json', body: JSON.stringify(body) }));
	await route(/\/api\/leagues\/overview/, f.overview);
	await route(/\/api\/standings(\?|$)/, f.standings);
	await route(/\/api\/standings\/start-history/, { week: 3, firstSeason: 2005, lastSeason: 2025, rows: [] });
	await route(/\/api\/playoff-odds/, f.odds);
	await route(/\/api\/power-rankings/, f.power);
	await route(/\/api\/transactions/, f.transactions);
}

test.use({ viewport: { width: 1280, height: 900 } });

test('Record and Playoff finish swap the final table and its note', async ({ page }) => {
	await stub(page, 'final');
	await page.goto('/this-season');

	await expect(page.getByText('2025 champion')).toBeVisible();
	const card = page.locator('section', { has: page.getByRole('heading', { name: 'Final standings' }) });
	const record = card.getByRole('radio', { name: 'Record' });
	const finish = card.getByRole('radio', { name: 'Playoff finish' });

	await expect(record).toHaveAttribute('aria-checked', 'true');
	await expect(card.getByText('Regular season record, with points for as the tiebreaker.')).toBeVisible();
	const table = card.getByRole('table', { name: 'Final standings by record' });
	await expect(table.getByRole('row').nth(1)).toContainText('Omar');
	await expect(card.getByText('Playoff line')).toBeVisible();

	await finish.click();
	await expect(finish).toHaveAttribute('aria-checked', 'true');
	await expect(card.getByText('Where everyone finished once the bracket played out.')).toBeVisible();
	await expect(table).toBeHidden();
	await expect(card.getByRole('heading', { name: 'Championship' })).toBeVisible();
	await expect(card.locator('li').first()).toContainText('Gabe the Younger');

	await record.click();
	await expect(card.getByText('Regular season record, with points for as the tiebreaker.')).toBeVisible();
});

test('Playoffs, Bye and Title re-sort the odds, with the cut only on Playoffs', async ({ page }) => {
	await stub(page, 'live');
	await page.goto('/this-season');

	const card = page.locator('section', { has: page.getByRole('heading', { name: 'Playoff Odds' }) });
	const rows = card.locator('button[aria-expanded]');
	const names = async () =>
		(await rows.allTextContents()).map((t) => t.trim().split(/\s\d/)[0].trim());

	await expect(card.getByRole('radio', { name: 'Playoffs' })).toHaveAttribute('aria-checked', 'true');
	await expect(rows.first()).toHaveAttribute('aria-expanded', 'true');
	expect((await names()).slice(0, 3)).toEqual(['Craig', 'Israel', 'Nick']);
	await expect(card.getByText('Projected cut')).toBeVisible();

	await card.getByRole('radio', { name: 'Bye' }).click();
	expect((await names()).slice(0, 3)).toEqual(['Israel', 'Craig', 'Nick']);
	await expect(card.getByText('Projected cut')).toHaveCount(0);
	// The same team stays open across the switch.
	await expect(card.locator('button[aria-expanded="true"]')).toContainText('Craig');

	await card.getByRole('radio', { name: 'Title' }).click();
	expect((await names())[0]).toBe('Craig');
	await expect(card.getByText('Projected cut')).toHaveCount(0);

	await card.getByRole('radio', { name: 'Playoffs' }).click();
	await expect(card.getByText('Projected cut')).toBeVisible();
});

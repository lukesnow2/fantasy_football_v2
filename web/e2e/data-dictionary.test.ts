import { readFileSync } from 'node:fs';
import { expect, test } from '@playwright/test';

/**
 * The Data Dictionary's anchors (WP11 S11.11): /api/meta-data is stubbed from
 * e2e/fixtures/data-dictionary.json, the dev database's 2026 metrics.
 */
const f = JSON.parse(readFileSync(new URL('./fixtures/data-dictionary.json', import.meta.url), 'utf8'));

test.use({ viewport: { width: 1280, height: 900 } });

test.beforeEach(async ({ page }) => {
	await page.route(/\/api\/meta-data/, (r) =>
		r.fulfill({ contentType: 'application/json', body: JSON.stringify(f) })
	);
});

test('a #metric link opens that entry with its heading in view and focused', async ({ page }) => {
	await page.goto('/data-dictionary#luck_factor');
	const entry = page.locator('[id="luck_factor"]');
	const heading = entry.getByRole('heading', { name: 'Luck Factor' });
	await expect(entry.getByRole('button', { name: 'Hide details' })).toHaveAttribute('aria-expanded', 'true');
	await expect(heading).toBeInViewport();
	await expect(heading).toBeFocused();
	await expect(entry.getByText('Calculation', { exact: true })).toBeVisible();
});

test('a hash that names nothing leaves every entry closed', async ({ page }) => {
	await page.goto('/data-dictionary#not_a_metric');
	await expect(page.getByRole('button', { name: 'Show details' }).first()).toBeVisible();
	await expect(page.getByRole('button', { name: 'Hide details' })).toHaveCount(0);
});

test('a later hash change clears the filters and opens the entry', async ({ page }) => {
	await page.goto('/data-dictionary');
	await page.getByRole('searchbox', { name: 'Search' }).fill('trade');
	await expect(page.locator('[id="luck_factor"]')).toHaveCount(0);
	await page.evaluate(() => (window.location.hash = 'luck_factor'));
	await expect(page.getByRole('searchbox', { name: 'Search' })).toHaveValue('');
	await expect(page.locator('[id="luck_factor"]').getByRole('heading', { name: 'Luck Factor' })).toBeFocused();
});

test('a malformed hash opens nothing and throws nothing', async ({ page }) => {
	const errors: string[] = [];
	page.on('pageerror', (e) => errors.push(e.message));
	await page.goto('/data-dictionary#%E0');
	await expect(page.getByRole('button', { name: 'Show details' }).first()).toBeVisible();
	await expect(page.getByRole('button', { name: 'Hide details' })).toHaveCount(0);
	expect(errors).toEqual([]);
});

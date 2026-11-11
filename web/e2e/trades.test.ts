import { readFileSync } from 'node:fs';
import { expect, test } from '@playwright/test';

/**
 * The Trade Center's season select at 1280 (WP8), on dev-database data from
 * 2026 week 3: /api/trades is stubbed from e2e/fixtures/trades.json (all
 * seasons, trimmed to the six newest trades).
 */
const f = JSON.parse(readFileSync(new URL('./fixtures/trades.json', import.meta.url), 'utf8'));
const json = (body: unknown) => ({ contentType: 'application/json', body: JSON.stringify(body) });

/** The all-seasons response relabeled as one season, with its own count. */
function season(year: string, totalTrades: number) {
	const body = structuredClone(f);
	body.analytics.overview.totalTrades = totalTrades;
	body.meta.season = year;
	return body;
}

test.use({ viewport: { width: 1280, height: 900 } });

test('a slow earlier season never replaces the one picked last', async ({ page }) => {
	await page.route(/\/api\/trades/, async (r) => {
		const s = new URL(r.request().url()).searchParams.get('season');
		if (s === '2019') {
			await new Promise((done) => setTimeout(done, 1500));
			return r.fulfill(json(season('2019', 7)));
		}
		return r.fulfill(json(f));
	});
	await page.goto('/trades');
	await expect(page.getByRole('heading', { name: 'Most lopsided trades' })).toBeVisible();
	await page.getByLabel('Season').selectOption('2019');
	await page.getByLabel('Season').selectOption('all');
	await page.waitForTimeout(2000);
	const first = page.getByRole('group', { name: 'Trades in numbers' }).locator('.tile').first();
	await expect(first).toContainText('Trades, all time');
	await expect(first).toContainText('104');
	await expect(page.getByText('Trades in 2019')).toHaveCount(0);
});

test('a failed season reload shows no stale count above the failure card', async ({ page }) => {
	await page.route(/\/api\/trades/, (r) => {
		const s = new URL(r.request().url()).searchParams.get('season');
		return s === '2025' ? r.fulfill({ status: 500, body: '{}' }) : r.fulfill(json(f));
	});
	await page.goto('/trades');
	await expect(page.getByRole('heading', { name: 'Most lopsided trades' })).toBeVisible();
	await page.getByLabel('Season').selectOption('2025');
	await expect(page.getByRole('heading', { name: "Trades didn't load" })).toBeVisible();
	await expect(page.getByText(/All \d+ trades in 2025/)).toHaveCount(0);
});

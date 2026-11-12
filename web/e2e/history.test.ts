import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';

/**
 * League History's head to head link (WP10 S10.11), on dev-database data from
 * 2026 week 8: /api/overview and /api/head-to-head are stubbed from
 * e2e/fixtures/history.json.
 */
const f = JSON.parse(readFileSync(new URL('./fixtures/history.json', import.meta.url), 'utf8'));

async function stub(page: Page) {
	const json = (body: unknown) => ({ contentType: 'application/json', body: JSON.stringify(body) });
	await page.route(/\/api\/overview/, (r) => r.fulfill(json(f.overview)));
	await page.route(/\/api\/head-to-head/, (r) => r.fulfill(json(f.headToHead)));
}

test.use({ viewport: { width: 1280, height: 900 } });

test.beforeEach(async ({ page }) => {
	await stub(page);
});

test('a rivalry link opens that pair, and a new name goes into the URL', async ({ page }) => {
	await page.goto('/historical?tab=head-to-head&a=luke-s&b=trevor');
	await expect(
		page.getByRole('navigation', { name: 'History sections' }).getByRole('link', { name: 'Head to head' })
	).toHaveAttribute('aria-current', 'page');

	const faceoff = page.getByRole('region', { name: 'Face-off' });
	await expect(faceoff.getByRole('button', { name: 'Change manager, now Luke S' })).toBeVisible();
	await expect(faceoff.getByRole('button', { name: 'Change manager, now Trevor' })).toBeVisible();

	await faceoff.getByRole('button', { name: 'Change manager, now Trevor' }).click();
	const list = page.getByRole('listbox', { name: 'Pick a manager' });
	await expect(list).toBeVisible();
	await list.getByRole('option', { name: 'Israel' }).click();

	await expect(list).toBeHidden();
	await expect(faceoff.getByRole('button', { name: 'Change manager, now Israel' })).toBeVisible();
	await expect(page).toHaveURL(/[?&]b=Israel(&|$)/);
	await expect(page).toHaveURL(/[?&]a=Luke(%20|\+)S(&|$)/);
});

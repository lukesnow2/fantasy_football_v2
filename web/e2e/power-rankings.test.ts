import { readFileSync } from 'node:fs';
import { expect, test, type Page } from '@playwright/test';

/**
 * Power Rankings at 1280 (WP5 S5.9), on the board's data: 2026 through week
 * 3, with weeks 1 and 2 behind the week control. /api/power-rankings is
 * stubbed from e2e/fixtures/power-rankings.json.
 */
const fixtures = JSON.parse(
	readFileSync(new URL('./fixtures/power-rankings.json', import.meta.url), 'utf8')
);

async function stub(page: Page) {
	await page.route(/\/api\/power-rankings/, (route) => {
		const week = new URL(route.request().url()).searchParams.get('week');
		const body = week === '1' ? fixtures.week1 : week === '2' ? fixtures.week2 : fixtures.board;
		return route.fulfill({ contentType: 'application/json', body: JSON.stringify(body) });
	});
}

test.use({ viewport: { width: 1280, height: 900 } });

test.beforeEach(async ({ page }) => {
	await stub(page);
	await page.goto('/power-rankings');
	await expect(page.getByText('Rankings · 2026 · through week 3')).toBeVisible();
});

test('the table has the nine headers in order', async ({ page }) => {
	const headers = page.getByRole('table').getByRole('columnheader');
	// innerText, so the closed StatTip cards inside the headers don't count.
	// It reads the headers uppercased, as they're drawn.
	const text = (await headers.allInnerTexts()).map((t) => t.replace(/[ⓘⒾ]/g, '').trim());
	const labels = [
		'#',
		'Move',
		'Manager',
		'W-L',
		'Power score',
		'Pts rk',
		'Pyth W',
		'Luck',
		'Playoffs'
	];
	expect(text).toEqual(labels.map((label) => label.toUpperCase()));
});

test('picking another week changes the kicker and the table', async ({ page }) => {
	const table = page.getByRole('table');
	await expect(table.getByRole('row').nth(4)).toContainText('Trevor');

	const week1 = page.getByRole('radiogroup', { name: 'Week' }).getByRole('radio', { name: 'Wk 1' });
	await week1.click();
	await expect(page.getByText('Rankings · 2026 · through week 1')).toBeVisible();
	await expect(week1).toHaveAttribute('aria-checked', 'true');
	// Week 1: Craig leads, and nobody has moved yet.
	await expect(table.getByRole('row').nth(1)).toContainText('Craig');
	await expect(page.getByText('No one moved up')).toBeVisible();
	// The footnote only names the last ranked week when it's selected.
	await expect(page.getByText('Week 3 is the last ranked week of 2026.')).toHaveCount(0);
});

test('Escape closes the Luck StatTip after a click opens it', async ({ page }) => {
	const luck = page.getByRole('columnheader').getByRole('button', { name: /Luck/ });
	await expect(luck).toHaveAttribute('aria-expanded', 'false');
	await luck.click();
	await expect(luck).toHaveAttribute('aria-expanded', 'true');
	await expect(page.getByRole('tooltip').filter({ visible: true })).toBeVisible();
	await page.keyboard.press('Escape');
	await expect(luck).toHaveAttribute('aria-expanded', 'false');
	await expect(page.getByRole('tooltip').filter({ visible: true })).toHaveCount(0);
});

test('hovering a legend button dims the other lines', async ({ page }) => {
	const lines = page.locator('.chart svg path');
	await expect(lines).toHaveCount(10);
	const legend = page.getByRole('group', { name: 'Legend' });
	await legend.getByRole('button', { name: /Gabe Flores/ }).hover();

	const opacities = await lines.evaluateAll((paths) =>
		paths.map((p) => [p.getAttribute('stroke-width'), p.getAttribute('stroke-opacity')])
	);
	// Gabe Flores is third in last week's order, so the third line.
	expect(opacities[2]).toEqual(['6', '0.9']);
	expect(opacities.filter((o, i) => i !== 2).every(([, op]) => op === '0.18')).toBe(true);

	await page.mouse.move(0, 0);
	await expect(lines.nth(0)).toHaveAttribute('stroke-opacity', '0.9');
});

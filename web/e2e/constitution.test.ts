import { expect, test } from '@playwright/test';

/**
 * The Constitution's reading layout (WP11 S11.11). The page reads the
 * document from the database, so these run against a seeded constitution
 * (npm run seed:constitution) and skip when there isn't one.
 */
test.use({ viewport: { width: 1280, height: 900 } });

const NUMERALS = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'A1'];

test('Contents reads I to IX then A1, and each link lands on its article', async ({ page }) => {
	await page.goto('/constitution');
	const contents = page.getByRole('navigation', { name: 'Articles' });
	test.skip((await contents.count()) === 0, 'The constitution has not been seeded.');

	const links = contents.getByRole('link');
	await expect(links).toHaveCount(NUMERALS.length);
	for (const [i, numeral] of NUMERALS.entries()) {
		await expect(links.nth(i).locator('span').first()).toHaveText(numeral);
	}
	await expect(links.first()).toHaveAttribute('aria-current', 'location');

	for (const [i] of NUMERALS.entries()) {
		const link = links.nth(i);
		const target = (await link.getAttribute('href'))!.slice(1);
		await link.click();
		await expect(page).toHaveURL(new RegExp(`#${target}$`));
		const top = await page.locator(`[id="${target}"]`).evaluate((el) => el.getBoundingClientRect().top);
		// At the top of the viewport, or as close as the page's end allows.
		const atEnd = await page.evaluate(
			() => Math.ceil(window.scrollY + window.innerHeight) >= document.documentElement.scrollHeight
		);
		if (!atEnd) expect(Math.abs(top - 16)).toBeLessThanOrEqual(2);
		else expect(top).toBeLessThan(900);
	}
});

test('signed out, Propose a change goes to sign in and back', async ({ page }) => {
	await page.goto('/constitution');
	const propose = page.getByRole('link', { name: 'Propose a change' });
	await expect(propose).toHaveAttribute('href', '/login?redirect=/constitution');
	await propose.click();
	await expect(page).toHaveURL(/\/login\?redirect=\/constitution$/);
});

import { expect, test } from '@playwright/test';

/**
 * The members-only pages (WP12, S12.11). Signed out, every one of them sends
 * you to sign in and remembers where you were going; the sign-in pages stand
 * alone, with no nav. There's no signed-in session helper in this suite, so
 * the Thread card is checked by hand in the PR.
 */

test.describe('signed out', () => {
	test.use({ viewport: { width: 1280, height: 900 } });

	for (const path of ['/chat', '/settings', '/admin/members']) {
		test(`${path} redirects to sign in and back`, async ({ page }) => {
			await page.goto(path);
			await expect(page).toHaveURL((url) => url.pathname === '/login');
			expect(new URL(page.url()).searchParams.get('redirect')).toBe(path);
		});
	}

	test('/login shows the sign-in card with no nav links', async ({ page }) => {
		await page.goto('/login');
		await expect(
			page.getByRole('heading', { level: 1, name: 'Sign in to The League' })
		).toBeVisible();
		await expect(page.getByRole('navigation')).toHaveCount(0);
		await expect(page.getByRole('contentinfo')).toHaveCount(0);
		await expect(page.getByRole('button', { name: 'Email me a sign-in link' })).toBeVisible();
	});

	test('/login/verify with no token says so and sends you back to /login', async ({ page }) => {
		await page.goto('/login/verify');
		await expect(
			page.getByRole('heading', { level: 1, name: 'No sign-in link found' })
		).toBeVisible();
		await page.getByRole('link', { name: 'Send me a new link' }).click();
		await expect(page).toHaveURL((url) => url.pathname === '/login');
	});
});

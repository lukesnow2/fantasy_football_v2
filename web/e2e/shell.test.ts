import { expect, test } from '@playwright/test';

/**
 * The site shell (WP2, S2.6): the header band's menus, the active page and the
 * footer at 1280, and the phone menu at 390.
 */

test.describe('at 1280', () => {
	test.use({ viewport: { width: 1280, height: 900 } });

	test('Rankings opens from the keyboard and Escape hands focus back', async ({ page }) => {
		await page.goto('/this-season');
		const trigger = page.getByRole('button', { name: 'Rankings' });
		await trigger.focus();
		await page.keyboard.press('Enter');

		const menu = page.getByRole('menu', { name: 'Rankings' });
		await expect(menu).toBeVisible();
		await expect(trigger).toHaveAttribute('aria-expanded', 'true');
		await expect(menu.getByRole('menuitem', { name: /Power Rankings/ })).toBeFocused();

		await page.keyboard.press('ArrowDown');
		await page.keyboard.press('ArrowDown');
		await expect(menu.getByRole('menuitem', { name: /League History/ })).toBeFocused();

		await page.keyboard.press('Escape');
		await expect(menu).toBeHidden();
		await expect(trigger).toBeFocused();
	});

	test("a manager's profile marks Managers as the current page", async ({ page }) => {
		await page.goto('/managers/Trevor');
		const bar = page.locator('header').getByRole('navigation', { name: 'Main' });
		await expect(bar.getByRole('link', { name: 'Managers' })).toHaveAttribute(
			'aria-current',
			'page'
		);
		await expect(bar.getByRole('link', { name: 'This Season' })).not.toHaveAttribute(
			'aria-current',
			'page'
		);
	});

	test('the footer reads the board copy', async ({ page }) => {
		await page.goto('/this-season');
		await expect(page.getByRole('contentinfo')).toContainText(
			'Est. 2005 · Data from Yahoo Fantasy'
		);
	});
});

test.describe('at 390', () => {
	test.use({ viewport: { width: 390, height: 844 } });

	test('the menu button opens the panel and Escape closes it', async ({ page }) => {
		await page.goto('/this-season');
		await page.getByRole('button', { name: 'Open menu' }).click();

		const panel = page.locator('#site-menu');
		await expect(panel).toBeVisible();
		await expect(page.getByRole('button', { name: 'Close menu' })).toBeFocused();

		await page.keyboard.press('Escape');
		await expect(panel).toBeHidden();
		await expect(page.getByRole('button', { name: 'Open menu' })).toBeFocused();
		await expect(page.getByRole('main')).toHaveCount(1);
	});
});

/**
 * The shell signed in (WP2, S2.3), rendered in a real browser with the layout's
 * data mocked, since a dev session needs a seeded member. The e2e suite in
 * web/e2e/shell.test.ts covers the signed-out shell against the running app.
 */
import { page, userEvent } from '@vitest/browser/context';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { render } from 'vitest-browser-svelte';
import { createRawSnippet } from 'svelte';

const state = vi.hoisted(() => ({ pathname: '/settings' }));

vi.mock('$app/state', () => ({
	page: {
		get url() {
			return new URL(`http://localhost${state.pathname}`);
		}
	}
}));
vi.mock('$app/navigation', () => ({ afterNavigate: () => {} }));
vi.mock('$app/forms', () => ({ enhance: () => ({ destroy() {} }) }));
vi.mock('@vercel/analytics/sveltekit', () => ({ injectAnalytics: () => {} }));

const Layout = (await import('./+layout.svelte')).default;

const children = createRawSnippet(() => ({ render: () => '<p>Page</p>' }));

function signedIn(role = 'manager') {
	return {
		data: {
			user: { id: 'u1', username: 'luke' },
			member: { role },
			authenticatedManager: { managerName: 'Luke S', displayName: 'Luke S' }
		},
		children
	} as never;
}

/** x, y, width, height, rounded to a tenth of a pixel. */
function box(el: Element) {
	const b = el.getBoundingClientRect();
	return [b.x, b.y, b.width, b.height].map((n) => Math.round(n * 10) / 10);
}

/**
 * The box matches the board's measured box to within `slack` px per value.
 * Text sets a fraction of a pixel differently on Linux (CI) and macOS, so a
 * box sized by text lands within a pixel of the board, and one stacked from
 * several lines of text within a pixel per line. Sizes fixed in CSS stay exact.
 */
function expectBox(el: Element, expected: number[], slack: number | number[] = 1) {
	const actual = box(el);
	const tol = Array.isArray(slack) ? slack : expected.map(() => slack);
	const off = actual.some((v, i) => Math.abs(v - expected[i]) > tol[i]);
	expect(off, `box ${JSON.stringify(actual)}, board ${JSON.stringify(expected)}`).toBe(false);
}

beforeAll(async () => {
	// The boards' fonts, so widths match what the app draws.
	const link = document.createElement('link');
	link.rel = 'stylesheet';
	link.href =
		'https://fonts.googleapis.com/css2?family=Zilla+Slab:wght@500;600;700&family=Instrument+Sans:wght@400;500;600;700&display=swap';
	document.head.append(link);
	await new Promise((resolve) => link.addEventListener('load', resolve, { once: true }));
	await Promise.all(
		['400', '500', '600', '700'].map((w) => document.fonts.load(`${w} 15px "Instrument Sans"`))
	);
	await document.fonts.load('700 26px "Zilla Slab"');
});

beforeEach(async () => {
	state.pathname = '/settings';
	await page.viewport(1280, 900);
});

describe('the signed-in pill and account menu at 1280', () => {
	it('draws the pill as the Chat, Bet Board and Settings boards do', async () => {
		render(Layout, signedIn());
		const pill = page.getByRole('button', { name: 'Luke S' });
		await expect.element(pill).toHaveAttribute('aria-haspopup', 'menu');
		await expect.element(pill).toHaveAttribute('aria-expanded', 'false');
		const el = pill.element();
		expect(el.textContent?.trim()).toBe('Luke S ▾');
		// The board: 114 by 42 at x 1102, y 13, with a 30px photo 6px in.
		expectBox(el, [1102, 13, 114, 42]);
		expectBox(el.querySelector('.avatar')!, [1108, 19, 30, 30], [1, 0, 0, 0]);
	});

	it('opens the account menu as the Settings board draws it', async () => {
		render(Layout, signedIn());
		const pill = page.getByRole('button', { name: 'Luke S' });
		await pill.click();
		await expect.element(pill).toHaveAttribute('aria-expanded', 'true');
		expect(pill.element().textContent?.trim()).toBe('Luke S ▴');
		expect(getComputedStyle(pill.element()).boxShadow).toBe(
			'rgb(240, 160, 110) 0px 0px 0px 2px inset'
		);

		const menu = page.getByRole('menu', { name: 'Account' });
		await expect.element(menu).toBeVisible();
		// 234 wide is fixed in CSS; the height stacks five lines of text.
		expectBox(menu.element(), [982, 76, 234, 195], [0, 0, 0, 5]);
		expect(menu.element().querySelector('.signed-in')!.textContent).toContain('Signed in as');

		const items = page.getByRole('menuitem');
		expect(items.elements().map((e) => e.textContent?.trim())).toEqual([
			'Your profile',
			'Settings',
			'Sign out'
		]);
		// Each item sits under the "Signed in as" block's two lines and the
		// items above it, so its y gets a pixel per line of text above it.
		const boardItems = [
			[989, 141.5, 220, 40.5],
			[989, 182, 220, 40.5],
			[989, 222.5, 220, 41.5]
		];
		items.elements().forEach((e, i) => expectBox(e, boardItems[i], [0, 2 + i, 0, 1]));
		// Settings is the page you're on: weight 700 on field-tint.
		const settings = page.getByRole('menuitem', { name: 'Settings' }).element();
		expect(settings.getAttribute('aria-current')).toBe('page');
		expect(getComputedStyle(settings).fontWeight).toBe('700');
		expect(getComputedStyle(settings).backgroundColor).toBe('rgb(227, 239, 231)');
	});

	it('keeps the Commissioner link between Settings and Sign out', async () => {
		render(Layout, signedIn('commissioner'));
		await page.getByRole('button', { name: 'Luke S' }).click();
		expect(
			page
				.getByRole('menuitem')
				.elements()
				.map((e) => e.textContent?.trim())
		).toEqual(['Your profile', 'Settings', 'Commissioner', 'Sign out']);
	});

	it('moves with the arrow keys and closes on Escape, back on the pill', async () => {
		render(Layout, signedIn());
		const pill = page.getByRole('button', { name: 'Luke S' });
		(pill.element() as HTMLElement).focus();
		await userEvent.keyboard('{Enter}');
		await expect.element(page.getByRole('menuitem', { name: 'Your profile' })).toHaveFocus();
		await userEvent.keyboard('{ArrowUp}');
		await expect.element(page.getByRole('menuitem', { name: 'Sign out' })).toHaveFocus();
		await userEvent.keyboard('{ArrowDown}');
		await expect.element(page.getByRole('menuitem', { name: 'Your profile' })).toHaveFocus();
		await userEvent.keyboard('{Escape}');
		await expect.element(page.getByRole('menu', { name: 'Account' })).not.toBeInTheDocument();
		await expect.element(pill).toHaveFocus();
	});

	it('closes on a click outside', async () => {
		render(Layout, signedIn());
		await page.getByRole('button', { name: 'Luke S' }).click();
		await expect.element(page.getByRole('menu', { name: 'Account' })).toBeVisible();
		await page.getByText('Page').click();
		await expect.element(page.getByRole('menu', { name: 'Account' })).not.toBeInTheDocument();
	});
});

describe('the phone panel signed in at 390', () => {
	it('ends with the photo and the three account targets', async () => {
		await page.viewport(390, 844);
		render(Layout, signedIn());
		await page.getByRole('button', { name: 'Open menu' }).click();
		await expect.element(page.getByRole('button', { name: 'Close menu' })).toHaveFocus();
		const row = document.querySelector('.phone-account')!;
		expect(row.textContent?.replace(/\s+/g, ' ').trim()).toBe('Your profile · Settings · Sign out');
		for (const target of row.querySelectorAll('a, button')) {
			expect(target.getBoundingClientRect().height).toBeGreaterThanOrEqual(44);
		}
		expect(box(row.querySelector('.avatar')!)[2]).toBe(28);
		// Each word where the board's single line puts it, " · " and all.
		const words = [...row.querySelectorAll('a, button')].map((e) => e.getBoundingClientRect().x);
		expect(words[0]).toBe(66);
		// The next two follow the text before them, so they get a pixel each.
		expect(Math.abs(words[1] - 161.1)).toBeLessThanOrEqual(1);
		expect(Math.abs(words[2] - 233.5)).toBeLessThanOrEqual(2);
	});
});

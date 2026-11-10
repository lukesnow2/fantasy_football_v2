import { page, userEvent } from '@vitest/browser/context';
import { afterEach, describe, expect, it, onTestFinished, vi } from 'vitest';
import { render } from 'vitest-browser-svelte';
import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';
import Segmented from './Segmented.svelte';
import StatTip from './StatTip.svelte';
import Movement from './Movement.svelte';
import ButtonHarness from './ButtonHarness.test.svelte';
import Card from './Card.svelte';
import CardHarness from './CardHarness.test.svelte';
import DataTableHarness from './DataTableHarness.test.svelte';
import { resetStatTipData } from './stat-tip-data';

const LUCK = {
	metricId: 'luck_factor',
	metricName: 'Luck Factor',
	categoryName: 'Head to head',
	shortDescription: 'Measures whether a record was better or worse than its points suggest.',
	calculationFormula: 'Regular-season wins - Pythagorean wins',
	interpretationGuide: 'One season spread here is about 1.1 wins.'
};

afterEach(() => {
	resetStatTipData();
	metricDefinitionsStore.clearCache();
	vi.restoreAllMocks();
});

describe('Segmented', () => {
	const options = [
		{ value: 1, label: 'Wk 1' },
		{ value: 2, label: 'Wk 2' },
		{ value: 3, label: 'Wk 3' }
	];

	it('is a radiogroup with the chosen option checked', async () => {
		render(Segmented, { options, value: 3, label: 'Week' });
		await expect.element(page.getByRole('radiogroup', { name: 'Week' })).toBeInTheDocument();
		await expect
			.element(page.getByRole('radio', { name: 'Wk 3' }))
			.toHaveAttribute('aria-checked', 'true');
		await expect
			.element(page.getByRole('radio', { name: 'Wk 1' }))
			.toHaveAttribute('tabindex', '-1');
	});

	it('moves and chooses with the arrow keys, and emits the value', async () => {
		const onchange = vi.fn();
		render(Segmented, { options, value: 3, label: 'Week', onchange });
		(page.getByRole('radio', { name: 'Wk 3' }).element() as HTMLElement).focus();
		await userEvent.keyboard('{ArrowRight}');
		expect(onchange).toHaveBeenLastCalledWith(1);
		await expect
			.element(page.getByRole('radio', { name: 'Wk 1' }))
			.toHaveAttribute('aria-checked', 'true');
		expect(document.activeElement?.textContent?.trim()).toBe('Wk 1');
		await userEvent.keyboard('{End}');
		expect(onchange).toHaveBeenLastCalledWith(3);
	});

	it('emits the value on click', async () => {
		const onchange = vi.fn();
		render(Segmented, { options, value: 3, label: 'Week', onchange });
		await page.getByRole('radio', { name: 'Wk 2' }).click();
		expect(onchange).toHaveBeenCalledWith(2);
	});
});

describe('StatTip', () => {
	it('opens on click, describes the button, and closes with Escape', async () => {
		metricDefinitionsStore.setMetric('luck_factor', { metric: LUCK as never });
		render(StatTip, { metricId: 'luck_factor', label: 'Luck' });

		const button = page.getByRole('button', { name: /Luck/ });
		await expect.element(button).toHaveAttribute('aria-expanded', 'false');
		await button.click();
		await expect.element(button).toHaveAttribute('aria-expanded', 'true');

		const card = page.getByRole('tooltip');
		await expect.element(card).toBeVisible();
		await expect.element(card).toHaveTextContent('Luck Factor');
		await expect.element(card).toHaveTextContent('Calculation');
		await expect
			.element(page.getByRole('link', { name: 'Full definition in the Data Dictionary →' }))
			.toHaveAttribute('href', '/data-dictionary#luck_factor');
		const describedBy = button.element().getAttribute('aria-describedby');
		expect(describedBy).toBe(card.element().id);

		const cardElement = card.element() as HTMLElement;
		await userEvent.keyboard('{Escape}');
		await expect.element(button).toHaveAttribute('aria-expanded', 'false');
		expect(cardElement.hidden).toBe(true);
		expect(document.activeElement).toBe(button.element());
	});

	it('shows the new metric when metricId changes', async () => {
		metricDefinitionsStore.setMetric('luck_factor', { metric: LUCK as never });
		metricDefinitionsStore.setMetric('pythagorean_wins', {
			metric: { ...LUCK, metricId: 'pythagorean_wins', metricName: 'Pythagorean Wins' } as never
		});
		const view = render(StatTip, { metricId: 'luck_factor', label: 'Luck' });
		await view.rerender({ metricId: 'pythagorean_wins', label: 'Pyth W' });

		await page.getByRole('button', { name: /Pyth W/ }).click();
		await expect.element(page.getByRole('tooltip')).toHaveTextContent('Pythagorean Wins');
		await expect
			.element(page.getByRole('link', { name: 'Full definition in the Data Dictionary →' }))
			.toHaveAttribute('href', '/data-dictionary#pythagorean_wins');
	});

	it('closes on a click outside', async () => {
		metricDefinitionsStore.setMetric('luck_factor', { metric: LUCK as never });
		render(StatTip, { metricId: 'luck_factor', label: 'Luck' });
		const button = page.getByRole('button', { name: /Luck/ });
		await button.click();
		await expect.element(button).toHaveAttribute('aria-expanded', 'true');
		document.body.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true }));
		await expect.element(button).toHaveAttribute('aria-expanded', 'false');
	});

	it('stays inside a 390 wide viewport', async () => {
		const before = { width: window.innerWidth, height: window.innerHeight };
		onTestFinished(() => page.viewport(before.width, before.height));
		await page.viewport(390, 800);
		metricDefinitionsStore.setMetric('luck_factor', { metric: LUCK as never });
		const { container } = render(StatTip, { metricId: 'luck_factor', label: 'Luck' });
		(container as HTMLElement).style.cssText = 'display:block;text-align:right;padding:0 4px';
		await page.getByRole('button', { name: /Luck/ }).click();
		const card = page.getByRole('tooltip');
		await expect.element(card).toBeVisible();
		await vi.waitFor(() => {
			const box = card.element().getBoundingClientRect();
			expect(box.left).toBeGreaterThanOrEqual(16);
			expect(box.right).toBeLessThanOrEqual(390 - 16);
		});
	});

	it('shows the label with no ⓘ and no button when the definition is missing', async () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		vi.spyOn(window, 'fetch').mockResolvedValue(
			new Response(JSON.stringify({ error: 'Metric not found' }), { status: 404 })
		);
		const { container } = render(StatTip, { metricId: 'not_a_metric', label: 'Mystery' });
		await vi.waitFor(() => expect(container.querySelector('button')).toBeNull());
		expect(container.textContent).toContain('Mystery');
		expect(container.textContent).not.toContain('ⓘ');
		expect(container.querySelector('[role="tooltip"]')).toBeNull();
		expect(warn).toHaveBeenCalledTimes(1);
	});
});

describe('Movement', () => {
	it('prints up, down and Same', async () => {
		const up = render(Movement, { change: 4 });
		expect(up.container.textContent?.replace(/\s+/g, ' ').trim()).toBe('▲Up 4');
		const same = render(Movement, { change: 0 });
		expect(same.container.textContent?.trim()).toBe('Same');
	});

	it('prints None for a missing or non-finite change', async () => {
		expect(render(Movement, { change: null }).container.textContent?.trim()).toBe('None');
		expect(render(Movement, { change: NaN }).container.textContent?.trim()).toBe('None');
	});
});

describe('Button', () => {
	it('adds a caller class to its own, and a disabled link neither navigates nor acts', async () => {
		const onclick = vi.fn();
		const { container } = render(ButtonHarness, { onclick });
		const links = container.querySelectorAll('a');
		expect(links[0].className).toMatch(/\bbtn\b.*\bprimary\b.*\bmt-4\b/);
		expect(links[0].getAttribute('href')).toBe('/power-rankings');
		expect(links[1].hasAttribute('href')).toBe(false);
		expect(links[1].getAttribute('aria-disabled')).toBe('true');
		expect(links[1].getAttribute('role')).toBe('link');
		links[1].click();
		expect(onclick).not.toHaveBeenCalled();
	});
});

describe('Card', () => {
	it('is a region named by its title', async () => {
		render(Card, { title: 'Standings' });
		await expect.element(page.getByRole('region', { name: 'Standings' })).toBeInTheDocument();
	});

	it('sizes the action slot to its content, not to the card line', async () => {
		const { container } = render(CardHarness);
		const action = container.querySelector('.action') as HTMLElement;
		const caption = action.firstElementChild as HTMLElement;
		expect(action.getBoundingClientRect().height).toBe(caption.getBoundingClientRect().height);
	});
});

describe('DataTable', () => {
	it('draws a divider row from dividerCell, spanning every column', async () => {
		const { container } = render(DataTableHarness);
		const dividers = [...container.querySelectorAll('tr.divider')];
		expect(dividers.map((d) => d.textContent?.trim())).toEqual([
			'Hall of Fame Elite opens at Trevor',
			'Hall of Fame opens at Nick'
		]);
		expect(dividers[0].querySelector('td')?.getAttribute('colspan')).toBe('2');
		expect(container.querySelector('.divider-line')).toBeNull();
	});

	it('holds a fixedHeader column to its width when the header text is wider', async () => {
		const { container } = render(DataTableHarness);
		const th = container.querySelectorAll('th')[1] as HTMLElement;
		const label = th.querySelector('span') as HTMLElement;
		expect(label.getBoundingClientRect().width).toBe(40);
		expect(label.scrollWidth).toBeGreaterThan(40);
	});
});

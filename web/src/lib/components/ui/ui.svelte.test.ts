import { page, userEvent } from '@vitest/browser/context';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { render } from 'vitest-browser-svelte';
import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';
import Segmented from './Segmented.svelte';
import StatTip from './StatTip.svelte';
import Movement from './Movement.svelte';
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
});

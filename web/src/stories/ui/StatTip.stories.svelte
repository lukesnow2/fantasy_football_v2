<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import StatTip from '$lib/components/ui/StatTip.svelte';
	import StatTipCard from '$lib/components/ui/StatTipCard.svelte';
	import { LUCK_FACTOR, seedDefinitions } from './fixtures';

	seedDefinitions();

	const { Story } = defineMeta({ title: 'UI/StatTip', component: StatTip, tags: ['autodocs'] });
</script>

<script>
	// The phone story's table edge, for the card to span.
	let spanEl = $state();
</script>

<Story name="Closed">
	{#snippet template()}
		<span class="header-label"><StatTip metricId="luck_factor" label="Luck" /></span>
	{/snippet}
</Story>

<Story name="Open">
	{#snippet template()}
		<div style="min-height: 320px">
			<span class="header-label"><StatTip metricId="luck_factor" label="Luck" open /></span>
		</div>
	{/snippet}
</Story>

<Story name="Card on its own">
	{#snippet template()}
		<div style="width: 360px"><StatTipCard definition={LUCK_FACTOR} /></div>
	{/snippet}
</Story>

<Story name="Labels in a row">
	{#snippet template()}
		<div class="header-label" style="display: flex; flex-wrap: wrap; gap: 8px 18px">
			<StatTip metricId="pythagorean_wins" label="Pyth W" />
			<StatTip metricId="power_score" label="Power score" />
			<StatTip metricId="hall_of_fame_index" label="HoF index" />
			<StatTip metricId="draft_grade" label="Grade" />
		</div>
	{/snippet}
</Story>

<Story name="Missing definition">
	{#snippet template()}
		<p style="font-size: 15px">
			<StatTip metricId="not_in_the_dictionary" label="Mystery stat" /> shows its label and no ⓘ.
		</p>
	{/snippet}
</Story>

<Story name="Open as Power Rankings draws Luck">
	{#snippet template()}
		<div style="min-height: 360px; padding-left: 320px">
			<span class="header-label"
				><StatTip metricId="luck_factor" label="Luck" endOffset={40} topOffset={26} open /></span
			>
		</div>
	{/snippet}
</Story>

<Story name="Spanning a table on phones">
	{#snippet template()}
		<div style="max-width: 328px; min-height: 380px">
			<div bind:this={spanEl} style="border-bottom: 2px solid var(--color-ink); text-align: right">
				<span class="header-label"
					><StatTip metricId="luck_factor" label="Luck" spanOf={spanEl} phoneTarget open /></span
				>
			</div>
		</div>
	{/snippet}
</Story>

<Story name="Open near the right edge">
	{#snippet template()}
		<div style="min-height: 320px; text-align: right">
			<span class="header-label"><StatTip metricId="luck_factor" label="Luck" open /></span>
		</div>
	{/snippet}
</Story>

<Story name="Inline tap area on a phone">
	{#snippet template()}
		<div
			style="display: flex; justify-content: space-between; align-items: center; min-height: 44px; width: 320px; font-size: 15px; border-bottom: 1px solid var(--color-line-soft)"
		>
			<span style="color: var(--color-ink-2)"
				><StatTip metricId="luck_factor" label="Luck" phoneTap="inline" /></span
			>
			<b>+0.39</b>
		</div>
	{/snippet}
</Story>

<Story name="As a chip (Data Dictionary)">
	{#snippet template()}
		<p style="font-size: 14px">
			Hover the ⓘ next to any stat, like <StatTip metricId="luck_factor" label="Luck Factor" chip />, to get the
			same definition you'll find on this page.
		</p>
	{/snippet}
</Story>

<style>
	.header-label {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-ink-2);
	}
</style>

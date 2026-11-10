<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Segmented from '$lib/components/ui/Segmented.svelte';

	const { Story } = defineMeta({ title: 'UI/Segmented', component: Segmented, tags: ['autodocs'] });

	const weeks = [1, 2, 3].map((week) => ({ value: week, label: `Wk ${week}` }));
	const seasons = [2023, 2024, 2025, 2026].map((season) => ({
		value: season,
		label: String(season)
	}));
</script>

<script>
	let week = $state(3);
	let season = $state(2026);
</script>

<Story name="Week selector">
	{#snippet template()}
		<Segmented label="Week" options={weeks} bind:value={week} />
		<p style="margin-top: 12px; font-size: 13px; color: var(--color-muted)">Chosen: Wk {week}</p>
	{/snippet}
</Story>

<Story name="Season selector">
	{#snippet template()}
		<Segmented label="Season" options={seasons} bind:value={season} />
	{/snippet}
</Story>

<Story name="Nothing chosen">
	{#snippet template()}
		<Segmented label="Week" options={weeks} value={0} />
	{/snippet}
</Story>

<Story name="Toggle size (This Season)">
	{#snippet template()}
		<Segmented
			label="Odds of"
			size="toggle"
			options={[
				{ value: 'p', label: 'Playoffs' },
				{ value: 'b', label: 'Bye' },
				{ value: 't', label: 'Title' }
			]}
			value="p"
		/>
	{/snippet}
</Story>

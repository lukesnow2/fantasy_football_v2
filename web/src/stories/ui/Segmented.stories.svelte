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

<Story name="Week selector (Power Rankings: 38px, 44px below 640px)">
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

<Story name="Compact, with years (League History Seasons)">
	{#snippet template()}
		<Segmented
			label="Seasons"
			size="compact"
			wrap
			phoneColumns={2}
			options={[
				{ value: 'all', label: 'All' },
				{ value: 'early', label: 'Early', sub: '05 to 12' },
				{ value: 'middle', label: 'Middle', sub: '13 to 19' },
				{ value: 'modern', label: 'Modern', sub: '20 to 26' }
			]}
			value="all"
		/>
	{/snippet}
</Story>

<Story name="Bleed on phones (League History Measure)">
	{#snippet template()}
		<div style="max-width: 358px; padding: 16px; border: 1px solid var(--color-line); border-radius: 12px">
			<Segmented
				label="Measure"
				size="compact"
				padX={12}
				wrap
				bleed={16}
				options={[
					{ value: 'scoring', label: 'Scoring' },
					{ value: 'balance', label: 'Skill vs luck' },
					{ value: 'champion', label: 'Does the best team win?' },
					{ value: 'titles', label: 'Championships' }
				]}
				value="scoring"
			/>
		</div>
	{/snippet}
</Story>

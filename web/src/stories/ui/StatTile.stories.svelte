<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import StatTile from '$lib/components/ui/StatTile.svelte';
	import StatStrip from '$lib/components/ui/StatStrip.svelte';
	import { SEASON_STATS, seedDefinitions } from './fixtures';

	seedDefinitions();

	const { Story } = defineMeta({ title: 'UI/StatTile', component: StatTile, tags: ['autodocs'] });
</script>

<Story name="Stat cell">
	{#snippet template()}
		<div style="max-width: 300px">
			<StatTile label="Championships" value="7" sub="Trevor · 22 seasons" />
		</div>
	{/snippet}
</Story>

<Story name="Stat cell with a StatTip">
	{#snippet template()}
		<div style="max-width: 300px">
			<StatTile label="Luck" metricId="luck_factor" value="-0.2" sub="Craig · wins" />
		</div>
	{/snippet}
</Story>

<Story name="Stat strip">
	{#snippet template()}
		<div style="max-width: 1152px">
			<StatStrip label="This season at a glance">
				{#each SEASON_STATS as stat (stat.label)}
					<StatTile {...stat} />
				{/each}
			</StatStrip>
		</div>
	{/snippet}
</Story>

<Story name="Stat strip, three tiles">
	{#snippet template()}
		<div style="max-width: 1152px">
			<StatStrip>
				{#each SEASON_STATS.slice(0, 3) as stat (stat.label)}
					<StatTile {...stat} />
				{/each}
			</StatStrip>
		</div>
	{/snippet}
</Story>

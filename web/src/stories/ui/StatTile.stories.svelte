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

<Story name="Stat strip, value first">
	{#snippet template()}
		<div style="max-width: 1152px">
			<StatStrip label="The league in numbers" twoUpOnPhones>
				<StatTile valueFirst value="22" label="seasons, 2005 to 2026" />
				<StatTile valueFirst value="20" label="managers all-time" />
				<StatTile valueFirst value="10,612" label="transactions" />
				<StatTile valueFirst value="1,595" label="matchups played" />
			</StatStrip>
		</div>
	{/snippet}
</Story>

<Story name="Compact (This Season phone strip)">
	{#snippet template()}
		<div style="width: 180px">
			<StatTile label="Most points · Craig" value="465.5" compact />
		</div>
	{/snippet}
</Story>

<Story name="Avatar layout (profile Head to head)">
	{#snippet template()}
		<div style="max-width: 1152px">
			<StatStrip label="Head to head highlights" stackOnPhones>
				<StatTile
					layout="avatar"
					avatar="Troy Colvin"
					label="Best against"
					value="17-6"
					sub="Troy Colvin, .739"
					tone="field"
				/>
				<StatTile
					layout="avatar"
					avatar="Luke S"
					label="Toughest"
					value="19-21"
					sub="Luke S, who has won the last 7"
					tone="pennant"
				/>
				<StatTile
					layout="avatar"
					avatar="Omar"
					label="Dead even"
					value="14-14"
					sub="Omar, over 28 games"
					tone="ink"
				/>
				<StatTile
					layout="avatar"
					label="Title games"
					value={null}
					sub="No title games yet"
					tone="pennant-dark"
				/>
			</StatStrip>
		</div>
	{/snippet}
</Story>

<Story name="Stat strip, small (Hall of Fame)">
	{#snippet template()}
		<div style="max-width: 1152px">
			<StatStrip
				label="The Hall of Fame in numbers"
				minColumn={170}
				twoUpOnPhones
				phoneRowRules="all"
			>
				<StatTile small value="12" label="managers ranked" />
				<StatTile small value="1.8" label="titles per manager" />
				<StatTile small value="50.4%" label="average win rate" />
				<StatTile small value="15.3" label="seasons per manager" />
				<StatTile small value="0.352" label="average HoF index" metricId="hall_of_fame_index" />
			</StatStrip>
		</div>
	{/snippet}
</Story>

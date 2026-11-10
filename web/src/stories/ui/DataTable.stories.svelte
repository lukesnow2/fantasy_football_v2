<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import DataTable from '$lib/components/ui/DataTable.svelte';
	import Avatar from '$lib/components/ui/Avatar.svelte';
	import OddsBar from '$lib/components/ui/OddsBar.svelte';
	import Movement from '$lib/components/ui/Movement.svelte';
	import StatTip from '$lib/components/ui/StatTip.svelte';
	import { STANDINGS, seedDefinitions } from './fixtures';

	seedDefinitions();

	const { Story } = defineMeta({ title: 'UI/DataTable', component: DataTable, tags: ['autodocs'] });

	// This Season's standings columns.
	const standings = [
		{ key: 'rank', label: '#', width: '32px' },
		{ key: 'name', label: 'Manager' },
		{ key: 'wl', label: 'W-L', numeric: true, width: '56px' },
		{ key: 'pf', label: 'PF', numeric: true, width: '72px' },
		{ key: 'pa', label: 'PA', numeric: true, width: '72px' },
		{ key: 'odds', label: 'Playoff odds', width: '260px' },
		{ key: 'move', label: 'Wk Δ', numeric: true, width: '60px' }
	];

	// The System board's table row.
	const system = [
		{ key: 'rank', label: '#', width: '24px' },
		{ key: 'name', label: 'Manager' },
		{ key: 'record', label: 'W-L', numeric: true, width: '60px' },
		{ key: 'pct', label: 'Win %', numeric: true, width: '44px' }
	];
	const systemRows = [{ rank: 1, name: 'Trevor', record: '187-145', pct: '.563' }];

	const luckColumns = [
		{ key: 'name', label: 'Manager' },
		{ key: 'luck', label: 'Luck', numeric: true, width: '80px' }
	];
	const luckRows = [
		{ name: 'Israel', luck: '+1.2' },
		{ name: 'Craig', luck: '-0.2' }
	];
</script>

<Story name="System row">
	{#snippet template()}
		<div style="max-width: 320px">
			<DataTable label="Career record" columns={system} rows={systemRows}>
				{#snippet cell(row, column)}
					{#if column.key === 'rank'}<span class="rank">{row.rank}</span>
					{:else if column.key === 'name'}<span style="font-weight: 600">{row.name}</span>
					{:else}{row[column.key]}{/if}
				{/snippet}
			</DataTable>
		</div>
	{/snippet}
</Story>

<Story name="Standings with playoff line">
	{#snippet template()}
		<div style="max-width: 1152px">
			<DataTable
				label="Standings"
				columns={standings}
				rows={STANDINGS}
				rowPadding={12}
				columnGap={14}
				minWidth="820px"
				rowKey={(row) => row.name}
				divider={(_, index) => (index === 6 ? 'Playoff line · top 6, 2 byes' : null)}
			>
				{#snippet cell(row, column, index)}
					{#if column.key === 'rank'}<span class="rank big" class:out={index >= 6}>{row.rank}</span>
					{:else if column.key === 'name'}<span class="who"
							><Avatar name={row.name} size={28} />{row.name}</span
						>
					{:else if column.key === 'wl'}<span style="font-weight: 600">{row.wl}</span>
					{:else if column.key === 'pa'}<span style="color: var(--color-muted)">{row.pa}</span>
					{:else if column.key === 'odds'}<OddsBar
							percent={row.odds}
							tone={index >= 6 ? 'sage' : 'field'}
						/>
					{:else if column.key === 'move'}<span style="font-size: 13px"
							><Movement change={row.move} /></span
						>
					{:else}{row[column.key]}{/if}
				{/snippet}
			</DataTable>
		</div>
	{/snippet}
</Story>

<Story name="Highlighted and selected rows">
	{#snippet template()}
		<div style="max-width: 560px">
			<DataTable
				label="Seasons"
				columns={system}
				rows={[
					{ rank: 1, name: 'Title season', record: '11-3', pct: '.786' },
					{ rank: 2, name: 'Selected row', record: '9-5', pct: '.643' },
					{ rank: 3, name: 'Plain row', record: '7-7', pct: '.500' }
				]}
				highlighted={(_, index) => index === 0}
				selected={(_, index) => index === 1}
			/>
		</div>
	{/snippet}
</Story>

<Story name="Phone sizes (Power Rankings)">
	{#snippet template()}
		<div style="max-width: 328px">
			<DataTable
				label="Luck"
				columns={luckColumns}
				rows={luckRows}
				rowPadding={4}
				columnGap={10}
				fontSize={14}
				headerHeight={44}
			>
				{#snippet header(column)}
					{#if column.key === 'luck'}<StatTip
							metricId="luck_factor"
							label="Luck"
							phoneTarget
						/>{:else}{column.label}{/if}
				{/snippet}
				{#snippet cell(row, column)}
					<span style="display: flex; align-items: center; min-height: 44px">{row[column.key]}</span
					>
				{/snippet}
			</DataTable>
		</div>
	{/snippet}
</Story>

<Story name="Header with a StatTip">
	{#snippet template()}
		<div style="max-width: 360px">
			<DataTable label="Luck" columns={luckColumns} rows={luckRows}>
				{#snippet header(column)}
					{#if column.key === 'luck'}<StatTip
							metricId="luck_factor"
							label="Luck"
						/>{:else}{column.label}{/if}
				{/snippet}
			</DataTable>
		</div>
	{/snippet}
</Story>

<Story name="Sticky first column (phone)">
	{#snippet template()}
		<div style="width: 358px">
			<DataTable
				label="Standings"
				stickyFirst
				minWidth="700px"
				fontSize={14}
				columns={[
					{ key: 'name', label: 'Manager', width: '168px' },
					{ key: 'wl', label: 'W-L', numeric: true, width: '60px' },
					{ key: 'pf', label: 'PF', numeric: true, width: '80px' },
					{ key: 'pa', label: 'PA', numeric: true, width: '80px' },
					{ key: 'odds', label: 'Playoffs', numeric: true }
				]}
				rows={STANDINGS}
			/>
		</div>
	{/snippet}
</Story>

<style>
	.rank {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 18px;
	}

	/* This Season sets its ranks a point larger than the System row. */
	.rank.big {
		font-size: 19px;
	}

	.rank.out {
		color: var(--color-muted);
	}

	.who {
		display: flex;
		align-items: center;
		gap: 10px;
		font-weight: 600;
	}
</style>

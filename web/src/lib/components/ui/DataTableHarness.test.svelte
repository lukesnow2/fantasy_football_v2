<script lang="ts">
	import DataTable from './DataTable.svelte';

	const columns = [
		{ key: 'name', label: 'Manager' },
		{ key: 'playoffs', label: 'Playoffs', numeric: true, width: '40px', fixedHeader: true }
	];
	const rows = [
		{ name: 'Trevor', playoffs: 18, tier: 'Hall of Fame Elite' },
		{ name: 'Gabe the Younger', playoffs: 12, tier: null },
		{ name: 'Nick', playoffs: 10, tier: 'Hall of Fame' }
	];
</script>

<div style="width: 400px">
	<DataTable {columns} {rows} label="Ranks" divider={(r) => r.tier}>
		{#snippet dividerCell(name, row)}
			<span class="tier">{name} opens at {row.name}</span>
		{/snippet}
	</DataTable>
</div>

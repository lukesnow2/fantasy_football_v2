<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import PositionChip from '$lib/components/ui/PositionChip.svelte';

	const { Story } = defineMeta({ title: 'UI/PositionChip', component: PositionChip, tags: ['autodocs'] });

	const positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF', 'DL'];
</script>

<Story name="Solid (selected pick)">
	{#snippet template()}
		<div style="display: flex; gap: 8px">
			{#each positions as position (position)}<PositionChip {position} />{/each}
		</div>
	{/snippet}
</Story>

<Story name="Neutral (value lists)">
	{#snippet template()}
		<div style="display: flex; gap: 8px">
			{#each positions as position (position)}<PositionChip {position} tone="neutral" />{/each}
		</div>
	{/snippet}
</Story>

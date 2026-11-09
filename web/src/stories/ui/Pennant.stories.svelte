<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Pennant from '$lib/components/ui/Pennant.svelte';

	const { Story } = defineMeta({ title: 'UI/Pennant', component: Pennant, tags: ['autodocs'] });
</script>

<Story name="One title">
	{#snippet template()}
		<Pennant />
	{/snippet}
</Story>

<Story name="A row of titles">
	{#snippet template()}
		<span role="img" aria-label="7 titles" style="display: inline-flex; gap: 3px">
			{#each [...Array(7).keys()] as i (i)}<Pennant />{/each}
		</span>
	{/snippet}
</Story>

<Story name="On a dark card">
	{#snippet template()}
		<div style="display: inline-flex; gap: 3px; padding: 16px; background: var(--color-clubhouse)">
			<Pennant staff="var(--color-white)" fill="var(--color-pennant-light)" />
			<Pennant staff="var(--color-white)" fill="var(--color-pennant-light)" />
		</div>
	{/snippet}
</Story>

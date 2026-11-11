<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import RadioChip from '$lib/components/ui/RadioChip.svelte';

	const { Story } = defineMeta({ title: 'UI/RadioChip', component: RadioChip, tags: ['autodocs'] });
</script>

<Story name="Commissioner's ruling (Bet Board)">
	{#snippet template()}
		<div style="display: flex; flex-wrap: wrap; gap: 6px">
			<RadioChip name="outcome" value="proposer" label="Luke S" checked required />
			<RadioChip name="outcome" value="taker" label="Trevor" />
			<RadioChip name="outcome" value="push" label="Push" />
			<RadioChip name="outcome" value="void" label="Void it" />
		</div>
	{/snippet}
</Story>

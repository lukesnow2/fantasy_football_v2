<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Switch from '$lib/components/ui/Switch.svelte';

	const { Story } = defineMeta({ title: 'UI/Switch', component: Switch, tags: ['autodocs'] });
</script>

<Story name="On and off">
	{#snippet template()}
		<div style="display: flex; gap: 16px; align-items: center">
			<Switch label="New rule proposals" checked />
			<Switch label="Trade offers" />
			<Switch label="Disabled" disabled checked />
		</div>
	{/snippet}
</Story>

<Story name="In a settings row">
	{#snippet template()}
		<label
			style="display: flex; align-items: center; justify-content: space-between; gap: 16px; width: 664px; padding: 14px 0; border-bottom: 1px solid var(--color-line-soft); cursor: pointer"
		>
			<span>
				<span style="display: block; font-weight: 700; font-size: 16px">Vote results</span>
				<span style="display: block; margin-top: 2px; font-size: 14px; color: var(--color-muted)"
					>When a proposal passes or fails.</span
				>
			</span>
			<Switch name="emailOnVoteResults" label="Vote results" checked />
		</label>
	{/snippet}
</Story>

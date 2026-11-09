<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Avatar from '$lib/components/ui/Avatar.svelte';

	const { Story } = defineMeta({ title: 'UI/Avatar', component: Avatar, tags: ['autodocs'] });

	const sizes = [20, 28, 40, 64, 104, 128];
</script>

<Story name="Photo, every size">
	{#snippet template()}
		<div style="display: flex; gap: 16px; align-items: center">
			{#each sizes as size (size)}<Avatar name="Trevor" {size} />{/each}
		</div>
	{/snippet}
</Story>

<Story name="No photo">
	{#snippet template()}
		<div style="display: flex; gap: 16px; align-items: center">
			{#each sizes as size (size)}<Avatar name="Bobby" {size} />{/each}
		</div>
	{/snippet}
</Story>

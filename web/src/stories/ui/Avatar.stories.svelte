<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Avatar from '$lib/components/ui/Avatar.svelte';

	const { Story } = defineMeta({ title: 'UI/Avatar', component: Avatar, tags: ['autodocs'] });

	const sizes = [20, 28, 30, 32, 36, 40, 64, 84, 104, 128];
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

<Story name="With a ring">
	{#snippet template()}
		<div
			style="display: flex; gap: 16px; align-items: center; padding: 24px; background: var(--color-pennant-tint)"
		>
			<Avatar name="Luke S" size={84} ring={3} />
			<Avatar name="Trevor" size={64} ring={3} />
		</div>
	{/snippet}
</Story>

<Story name="No photo, field tone">
	{#snippet template()}
		<div style="display: flex; gap: 16px; align-items: center">
			{#each sizes as size (size)}<Avatar name="Justin" {size} initialTone="field" />{/each}
		</div>
	{/snippet}
</Story>

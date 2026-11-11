<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Avatar from '$lib/components/ui/Avatar.svelte';

	const { Story } = defineMeta({ title: 'UI/Avatar', component: Avatar, tags: ['autodocs'] });

	const sizes = [20, 26, 28, 30, 32, 34, 36, 40, 44, 48, 56, 64, 84, 104, 128];
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

<Story name="Ring color (This Season's champion)">
	{#snippet template()}
		<div style="display: inline-flex; padding: 20px; background: var(--color-clubhouse)">
			<Avatar name="Gabe the Younger" size={104} ring={4} ringColor="var(--color-pennant-light)" />
		</div>
	{/snippet}
</Story>

<Story name="Ring as a shadow (Managers rail)">
	{#snippet template()}
		<div style="display: flex; gap: 16px; padding: 16px; background: var(--color-clubhouse)">
			<Avatar
				name="Gabe the Younger"
				size={40}
				ring={2}
				ringStyle="shadow"
				ringColor="var(--color-pennant-light)"
				alt=""
			/>
			<Avatar name="Trevor" size={40} alt="" />
		</div>
	{/snippet}
</Story>

<Story name="Profile header sizes">
	{#snippet template()}
		<div
			style="display: flex; gap: 16px; align-items: center; padding: 24px; background: var(--color-clubhouse)"
		>
			<Avatar name="Trevor" size={128} ring={4} ringColor="var(--color-pennant-light)" />
			<Avatar name="Gabe the Younger" size={120} ring={4} ringColor="var(--color-pennant-light)" />
			<Avatar name="Trevor" size={84} ring={3} ringColor="var(--color-pennant-light)" />
			<Avatar name="Trevor" size={64} ring={3} ringColor="var(--color-pennant-light)" />
			<Avatar name="Bobby" size={30} initialSize={13} />
		</div>
	{/snippet}
</Story>

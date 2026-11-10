<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import TitleFlags from '$lib/components/ui/TitleFlags.svelte';

	const { Story } = defineMeta({
		title: 'UI/TitleFlags',
		component: TitleFlags,
		tags: ['autodocs']
	});
</script>

<Story name="Rail, not selected">
	{#snippet template()}
		<TitleFlags count={7} width={9} height={13} stroke={1.8} gap={2} />
	{/snippet}
</Story>

<Story name="Rail, selected">
	{#snippet template()}
		<div style="display: inline-flex; padding: 12px; background: var(--color-clubhouse)">
			<TitleFlags
				count={3}
				width={9}
				height={13}
				stroke={1.8}
				gap={2}
				pole="white"
				flag="pennant-light"
			/>
		</div>
	{/snippet}
</Story>

<Story name="Managers hero">
	{#snippet template()}
		<div style="display: inline-flex; padding: 16px; background: var(--color-clubhouse)">
			<TitleFlags count={3} width={14} height={20} pole="white" flag="pennant-light" gap={4} />
		</div>
	{/snippet}
</Story>

<Story name="One title, and none">
	{#snippet template()}
		<div style="display: flex; gap: 24px; align-items: center">
			<TitleFlags count={1} />
			<span style="min-height: 13px; outline: 1px dashed var(--color-line)"
				><TitleFlags count={0} /></span
			>
		</div>
	{/snippet}
</Story>

<Story name="A won title game">
	{#snippet template()}
		<TitleFlags count={1} width={14} height={20} label="Won" />
	{/snippet}
</Story>

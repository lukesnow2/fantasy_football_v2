<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Tag from '$lib/components/ui/Tag.svelte';

	const { Story } = defineMeta({ title: 'UI/Tag', component: Tag, tags: ['autodocs'] });
</script>

<Story name="Status tags">
	{#snippet template()}
		<div style="display: flex; gap: 8px; flex-wrap: wrap">
			<Tag variant="clinched">Clinched</Tag>
			<Tag variant="bye">Bye</Tag>
			<Tag variant="eliminated">Eliminated</Tag>
			<Tag variant="champion">Champion</Tag>
		</div>
	{/snippet}
</Story>

<Story name="Champion, strong">
	{#snippet template()}
		<Tag variant="champion-strong">Champion</Tag>
	{/snippet}
</Story>

<Story name="In a row">
	{#snippet template()}
		<div style="display: flex; gap: 8px; flex-wrap: wrap; font-size: 15px">
			<Tag variant="champion-strong">Champion</Tag>
			<Tag variant="champion">Runner-up</Tag>
			<Tag variant="bye">Semifinalist</Tag>
			<Tag variant="eliminated">Consolation winner</Tag>
		</div>
	{/snippet}
</Story>

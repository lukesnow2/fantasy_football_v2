<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import TabNav from '$lib/components/ui/TabNav.svelte';

	const { Story } = defineMeta({ title: 'UI/TabNav', component: TabNav, tags: ['autodocs'] });

	const tabs = [
		{ id: 'overview', label: 'Overview', href: '?' },
		{ id: 'seasons', label: 'Season history', href: '?tab=seasons' },
		{ id: 'head-to-head', label: 'Head to head', href: '?tab=head-to-head' },
		{ id: 'achievements', label: 'Achievements', href: '?tab=achievements' }
	];
</script>

<script>
	let current = $state('overview');
</script>

<Story name="Profile sections">
	{#snippet template()}
		<TabNav label="Profile sections" {tabs} {current} onselect={(id) => (current = id)} />
	{/snippet}
</Story>

<Story name="Last tab current">
	{#snippet template()}
		<TabNav label="Profile sections" {tabs} current="achievements" />
	{/snippet}
</Story>

<Story name="Phone width (scrolls)">
	{#snippet template()}
		<div style="width: 358px">
			<TabNav label="Profile sections" {tabs} current="achievements" />
		</div>
	{/snippet}
</Story>

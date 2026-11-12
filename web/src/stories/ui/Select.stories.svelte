<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Select from '$lib/components/ui/Select.svelte';

	const { Story } = defineMeta({ title: 'UI/Select', component: Select, tags: ['autodocs'] });

	const seasons = [
		{ value: 'all', label: 'All seasons' },
		{ value: '2026', label: '2026' },
		{ value: '2025', label: '2025' }
	];
</script>

<Story name="Season (Trade Center)">
	{#snippet template()}
		<Select label="Season" options={seasons} value="all" />
	{/snippet}
</Story>

<Story name="A year picked (Draft Central)">
	{#snippet template()}
		<Select label="Season" options={seasons} value="2025" />
	{/snippet}
</Story>

<Story name="Full width at 390" parameters={{ viewport: { defaultViewport: 'mobile1' } }}>
	{#snippet template()}
		<div style="width: 358px">
			<Select label="Season" options={seasons} value="all" />
		</div>
	{/snippet}
</Story>

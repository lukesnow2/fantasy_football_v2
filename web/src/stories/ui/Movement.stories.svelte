<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Movement from '$lib/components/ui/Movement.svelte';

	const { Story } = defineMeta({ title: 'UI/Movement', component: Movement, tags: ['autodocs'] });
</script>

<Story name="Up, down and same">
	{#snippet template()}
		<div style="display: flex; gap: 18px; font-size: 15px">
			<Movement change={4} />
			<Movement change={-2} />
			<Movement change={0} />
		</div>
	{/snippet}
</Story>

<Story name="Inherit the parent's type (Power Rankings)">
	{#snippet template()}
		<div style="display: flex; gap: 24px; align-items: baseline">
			<span style="font-size: 13px; font-weight: 700"><Movement change={4} inherit /></span>
			<span style="font-size: 13px; font-weight: 700"><Movement change={0} inherit /></span>
			<span style="font-family: var(--font-slab); font-weight: 700; font-size: 30px"
				><Movement change={-5} inherit /></span
			>
		</div>
	{/snippet}
</Story>

<Story name="Up" args={{ change: 4 }} />
<Story name="Down" args={{ change: -2 }} />
<Story name="Same" args={{ change: 0 }} />
<Story name="No value" args={{ change: null }} />

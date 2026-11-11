<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import OddsBar from '$lib/components/ui/OddsBar.svelte';

	const { Story } = defineMeta({
		title: 'UI/OddsBar',
		component: OddsBar,
		tags: ['autodocs']
	});
</script>

<Story name="At 85 percent">
	{#snippet template()}
		<div style="width: 300px"><OddsBar percent={85} /></div>
	{/snippet}
</Story>

<Story name="Below the line, sage">
	{#snippet template()}
		<div style="width: 300px"><OddsBar percent={45} tone="sage" /></div>
	{/snippet}
</Story>

<Story name="Ends">
	{#snippet template()}
		<div style="display: grid; gap: 12px; width: 300px">
			<OddsBar percent={0} />
			<OddsBar percent={100} />
			<OddsBar percent={0.4} text="Under 1%" />
		</div>
	{/snippet}
</Story>

<Story name="Fixed number width">
	{#snippet template()}
		<div style="display: grid; gap: 12px; width: 260px">
			<OddsBar percent={85} numberWidth={38} />
			<OddsBar percent={5} tone="sage" numberWidth={38} />
		</div>
	{/snippet}
</Story>

<Story name="Tighter gap (Hall of Fame phone)">
	{#snippet template()}
		<div style="width: 86px">
			<OddsBar percent={62} text="0.510" numberWidth={38} gap={8} />
		</div>
	{/snippet}
</Story>

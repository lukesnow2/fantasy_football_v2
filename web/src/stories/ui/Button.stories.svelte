<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Button from '$lib/components/ui/Button.svelte';
	import { fn } from 'storybook/test';

	const { Story } = defineMeta({
		title: 'UI/Button',
		component: Button,
		tags: ['autodocs'],
		args: { onclick: fn() }
	});
</script>

<Story name="Primary">
	{#snippet template()}
		<Button variant="primary">Submit proposal</Button>
	{/snippet}
</Story>

<Story name="Secondary">
	{#snippet template()}
		<Button variant="secondary">Cancel</Button>
	{/snippet}
</Story>

<Story name="Link">
	{#snippet template()}
		<Button variant="link" href="#">Full rankings →</Button>
	{/snippet}
</Story>

<Story name="Secondary as a link">
	{#snippet template()}
		<Button variant="secondary" href="#">Open in Yahoo ↗</Button>
	{/snippet}
</Story>

<Story name="Disabled">
	{#snippet template()}
		<div style="display: flex; gap: 10px">
			<Button variant="primary" disabled>Submit proposal</Button>
			<Button variant="secondary" disabled>Cancel</Button>
		</div>
	{/snippet}
</Story>

<Story name="On the band, large">
	{#snippet template()}
		<div
			style="display: flex; flex-wrap: wrap; gap: 12px; padding: 24px; background: var(--color-clubhouse)"
		>
			<Button variant="band-primary" size="large" href="#">This season →</Button>
			<Button variant="band-outline" size="large" href="#">League history</Button>
		</div>
	{/snippet}
</Story>

<Story name="Link with a touch target">
	{#snippet template()}
		<Button variant="link" size="tap" href="#">All records →</Button>
	{/snippet}
</Story>

<Story name="Pennant link">
	{#snippet template()}
		<div style="padding: 24px; background: var(--color-pennant-tint)">
			<Button variant="link-pennant" size="tap" href="#">Head to head →</Button>
		</div>
	{/snippet}
</Story>

<Story name="White on the clubhouse card">
	{#snippet template()}
		<div style="padding: 24px; background: var(--color-clubhouse)">
			<Button variant="band-white" href="#">Open full profile →</Button>
		</div>
	{/snippet}
</Story>

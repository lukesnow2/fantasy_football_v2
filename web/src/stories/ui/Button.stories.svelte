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

<Story name="Toggle chips (Show tags)">
	{#snippet template()}
		<div style="display: flex; gap: 8px">
			<Button variant="chip" aria-pressed="true">29 picks</Button>
			<Button variant="chip" aria-pressed="false">27 picks</Button>
		</div>
	{/snippet}
</Story>

<Story name="Large, bold (Post a bet)">
	{#snippet template()}
		<div style="display: flex; gap: 10px">
			<Button size="lg">Post a bet</Button>
			<Button size="lg" padX={26} type="submit">Post it</Button>
		</div>
	{/snippet}
</Story>

<Story name="Small, on a bet">
	{#snippet template()}
		<div style="display: grid; gap: 12px; max-width: 360px">
			<div style="display: flex; gap: 8px">
				<Button size="sm">Take it</Button>
				<Button size="sm" variant="secondary">Pass</Button>
				<Button size="sm" variant="text" tone="pennant" style="margin-left: auto">Pull it</Button>
			</div>
			<div><Button size="sm" variant="outline">This one's over</Button></div>
			<div style="display: flex; gap: 8px">
				<Button size="sm" tone="pennant">Record the ruling</Button>
				<Button size="sm" variant="text">Cancel</Button>
			</div>
		</div>
	{/snippet}
</Story>

<Story name="Filter chips and extra small (League History)">
	{#snippet template()}
		<div style="display: grid; gap: 12px; max-width: 560px">
			<div style="display: flex; flex-wrap: wrap; gap: 8px">
				<Button variant="filter" aria-pressed="true">All <span style="opacity: 0.7">23</span></Button>
				<Button variant="filter" aria-pressed="false"
					>Scoring <span style="opacity: 0.7">13</span></Button
				>
			</div>
			<div style="display: flex; gap: 8px">
				<Button variant="secondary" size="xs">Save image</Button>
				<Button variant="secondary" size="xs" aria-pressed="false">Show table</Button>
			</div>
		</div>
	{/snippet}
</Story>

<Story name="Constitution and Data Dictionary (WP11)">
	{#snippet template()}
		<div style="display: grid; gap: 14px; justify-items: start">
			<Button size="lg" padX={20}>Propose a change</Button>
			<Button size="lg" padX={20} aria-pressed="true">Done proposing</Button>
			<Button variant="dashed">+ Propose a new clause in this article</Button>
			<div style="display: flex; gap: 4px">
				<Button variant="icon" aria-label="Propose an edit to this clause"
					><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 20h4L19 9l-4-4L4 16z"></path></svg></Button
				>
				<Button variant="icon" tone="pennant" aria-label="Propose removing this clause"
					><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"></path></svg></Button
				>
			</div>
			<Button variant="link" size="sm">Propose an amendment</Button>
			<Button variant="link" size="xs">Show details</Button>
			<div style="display: flex; gap: 8px">
				<Button variant="filter" aria-pressed="true">All <span style="opacity: 0.7">21</span></Button>
				<Button variant="filter" aria-pressed="false">Trades <span style="opacity: 0.7">2</span></Button>
				<Button variant="filter" tone="muted">+ 5 more</Button>
			</div>
			<div style="display: flex; gap: 10px">
				<Button height={46} padX={20} bold>Save as draft</Button>
				<Button variant="secondary" height={46}>Cancel</Button>
			</div>
		</div>
	{/snippet}
</Story>

<Story name="Sign in (WP12)">
	{#snippet template()}
		<div style="display: grid; gap: 14px; width: 388px">
			<Button size="xl" type="submit">Email me a sign-in link</Button>
			<div><Button size="xl" href="#">Send me a new link</Button></div>
			<div style="display: flex; gap: 14px; align-items: center; justify-content: flex-end">
				<Button height={46} padX={20} bold>Save preferences</Button>
			</div>
		</div>
	{/snippet}
</Story>

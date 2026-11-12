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

<Story name="Last place">
	{#snippet template()}
		<Tag variant="last-place">Last Place Trophy</Tag>
	{/snippet}
</Story>

<Story name="Row size (This Season)">
	{#snippet template()}
		<div style="display: flex; gap: 8px; flex-wrap: wrap; font-size: 15px">
			<Tag variant="bye" size="row">Add</Tag>
			<Tag variant="champion" size="row">Drop</Tag>
			<Tag variant="eliminated" size="row">Add/drop</Tag>
			<Tag variant="champion-strong" size="row">Champion</Tag>
			<Tag variant="last-place" size="row">Last Place Trophy</Tag>
		</div>
	{/snippet}
</Story>

<Story name="Small size (Playoff Field's Bye)">
	{#snippet template()}
		<Tag variant="bye" size="small">Bye</Tag>
	{/snippet}
</Story>

<Story name="Season outcomes">
	{#snippet template()}
		<div style="display: flex; gap: 8px; flex-wrap: wrap">
			<Tag variant="champion" size="row">Champion</Tag>
			<Tag variant="playoffs" size="row">Playoffs</Tag>
			<Tag variant="missed" size="row">Missed playoffs</Tag>
			<Tag variant="in-progress" size="row">In progress · 61% playoff odds</Tag>
		</div>
	{/snippet}
</Story>

<Story name="Title game results">
	{#snippet template()}
		<div style="display: flex; gap: 8px">
			<Tag variant="champion-strong" size="row">Champion</Tag>
			<Tag variant="runner-up" size="row">Runner-up</Tag>
		</div>
	{/snippet}
</Story>

<Story name="Achievement chips">
	{#snippet template()}
		<div style="display: flex; gap: 8px; flex-wrap: wrap">
			<Tag variant="champion-strong" size="chip">Champion</Tag>
			<Tag variant="clinched" size="chip">Consistent winner</Tag>
			<Tag variant="bye" size="chip">High scorer</Tag>
			<Tag variant="eliminated" size="chip">Long timer</Tag>
			<Tag variant="band-champion" size="chip-badge">Dynasty builder</Tag>
		</div>
	{/snippet}
</Story>

<Story name="On the clubhouse card">
	{#snippet template()}
		<div style="display: flex; gap: 6px; padding: 16px; background: var(--color-clubhouse)">
			<Tag variant="band-champion" caps>League legend</Tag>
			<Tag variant="band-champion" size="chip-small">Champion</Tag>
			<Tag variant="band" size="chip-small">High scorer</Tag>
			<Tag variant="band" size="chip-small">Long timer</Tag>
		</div>
	{/snippet}
</Story>

<Story name="Wraps on a phone">
	{#snippet template()}
		<div style="width: 150px">
			<Tag variant="in-progress" size="row" wrapOnPhone>In progress · 61% playoff odds</Tag>
		</div>
	{/snippet}
</Story>

<Story name="Trade status (Trade Center)">
	{#snippet template()}
		<div style="display: flex; gap: 8px">
			<Tag variant="pending" size="trade">In progress</Tag>
			<Tag variant="final" size="trade">Final</Tag>
		</div>
	{/snippet}
</Story>

<Story name="Draft board tags">
	{#snippet template()}
		<div style="display: grid; gap: 12px">
			<div style="display: flex; gap: 8px">
				<Tag variant="steal" size="sample">Steal</Tag>
				<Tag variant="value" size="sample">Value</Tag>
				<Tag variant="letdown" size="sample">Letdown</Tag>
				<Tag variant="bust" size="sample">Bust</Tag>
			</div>
			<div style="display: flex; gap: 8px">
				<Tag variant="steal" size="cell">Steal</Tag>
				<Tag variant="value" size="cell">Value</Tag>
				<Tag variant="letdown" size="cell">Letdown</Tag>
				<Tag variant="bust" size="cell">Bust</Tag>
			</div>
		</div>
	{/snippet}
</Story>

<Story name="Counts (Bet Board)">
	{#snippet template()}
		<div style="display: grid; gap: 16px; max-width: 380px">
			<div style="display: flex; flex-wrap: wrap; gap: 8px">
				<Tag variant="count" size="count">1 on the board</Tag>
				<Tag variant="count" size="count">0 settled</Tag>
			</div>
			<div style="display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px">
				<Tag variant="count" size="count" center>1 on the board</Tag>
				<Tag variant="count" size="count" center>1 awaiting a ruling</Tag>
			</div>
		</div>
	{/snippet}
</Story>

<Story name="Proposal status (WP11)">
	{#snippet template()}
		<div style="display: flex; gap: 8px; flex-wrap: wrap">
			<Tag variant="pending" size="status">Draft</Tag>
			<Tag variant="clinched" size="status">Voting open</Tag>
			<Tag variant="passed" size="status">Passed</Tag>
			<Tag variant="champion" size="status">Rejected</Tag>
			<Tag variant="missed" size="status">Withdrawn</Tag>
			<Tag variant="missed" size="status">Superseded</Tag>
			<Tag variant="champion" size="pill">1 proposed</Tag>
		</div>
	{/snippet}
</Story>

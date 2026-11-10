<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Button from '$lib/components/ui/Button.svelte';
	import StatStrip from '$lib/components/ui/StatStrip.svelte';
	import StatTile from '$lib/components/ui/StatTile.svelte';
	import { SEASON_STATS } from './fixtures';

	const { Story } = defineMeta({
		title: 'UI/PageHeader',
		component: PageHeader,
		tags: ['autodocs']
	});
</script>

<Story name="With a page action">
	{#snippet template()}
		<div style="max-width: 1152px">
			<PageHeader
				kicker="2026 Season · Week 3 of 14 · Regular season in progress"
				title="This Season"
			>
				Live standings, matchups and the playoff picture as the season plays out.
				{#snippet action()}<Button variant="secondary" href="#">Open in Yahoo ↗</Button>{/snippet}
			</PageHeader>
		</div>
	{/snippet}
</Story>

<Story name="This Season opening" parameters={{ layout: 'fullscreen' }}>
	{#snippet template()}
		<main class="frame">
			<PageHeader
				kicker="2026 Season · Week 3 of 14 · Regular season in progress"
				title="This Season"
			>
				Live standings, matchups and the playoff picture as the season plays out.
				{#snippet action()}<Button variant="secondary" href="#">Open in Yahoo ↗</Button>{/snippet}
			</PageHeader>
			<div style="margin-top: 28px">
				<StatStrip label="This season at a glance">
					{#each SEASON_STATS as stat (stat.label)}<StatTile {...stat} />{/each}
				</StatStrip>
			</div>
		</main>
	{/snippet}
</Story>

<Story name="Kicker, title and intro">
	{#snippet template()}
		<div style="max-width: 1152px">
			<PageHeader kicker="2025 Season · Season complete" title="This Season">
				Final standings and how the bracket played out. The page switches back to live standings
				when the next season starts.
			</PageHeader>
		</div>
	{/snippet}
</Story>

<Story name="Title only">
	{#snippet template()}
		<div style="max-width: 1152px">
			<PageHeader title="Hall of Fame" />
		</div>
	{/snippet}
</Story>

<Story name="Page sizes">
	{#snippet template()}
		<div style="display: grid; gap: 32px; max-width: 1152px">
			<PageHeader kicker="Size 96" title="The League" size={96} />
			<PageHeader kicker="Size 60, the default" title="Power Rankings" />
			<PageHeader kicker="Size 40" title="Sign in" size={40} />
		</div>
	{/snippet}
</Story>

<Story name="Old caller with an icon">
	{#snippet template()}
		<div style="max-width: 1152px">
			<PageHeader title="Trade Center" icon={{}} accent="amber">
				The icon and accent props still compile and draw nothing.
			</PageHeader>
		</div>
	{/snippet}
</Story>

<style>
	/* The boards' main column: 1200px wide, 40px 24px 64px, 16px sides on phones. */
	.frame {
		width: 100%;
		max-width: 1200px;
		color: var(--color-ink);
		margin: 0 auto;
		padding: 40px 24px 64px;
		box-sizing: border-box;
	}

	@media (max-width: 480px) {
		.frame {
			padding-left: 16px;
			padding-right: 16px;
		}
	}
</style>

<Story name="Loading">
	{#snippet template()}
		<div style="max-width: 1152px">
			<PageHeader title="This Season" pending>Intro.</PageHeader>
		</div>
	{/snippet}
</Story>

<Story name="Phone kicker" parameters={{ viewport: { defaultViewport: 'mobile1' } }}>
	{#snippet template()}
		<PageHeader
			kicker="2026 Season · Week 3 of 14 · Regular season in progress"
			phoneKicker="2026 · Week 3 of 14"
			title="This Season"
			hideActionOnPhone
		>
			Live standings, matchups and the playoff picture as the season plays out.
			{#snippet action()}<Button variant="secondary" href="#">Open in Yahoo ↗</Button>{/snippet}
		</PageHeader>
	{/snippet}
</Story>

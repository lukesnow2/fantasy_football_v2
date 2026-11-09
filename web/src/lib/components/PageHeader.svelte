<script lang="ts">
	/**
	 * The standard page header for every top-level route (System board, "Every
	 * page opens the same way"; This Season for the measurements). A field
	 * kicker, the slab title, one ink-2 intro line, left aligned, no icon. A page
	 * action, such as "Open in Yahoo ↗", sits on the right, bottom aligned with
	 * the title block, and drops under it when there's no room.
	 *
	 *   <PageHeader kicker="2026 Season · Week 3 of 14" title="This Season">
	 *     Live standings, matchups and the playoff picture as the season plays out.
	 *     {#snippet action()}<Button variant="secondary" href={yahooUrl}>Open in Yahoo ↗</Button>{/snippet}
	 *   </PageHeader>
	 *
	 * `icon` and `accent` are left over from the old centered header. They still
	 * compile so unrestyled callers don't break, and they render nothing. The
	 * housekeeping PR after WP12 removes them.
	 */
	import type { Snippet } from 'svelte';

	interface Props {
		title: string;
		kicker?: string;
		/** The title's size in px, from the page's board. 60 on most pages. */
		size?: number;
		/** Deprecated: renders nothing. */
		icon?: unknown;
		/** Deprecated: renders nothing. */
		accent?: string;
		/** The intro line. */
		children?: Snippet;
		/** A page action on the right. */
		action?: Snippet;
	}

	let { title, kicker, size = 60, children, action }: Props = $props();
</script>

<div class="page-header">
	<div class="block">
		{#if kicker}
			<div class="kicker">{kicker}</div>
		{/if}
		<h1 class:first={!kicker} style:font-size="{size}px">{title}</h1>
		{#if children}
			<p>{@render children()}</p>
		{/if}
	</div>
	{#if action}
		<div class="action">{@render action()}</div>
	{/if}
</div>

<style>
	.page-header {
		display: flex;
		flex-wrap: wrap;
		gap: 20px 40px;
		justify-content: space-between;
		align-items: flex-end;
	}

	.block {
		flex: 1 1 520px;
		min-width: 0;
	}

	.kicker {
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-field);
	}

	h1 {
		margin: 8px 0 10px;
		font-family: var(--font-slab);
		font-weight: 700;
		line-height: 1.02;
		letter-spacing: -0.01em;
		color: var(--color-ink);
	}

	h1.first {
		margin-top: 0;
	}

	p {
		margin: 0;
		max-width: 620px;
		font-size: 18px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}
</style>

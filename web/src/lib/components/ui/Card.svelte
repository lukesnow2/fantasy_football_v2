<script lang="ts">
	/**
	 * The one card (System board, "One card style"): white, a 1px line border,
	 * 12px corners, no shadow, 22px 24px of padding. An optional header row puts
	 * the slab title on the left and a link or control on the right, baseline
	 * aligned, 12px above the body.
	 *
	 *   <Card title="Standings">
	 *     {#snippet action()}<Button variant="link" href="/power-rankings">Full rankings →</Button>{/snippet}
	 *     ...
	 *   </Card>
	 *
	 * `flush` drops the padding so a table can run edge to edge; the header row
	 * keeps its padding so the title still lines up with other cards.
	 */
	import type { Snippet } from 'svelte';

	interface Props {
		title?: string;
		/** The title's size in px. 28 on most boards; take it from the board. */
		titleSize?: number;
		/** The heading level, 2 unless the card sits under another card's title. */
		level?: 2 | 3;
		flush?: boolean;
		/** Classes for spacing between cards, passed by the page. */
		class?: string;
		action?: Snippet;
		children?: Snippet;
	}

	let {
		title,
		titleSize = 28,
		level = 2,
		flush = false,
		class: className = '',
		action,
		children
	}: Props = $props();
</script>

<section class="card {className}" class:flush>
	{#if title || action}
		<div class="head">
			{#if title}
				<svelte:element this={`h${level}`} class="title" style:font-size="{titleSize}px">
					{title}
				</svelte:element>
			{/if}
			{#if action}
				<div class="action">{@render action()}</div>
			{/if}
		</div>
	{/if}
	{@render children?.()}
</section>

<style>
	.card {
		background: var(--color-card);
		border: 1px solid var(--color-line);
		border-radius: var(--radius-card);
		padding: 22px 24px;
		min-width: 0;
	}

	.card.flush {
		padding: 0;
	}

	.head {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		align-items: baseline;
		gap: 8px 12px;
		margin-bottom: 12px;
	}

	.flush .head {
		padding: 22px 24px 0;
	}

	.title {
		margin: 0;
		font-family: var(--font-slab);
		font-weight: 700;
	}
</style>

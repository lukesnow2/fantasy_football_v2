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
	 * Below 640px it follows the build spec's Phones rules: a 12px kicker, a 38px
	 * title 6px under it, and the intro 10px down at 16px with no max-width.
	 * `phoneKicker` swaps in a shorter kicker there, and `hideActionOnPhone`
	 * drops the action, as This Season's phone board draws it. `pending` shows
	 * the kicker and intro as line-soft bars while the page loads.
	 *
	 * `icon` and `accent` are left over from the old centered header. They still
	 * compile so unrestyled callers don't break, and they render nothing. The
	 * housekeeping PR after WP12 removes them.
	 */
	import type { Snippet } from 'svelte';

	interface Props {
		title: string;
		kicker?: string;
		/** A shorter kicker below 640px. */
		phoneKicker?: string;
		/** Kicker and intro as bars while the page's data loads. */
		pending?: boolean;
		hideActionOnPhone?: boolean;
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

	let {
		title,
		kicker,
		phoneKicker,
		pending = false,
		hideActionOnPhone = false,
		size = 60,
		children,
		action
	}: Props = $props();
</script>

<div class="page-header" aria-busy={pending || undefined}>
	<div class="block">
		{#if pending}
			<div class="kicker"><span class="bar kicker-bar"></span></div>
		{:else if kicker}
			<div class="kicker">
				{#if phoneKicker}
					<span class="desktop-only">{kicker}</span><span class="phone-only">{phoneKicker}</span>
				{:else}
					{kicker}
				{/if}
			</div>
		{/if}
		<h1 class:first={!kicker && !pending} style:--title-size="{size}px">{title}</h1>
		{#if pending}
			<p><span class="bar intro-bar"></span></p>
		{:else if children}
			<p>{@render children()}</p>
		{/if}
	</div>
	{#if action && !pending}
		<div class="action" class:hide-phone={hideActionOnPhone}>{@render action()}</div>
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
		font-size: var(--title-size);
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

	.bar {
		display: block;
		max-width: 100%;
		border-radius: 4px;
		background: var(--color-line-soft);
	}

	.kicker-bar {
		width: 420px;
		height: 16px;
	}

	.intro-bar {
		width: 560px;
		height: 27px;
	}

	.phone-only {
		display: none;
	}

	@media (max-width: 639.98px) {
		.kicker {
			font-size: 12px;
		}

		h1 {
			margin: 6px 0 0;
			font-size: 38px;
			line-height: 1;
		}

		h1.first {
			margin-top: 0;
		}

		p {
			margin-top: 10px;
			max-width: none;
			font-size: 16px;
		}

		.kicker-bar {
			width: 200px;
			height: 15px;
		}

		.intro-bar {
			height: 48px;
		}

		.desktop-only,
		.action.hide-phone {
			display: none;
		}

		.phone-only {
			display: inline;
		}
	}
</style>

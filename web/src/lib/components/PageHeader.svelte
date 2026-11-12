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
	 * drops the action, as This Season's phone board draws it, and
	 * `phoneActionGap` sets the px between the intro and an action that wraps
	 * under it (14 on Power Rankings' phone board; 20 otherwise), and
	 * `phoneActionFull` stretches the action across the column (Trade Center's
	 * and Draft Central's Season select). `pending` shows
	 * the kicker and intro as line-soft bars while the page loads.
	 * `phoneKickerLineHeight` sets the kicker's line height below 640px, for
	 * the boards that let a long kicker wrap (WP11's, at 1.4).
	 *
	 * The members-only pages (WP12) add:
	 * - `kickerTone="pennant"`: a pennant-dark kicker ("Commissioner only")
	 * - `tight`: Chat's header. The title at line height 1 with 6px under the
	 *   kicker, the intro 6px under it at 16px and the normal line height, and
	 *   12px 24px between the block and the action
	 * - `titleSuffix`: text after the title in sans 600 muted, 20px (16px below
	 *   640px), as Chat's " #general"
	 * - `phoneActionBesideTitle`: below 640px the action sits at the right end
	 *   of the title row, centered on it, with 12px between (Chat's Live pill)
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
		/** Px between the intro and the action below 640px. */
		phoneActionGap?: number;
		/** Below 640px, the action runs the full width of the column. */
		phoneActionFull?: boolean;
		/** The kicker's line height below 640px, when the phone board sets one. */
		phoneKickerLineHeight?: number;
		/** The title's size in px, from the page's board. 60 on most pages. */
		size?: number;
		/** The intro's max-width in px: 620 unless the board draws another (Managers, 640). */
		introWidth?: number;
		/** The title block's flex basis in px: 520 unless the board draws another (Bet Board, 560). */
		blockBasis?: number;
		/** The kicker's color: field, or pennant-dark for the Commissioner page. */
		kickerTone?: 'field' | 'pennant';
		/** Chat's tighter header (WP12 S12.1). */
		tight?: boolean;
		/** Text after the title in sans 600 muted ("#general"). */
		titleSuffix?: string;
		/** Below 640px, the action sits on the title's row. */
		phoneActionBesideTitle?: boolean;
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
		phoneActionGap = 20,
		phoneActionFull = false,
		phoneKickerLineHeight,
		size = 60,
		introWidth = 620,
		blockBasis = 520,
		kickerTone = 'field',
		tight = false,
		titleSuffix,
		phoneActionBesideTitle = false,
		children,
		action
	}: Props = $props();
</script>

<div
	class="page-header"
	class:tight
	class:beside={phoneActionBesideTitle}
	aria-busy={pending || undefined}
	style:--phone-action-gap="{phoneActionGap}px"
	style:--phone-kicker-lh={phoneKickerLineHeight}
>
	<div class="block" style:--block-basis="{blockBasis}px">
		{#if pending}
			<div class="kicker"><span class="bar kicker-bar"></span></div>
		{:else if kicker}
			<div class="kicker" class:pennant={kickerTone === 'pennant'}>
				{#if phoneKicker}
					<span class="desktop-only">{kicker}</span><span class="phone-only">{phoneKicker}</span>
				{:else}
					{kicker}
				{/if}
			</div>
		{/if}
		<h1 class:first={!kicker && !pending} style:--title-size="{size}px"
			>{title}{#if titleSuffix}{' '}<span class="suffix">{titleSuffix}</span>{/if}</h1
		>
		{#if pending}
			<p><span class="bar intro-bar"></span></p>
		{:else if children}
			<p style:--intro-width="{introWidth}px">{@render children()}</p>
		{/if}
	</div>
	{#if action && !pending}
		<div class="action" class:hide-phone={hideActionOnPhone} class:full-phone={phoneActionFull}>{@render action()}</div>
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
		flex: 1 1 var(--block-basis, 520px);
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

	.kicker.pennant {
		color: var(--color-pennant-dark);
	}

	.suffix {
		font-family: var(--font-sans);
		font-size: 20px;
		font-weight: 600;
		letter-spacing: normal;
		color: var(--color-muted);
	}

	.tight {
		gap: 12px 24px;
	}

	.tight h1 {
		margin: 6px 0 0;
		line-height: 1;
	}

	.tight p {
		margin-top: 6px;
		max-width: none;
		font-size: 16px;
		line-height: normal;
	}

	p {
		margin: 0;
		max-width: var(--intro-width, 620px);
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
		.page-header {
			row-gap: var(--phone-action-gap);
		}

		.kicker {
			font-size: 12px;
			line-height: var(--phone-kicker-lh, normal);
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

		.tight p {
			margin-top: 10px;
			line-height: 1.5;
		}

		.suffix {
			font-size: 16px;
		}

		/* The block's three lines and the action on one grid, so the action can
		   take the title's row without rendering twice. */
		.beside {
			display: grid;
			grid-template-columns: minmax(0, 1fr) auto;
			column-gap: 12px;
			row-gap: 0;
		}

		.beside .block {
			display: contents;
		}

		.beside .kicker,
		.beside p {
			grid-column: 1 / -1;
		}

		.beside .kicker {
			grid-row: 1;
		}

		.beside h1 {
			grid-row: 2;
			grid-column: 1;
		}

		.beside .action {
			grid-row: 2;
			grid-column: 2;
			align-self: center;
			margin-top: 6px;
		}

		.beside p {
			grid-row: 3;
		}

		.action.full-phone {
			flex: 1 1 100%;
			min-width: 0;
		}

		.action.full-phone > :global(*) {
			width: 100%;
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

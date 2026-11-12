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
	 *
	 * Some boards draw other measurements; take each from the board:
	 * - `padding`: the card's padding as CSS ("24px" on Home's cards)
	 * - `headGap`: px between the header row and the body (12 unless drawn otherwise)
	 * - `tone`: "borderless" for a white card on the clubhouse band (Home's This
	 *   week), "pennant" for a pennant-tint card with a pennant-line border
	 *   (Home's Biggest rivalry), "clubhouse" for the dark card with white text
	 *   (This Season's champion)
	 *   WP8 adds "field", a field-tint card with a field-line border (the
	 *   season draft's Best value), and "panel", a box inside a card with a
	 *   line-soft border and 10px corners (Best drafters)
	 *   WP9 (the Bet Board) adds "highlight", a card with a 2px field border
	 *   (the open bet form); "inset", a box inside a card with a 1px line border
	 *   and 10px corners (a bet), and "inset-pennant", the same with a
	 *   pennant-line border on pennant-wash (a bet awaiting a ruling); "dashed",
	 *   the 1.5px dashed line-strong box with centered 15px ink-2 text that an
	 *   empty stage shows; and "tile", card fill with no border and 10px corners
	 *   (a ledger tile)
	 * - `dot`: a 10px dot of that color token 10px left of the title, centered
	 *   on it (the Bet Board's stage heads)
	 * - `stack`: lays the card out as a flex column with this many px between
	 *   its header and each child, so a child can push itself down with
	 *   `margin-top: auto` (the Bet Board's stage cards, 12)
	 * - `label`: an accessible name for a card with no title ("Selected pick")
	 * - `element`: bindable, the card's <section>, for a page that scrolls to it
	 * - `headAlign`, `headSpacing`: the header row's alignment and gap, when a
	 *   board draws a control beside the title (This Season's Final standings:
	 *   center, "10px 16px")
	 *
	 * League History (WP10) adds:
	 * - `headRule`: a 2px ink rule under the header row, 10px below it, with
	 *   the body right under the rule (the record book's group cards)
	 * - `icon`: a snippet 10px left of the title, centered on it (the
	 *   Championship records band's pennant)
	 *
	 * Below 640px every card follows the build spec's Phones rules: 16px of
	 * padding and a 22px title. A phone board that draws something else sets:
	 * - `phonePadding`: the padding as CSS ("16px 16px 6px" when rows run to the
	 *   bottom)
	 * - `phoneTitleSize`, `phoneHeadGap`: as `titleSize` and `headGap`
	 * - `phoneHeadStack`: the action drops under the title, 4px down, or
	 *   `phoneHeadStackGap` px
	 * - `phoneHeadAlign`: "center" when the action is a 44px link the title
	 *   centers against
	 */
	import type { Snippet } from 'svelte';

	interface Props {
		title?: string;
		/** The title's size in px. 28 on most boards; take it from the board. */
		titleSize?: number;
		/** The heading level, 2 unless the card sits under another card's title. */
		level?: 2 | 3;
		flush?: boolean;
		padding?: string;
		headGap?: number;
		tone?:
			| 'default'
			| 'borderless'
			| 'pennant'
			| 'clubhouse'
			| 'field'
			| 'panel'
			| 'highlight'
			| 'inset'
			| 'inset-pennant'
			| 'dashed'
			| 'tile';
		/** A dot of this color token beside the title. */
		dot?: 'sage' | 'field' | 'pennant' | 'ink' | 'muted';
		/** Px between the header and each child, as a flex column. */
		stack?: number;
		/** An accessible name for a card with no title. */
		label?: string;
		/** The card's <section>, bindable. */
		element?: HTMLElement | null;
		headAlign?: 'baseline' | 'center';
		headSpacing?: string;
		phonePadding?: string;
		phoneTitleSize?: number;
		phoneHeadGap?: number;
		phoneHeadStack?: boolean;
		phoneHeadStackGap?: number;
		phoneHeadAlign?: 'baseline' | 'center';
		/** A 2px ink rule under the header row. */
		headRule?: boolean;
		/** Drawn 10px left of the title. */
		icon?: Snippet;
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
		padding = '22px 24px',
		headGap = 12,
		tone = 'default',
		dot,
		stack,
		label,
		element = $bindable(null),
		headAlign = 'baseline',
		headSpacing = '8px 12px',
		phonePadding = '16px',
		phoneTitleSize = 22,
		phoneHeadGap,
		phoneHeadStack = false,
		phoneHeadStackGap = 4,
		phoneHeadAlign = 'baseline',
		headRule = false,
		icon,
		class: className = '',
		action,
		children
	}: Props = $props();

	const headingId = $props.id();
</script>

<section
	bind:this={element}
	class="card {tone} {className}"
	class:flush
	class:stack={stack != null}
	style:--card-stack={stack == null ? undefined : `${stack}px`}
	style:--card-pad={padding}
	style:--card-pad-phone={phonePadding}
	style:--card-title={`${titleSize}px`}
	style:--card-title-phone={`${phoneTitleSize}px`}
	style:--card-head-gap={`${headGap}px`}
	style:--card-head-gap-phone={`${phoneHeadGap ?? headGap}px`}
	style:--card-head-align={headAlign}
	style:--card-head-spacing={headSpacing}
	style:--card-head-stack-gap={`${phoneHeadStackGap}px`}
	aria-labelledby={title ? `card-${headingId}` : undefined}
	aria-label={title ? undefined : label}
>
	{#if title || action}
		<div
			class="head"
			class:stack-phone={phoneHeadStack}
			class:center-phone={phoneHeadAlign === 'center'}
			class:rule={headRule}
		>
			{#if title && icon}
				<div class="title-row">
					{@render icon()}
					<svelte:element this={`h${level}`} id="card-{headingId}" class="title">
						{title}
					</svelte:element>
				</div>
			{:else if title && dot}
				<div class="title-row">
					<span class="dot" aria-hidden="true" style:background="var(--color-{dot})"></span>
					<svelte:element this={`h${level}`} id="card-{headingId}" class="title">
						{title}
					</svelte:element>
				</div>
			{:else if title}
				<svelte:element this={`h${level}`} id="card-{headingId}" class="title">
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
		padding: var(--card-pad);
		min-width: 0;
	}

	.card.flush {
		padding: 0;
	}

	.card.borderless {
		border: 0;
	}

	.card.pennant {
		background: var(--color-pennant-tint);
		border-color: var(--color-pennant-line);
	}

	.card.field {
		background: var(--color-field-tint);
		border-color: var(--color-field-line);
	}

	.card.panel {
		border-color: var(--color-line-soft);
		border-radius: 10px;
	}

	.card.highlight {
		border: 2px solid var(--color-field);
	}

	.card.inset {
		border-radius: 10px;
	}

	.card.inset-pennant {
		border-color: var(--color-pennant-line);
		border-radius: 10px;
		background: var(--color-pennant-wash);
	}

	.card.dashed {
		border: 1.5px dashed var(--color-line-strong);
		border-radius: 10px;
		background: transparent;
		text-align: center;
		font-size: 15px;
		color: var(--color-ink-2);
	}

	/* A flex box, so a link inside it fills the tile when a grid row
	   stretches it to a neighbor's height. */
	.card.tile {
		display: flex;
		border: 0;
		border-radius: 10px;
	}

	.card.tile > :global(*) {
		flex: 1;
		min-width: 0;
	}

	.card.stack {
		display: flex;
		flex-direction: column;
		gap: var(--card-stack);
	}

	.card.stack > .head {
		margin-bottom: 0;
	}

	.title-row {
		display: flex;
		align-items: center;
		gap: 10px;
		min-width: 0;
	}

	.dot {
		flex: none;
		width: 10px;
		height: 10px;
		border-radius: 50%;
	}

	.card.clubhouse {
		border: 0;
		background: var(--color-clubhouse);
		color: var(--color-white);
	}

	.head {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		align-items: var(--card-head-align);
		gap: var(--card-head-spacing);
		margin-bottom: var(--card-head-gap);
	}

	.flush .head {
		padding: 22px 24px 0;
	}

	.head.rule {
		margin-bottom: 0;
		padding-bottom: 10px;
		border-bottom: 2px solid var(--color-ink);
	}

	/* A flex box, so whatever sits in the action slot is as tall as its own
	   line and no taller. As a plain block, it put a line of the card's own
	   size around a 13px caption, so a stacked phone header came out 4px
	   taller than the boards, which draw the caption as a bare flex item.
	   Its baseline is its first item's, so the header still aligns on it. */
	.action {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		min-width: 0;
	}

	.title {
		margin: 0;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: var(--card-title);
	}

	@media (max-width: 639.98px) {
		.card {
			padding: var(--card-pad-phone);
		}

		.card.flush {
			padding: 0;
		}

		.flush .head {
			padding: 16px 16px 0;
		}

		.head {
			margin-bottom: var(--card-head-gap-phone);
		}

		.head.rule {
			margin-bottom: 0;
		}

		.head.stack-phone {
			flex-direction: column;
			align-items: flex-start;
			gap: var(--card-head-stack-gap);
		}

		.head.center-phone {
			align-items: center;
		}

		.title {
			font-size: var(--card-title-phone);
		}
	}
</style>

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
	 * - `headAlign`, `headSpacing`: the header row's alignment and gap, when a
	 *   board draws a control beside the title (This Season's Final standings:
	 *   center, "10px 16px")
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
		tone?: 'default' | 'borderless' | 'pennant' | 'clubhouse';
		headAlign?: 'baseline' | 'center';
		headSpacing?: string;
		phonePadding?: string;
		phoneTitleSize?: number;
		phoneHeadGap?: number;
		phoneHeadStack?: boolean;
		phoneHeadStackGap?: number;
		phoneHeadAlign?: 'baseline' | 'center';
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
		headAlign = 'baseline',
		headSpacing = '8px 12px',
		phonePadding = '16px',
		phoneTitleSize = 22,
		phoneHeadGap,
		phoneHeadStack = false,
		phoneHeadStackGap = 4,
		phoneHeadAlign = 'baseline',
		class: className = '',
		action,
		children
	}: Props = $props();

	const headingId = $props.id();
</script>

<section
	class="card {tone} {className}"
	class:flush
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
>
	{#if title || action}
		<div
			class="head"
			class:stack-phone={phoneHeadStack}
			class:center-phone={phoneHeadAlign === 'center'}
		>
			{#if title}
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

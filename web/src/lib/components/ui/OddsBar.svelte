<script lang="ts">
	/**
	 * A percentage bar with its number to the right (System board, "Odds bar"):
	 * an 8px field-tint track with 4px corners, a field fill, 10px to a weight
	 * 600 number. This Season draws the teams below the playoff line in sage;
	 * that's `tone="sage"`.
	 *
	 * The Trade Center's lopsided rows (WP8 S8.2) draw a bare bar: `track` is
	 * "pennant" for a pennant-tint track, and `bare` drops the number.
	 *
	 * The Constitution's vote count (WP11 S11.6) draws a 10px bar on a wash
	 * track: `height` sets the bar's height in px (its corners are half that),
	 * `track="wash"`, and children replace the number with a label of the
	 * page's own, each of its parts a flex item `gap` px apart ("1 of 7 yes",
	 * "Super-majority"). `phoneGap` is the gap below 640px.
	 */
	import type { Snippet } from 'svelte';
	import { clampPercent, formatPercent } from './parts';

	interface Props {
		/** 0 to 100. Values outside are held to the ends. */
		percent: number;
		tone?: 'field' | 'sage';
		/** The number to print, when the page formats it itself. */
		text?: string;
		/** A fixed width for the number, right aligned (This Season's 38px). */
		numberWidth?: number;
		/** Px between the bar and the number: 10 unless the board draws another (8 on the Hall of Fame phone board). */
		gap?: number;
		/** The track's fill: field-tint unless the board draws pennant-tint. */
		track?: 'field' | 'pennant' | 'wash';
		/** The bar's height in px: 8 unless the board draws another. */
		height?: number;
		/** Px between the parts below 640px. */
		phoneGap?: number;
		/** A label in place of the number. */
		children?: Snippet;
		/** No number beside the bar. */
		bare?: boolean;
		/** An accessible name, for a bare bar. */
		label?: string;
	}

	let {
		percent,
		tone = 'field',
		text,
		numberWidth,
		gap,
		track = 'field',
		bare = false,
		label,
		height,
		phoneGap,
		children
	}: Props = $props();

	const width = $derived(clampPercent(percent));
</script>

<span
	class="odds"
	style:--odds-gap={gap === undefined ? undefined : `${gap}px`}
	style:--odds-phone-gap={phoneGap === undefined ? undefined : `${phoneGap}px`}
	style:--odds-height={height === undefined ? undefined : `${height}px`}
>
	<span class="track track-{track}" aria-hidden="true"
		><span class="fill {tone}" style:width="{width}%"></span></span
	>
	{#if children}{@render children()}{:else if bare}{#if label}<span class="sr-only">{label}</span>{/if}{:else}<span
		class="number"
		class:set-width={numberWidth !== undefined}
		style:width={numberWidth === undefined ? undefined : `${numberWidth}px`}
		>{text ?? formatPercent(percent)}</span
	>{/if}
</span>

<style>
	.odds {
		display: flex;
		align-items: center;
		gap: var(--odds-gap, 10px);
	}

	.track {
		flex: 1;
		height: var(--odds-height, 8px);
		border-radius: calc(var(--odds-height, 8px) / 2);
		background: var(--color-field-tint);
	}

	.track-wash {
		background: var(--color-wash);
		overflow: hidden;
	}

	.track-wash .fill {
		border-radius: 0;
	}

	@media (max-width: 639.98px) {
		.odds {
			gap: var(--odds-phone-gap, var(--odds-gap, 10px));
		}
	}

	/* The board clips a square-ended fill in a rounded track. */
	.track-pennant {
		background: var(--color-pennant-tint);
		overflow: hidden;
	}

	.track-pennant .fill {
		border-radius: 0;
	}

	.fill {
		display: block;
		height: var(--odds-height, 8px);
		border-radius: calc(var(--odds-height, 8px) / 2);
	}

	.field {
		background: var(--color-field);
	}

	.sage {
		background: var(--color-sage);
	}

	.number {
		font-weight: 600;
	}

	.number.set-width {
		flex: none;
		text-align: right;
	}
</style>

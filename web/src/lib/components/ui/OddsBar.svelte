<script lang="ts">
	/**
	 * A percentage bar with its number to the right (System board, "Odds bar"):
	 * an 8px field-tint track with 4px corners, a field fill, 10px to a weight
	 * 600 number. This Season draws the teams below the playoff line in sage;
	 * that's `tone="sage"`.
	 *
	 * The Trade Center's lopsided rows (WP8 S8.2) draw a bare bar: `track` is
	 * "pennant" for a pennant-tint track, and `bare` drops the number.
	 */
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
		track?: 'field' | 'pennant';
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
		label
	}: Props = $props();

	const width = $derived(clampPercent(percent));
</script>

<span class="odds" style:gap={gap === undefined ? undefined : `${gap}px`}>
	<span class="track track-{track}" aria-hidden="true"
		><span class="fill {tone}" style:width="{width}%"></span></span
	>
	{#if bare}{#if label}<span class="sr-only">{label}</span>{/if}{:else}<span
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
		gap: 10px;
	}

	.track {
		flex: 1;
		height: 8px;
		border-radius: 4px;
		background: var(--color-field-tint);
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
		height: 8px;
		border-radius: 4px;
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

<script lang="ts">
	/**
	 * A row of StatTiles in one bordered card (This Season, under the header):
	 * auto-fit columns of at least 200px, 1px line rules between the tiles.
	 * `twoUpOnPhones` holds it to two columns below 640px (Home's phone strip),
	 * with the phone board's dividers: a right rule on the left column and a
	 * bottom rule on the first row. `stackOnPhones` puts one tile per row below
	 * 640px (the profile's Head to head strip), whatever the width.
	 *
	 * `minColumn` is the narrowest a column gets before the tiles wrap (200px
	 * unless the board draws another: the Hall of Fame's strip is 170px).
	 * `phoneRowRules="all"` draws the two-up bottom rule under every row but the
	 * last, as the Hall of Fame phone board does, instead of the first row only.
	 *
	 *   <StatStrip>
	 *     <StatTile label="Top seed" value="Omar" sub="3-0 · 412.6 PF" />
	 *     ...
	 *   </StatStrip>
	 */
	import { setContext, type Snippet } from 'svelte';
	import { STAT_STRIP } from './stat-strip';

	interface Props {
		label?: string;
		twoUpOnPhones?: boolean;
		stackOnPhones?: boolean;
		/** Px: the narrowest a column gets before the tiles wrap. */
		minColumn?: number;
		/** Two-up bottom rules: under the first row, or under every row but the last. */
		phoneRowRules?: 'first' | 'all';
		children: Snippet;
	}

	let {
		label,
		twoUpOnPhones = false,
		stackOnPhones = false,
		minColumn = 200,
		phoneRowRules = 'first',
		children
	}: Props = $props();

	setContext(STAT_STRIP, true);
</script>

<div
	class="strip"
	class:two-up={twoUpOnPhones}
	class:stack={stackOnPhones}
	class:all-rows={phoneRowRules === 'all'}
	style:--strip-min="{minColumn}px"
	role={label ? 'group' : undefined}
	aria-label={label}
>
	{@render children()}
</div>

<style>
	.strip {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(var(--strip-min, 200px), 1fr));
		background: var(--color-card);
		border: 1px solid var(--color-line);
		border-radius: var(--radius-card);
		overflow: hidden;
	}

	@media (max-width: 639.98px) {
		.strip.stack {
			grid-template-columns: minmax(0, 1fr);
		}

		.strip.two-up {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}

		/* Drawn rules, not the tiles' pulled-back ones, so the cells come out
		   the board's size. */
		.strip.two-up > :global(.tile) {
			margin: 0;
			border: 0;
		}

		.strip.two-up > :global(.tile:nth-child(odd)) {
			border-right: 1px solid var(--color-line);
		}

		.strip.two-up > :global(.tile:nth-child(-n + 2)) {
			border-bottom: 1px solid var(--color-line);
		}

		/* Every row but the last: rule them all, then take it off the last
		   row's left tile and the tile beside it, if there is one. */
		.strip.two-up.all-rows > :global(.tile) {
			border-bottom: 1px solid var(--color-line);
		}

		.strip.two-up.all-rows > :global(.tile:nth-child(odd):nth-last-child(-n + 2)),
		.strip.two-up.all-rows > :global(.tile:nth-child(odd):nth-last-child(-n + 2) + .tile) {
			border-bottom: 0;
		}
	}
</style>

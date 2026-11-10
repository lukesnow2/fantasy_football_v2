<script lang="ts">
	/**
	 * A row of StatTiles in one bordered card (This Season, under the header):
	 * auto-fit columns of at least 200px, 1px line rules between the tiles.
	 * `twoUpOnPhones` holds it to two columns below 640px (Home's phone strip),
	 * with the phone board's dividers: a right rule on the left column and a
	 * bottom rule on the first row. `stackOnPhones` puts one tile per row below
	 * 640px (the profile's Head to head strip), whatever the width.
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
		children: Snippet;
	}

	let { label, twoUpOnPhones = false, stackOnPhones = false, children }: Props = $props();

	setContext(STAT_STRIP, true);
</script>

<div
	class="strip"
	class:two-up={twoUpOnPhones}
	class:stack={stackOnPhones}
	role={label ? 'group' : undefined}
	aria-label={label}
>
	{@render children()}
</div>

<style>
	.strip {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
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
	}
</style>

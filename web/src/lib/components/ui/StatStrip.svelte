<script lang="ts">
	/**
	 * A row of StatTiles in one bordered card (This Season, under the header):
	 * auto-fit columns of at least 200px, 1px line rules between the tiles.
	 * `twoUpOnPhones` holds it to two columns below 640px (Home's phone strip).
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
		children: Snippet;
	}

	let { label, twoUpOnPhones = false, children }: Props = $props();

	setContext(STAT_STRIP, true);
</script>

<div
	class="strip"
	class:two-up={twoUpOnPhones}
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
		.strip.two-up {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
	}
</style>

<script lang="ts">
	/**
	 * A row of StatTiles in one bordered card (This Season, under the header):
	 * auto-fit columns of at least 200px, 1px line rules between the tiles.
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
		children: Snippet;
	}

	let { label, children }: Props = $props();

	setContext(STAT_STRIP, true);
</script>

<div class="strip" role={label ? 'group' : undefined} aria-label={label}>
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
</style>

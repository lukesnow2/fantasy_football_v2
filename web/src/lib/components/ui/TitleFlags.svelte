<script lang="ts">
	/**
	 * A manager's titles as a row of pennants (Managers board's rail and hero;
	 * WP7's Hall of Fame uses it too). `count` copies of the board's 12 by 18
	 * pennant, drawn at `width` by `height`, with the staff in `pole` and the
	 * flag in `flag` (token names, such as "ink" or "pennant-light"), `gap` px
	 * apart. The row is one image, "3 titles" ("1 title"), and draws nothing
	 * at zero.
	 *
	 *   <TitleFlags count={3} width={14} height={20} pole="white" flag="pennant-light" gap={4} />
	 */
	import Pennant from './Pennant.svelte';

	interface Props {
		count: number;
		width?: number;
		height?: number;
		/** The staff's color, a token name. */
		pole?: string;
		/** The flag's color, a token name. */
		flag?: string;
		/** The staff's width: 1.5, or 1.8 on the rail. */
		stroke?: number;
		/** Px between flags. */
		gap?: number;
		/** Override the label, as the Title games rows do ("Won"). */
		label?: string;
	}

	let {
		count,
		width = 12,
		height = 18,
		pole = 'ink',
		flag = 'pennant',
		stroke = 1.5,
		gap = 2,
		label
	}: Props = $props();

	const flags = $derived(Array.from({ length: Math.max(0, Math.floor(count)) }, (_, i) => i));
</script>

{#if flags.length > 0}
	<span
		class="flags"
		role="img"
		aria-label={label ?? `${flags.length} ${flags.length === 1 ? 'title' : 'titles'}`}
		style:gap="{gap}px"
	>
		{#each flags as i (i)}
			<Pennant {width} {height} {stroke} staff="var(--color-{pole})" fill="var(--color-{flag})" />
		{/each}
	</span>
{/if}

<style>
	.flags {
		display: inline-flex;
		align-items: center;
	}
</style>

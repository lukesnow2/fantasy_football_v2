<script lang="ts">
	/**
	 * A draft grade as a colored chip (Draft Central). Colors come from
	 * gradeChip in $lib/draft-display: A field with white, B field-tint with
	 * clubhouse, C wash with ink-2, D pennant-tint with pennant-dark, F pennant
	 * with white, a plus following its letter.
	 *
	 * - `square` (default): a `size` px square with 8px corners and the letter
	 *   in slab 700 at `fontSize` (34 and 20 in the career table, 40 and 20 on
	 *   the selected pick, 30 and 18 in the phone table)
	 * - `inline`: the board's column-head chip, at least 22px wide, 1px 4px of
	 *   padding, 5px corners, slab 700 13px
	 *
	 * A missing grade is EmptyValue, never a dash (G6).
	 */
	import EmptyValue from './EmptyValue.svelte';
	import { gradeChip } from '$lib/draft-display';

	interface Props {
		grade: string | null | undefined;
		shape?: 'square' | 'inline';
		size?: number;
		fontSize?: number;
		title?: string;
	}

	let { grade, shape = 'square', size = 34, fontSize = 20, title }: Props = $props();

	const colors = $derived(gradeChip(grade));
</script>

{#if colors}
	<span
		class="chip {shape}"
		{title}
		style:background="var(--color-{colors.bg})"
		style:color="var(--color-{colors.fg})"
		style:width={shape === 'square' ? `${size}px` : undefined}
		style:height={shape === 'square' ? `${size}px` : undefined}
		style:font-size={shape === 'square' ? `${fontSize}px` : undefined}>{grade}</span
	>
{:else}
	<EmptyValue />
{/if}

<style>
	.chip {
		font-family: var(--font-slab);
		font-weight: 700;
	}

	.square {
		display: inline-flex;
		flex: none;
		align-items: center;
		justify-content: center;
		border-radius: 8px;
	}

	.inline {
		display: inline-block;
		box-sizing: content-box;
		min-width: 22px;
		padding: 1px 4px;
		border-radius: 5px;
		font-size: 13px;
		text-align: center;
	}
</style>

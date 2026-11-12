<script lang="ts">
	/**
	 * A rank change (System board, "Movement"): "▲ 4" in field, "▼ 2" in
	 * pennant, "Same" in muted, from a signed number. Screen readers hear "Up 4"
	 * and "Down 2" rather than the triangle's name.
	 *
	 * `inherit` takes size, weight and font from the parent instead of 600, for
	 * a board that draws movement in other type: Power Rankings' Move column
	 * (13px, 700) and its callouts (slab, 700, 30px).
	 */
	import EmptyValue from './EmptyValue.svelte';
	import { movement } from './parts';

	interface Props {
		/**
		 * Places gained (positive) or lost (negative). Null, or a value that isn't
		 * a finite number (a subtraction with a missing week), renders "None".
		 */
		change: number | null | undefined;
		/** Use the parent's font, size and weight. */
		inherit?: boolean;
	}

	let { change, inherit = false }: Props = $props();

	const move = $derived(change == null || !Number.isFinite(change) ? null : movement(change));
</script>

{#if move === null}
	<EmptyValue />
{:else if move.direction === 'same'}
	<span class="movement same" class:inherit>{move.text}</span>
{:else}
	<span class="movement {move.direction}" class:inherit
		><span aria-hidden="true">{move.direction === 'up' ? '▲' : '▼'}</span><span class="sr-only"
			>{move.direction === 'up' ? 'Up' : 'Down'}</span
		>
		{move.text}</span
	>
{/if}

<style>
	.movement {
		position: relative;
		font-weight: 600;
		white-space: nowrap;
	}

	.movement.inherit {
		font-weight: inherit;
	}

	.up {
		color: var(--color-field);
	}

	.down {
		color: var(--color-pennant);
	}

	.same {
		color: var(--color-muted);
	}
</style>

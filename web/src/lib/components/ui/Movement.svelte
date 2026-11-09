<script lang="ts">
	/**
	 * A rank change (System board, "Movement"): "▲ 4" in field, "▼ 2" in
	 * pennant, "Same" in muted, from a signed number. Screen readers hear "Up 4"
	 * and "Down 2" rather than the triangle's name.
	 */
	import EmptyValue from './EmptyValue.svelte';
	import { movement } from './parts';

	interface Props {
		/**
		 * Places gained (positive) or lost (negative). Null, or a value that isn't
		 * a finite number (a subtraction with a missing week), renders "None".
		 */
		change: number | null | undefined;
	}

	let { change }: Props = $props();

	const move = $derived(change == null || !Number.isFinite(change) ? null : movement(change));
</script>

{#if move === null}
	<EmptyValue />
{:else if move.direction === 'same'}
	<span class="movement same">{move.text}</span>
{:else}
	<span class="movement {move.direction}"
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

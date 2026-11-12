<script lang="ts">
	/**
	 * The 56px tile at the top of a sign-in status card (the Sign in board):
	 * field-tint with a field icon for "Check your email", pennant-tint with a
	 * pennant-dark icon for a link problem.
	 */
	import type { Snippet } from 'svelte';

	let { tone = 'field', children }: { tone?: 'field' | 'pennant'; children: Snippet } = $props();
</script>

<div class="tile {tone}" aria-hidden="true">{@render children()}</div>

<style>
	.tile {
		width: 56px;
		height: 56px;
		border-radius: 14px;
		display: inline-flex;
		align-items: center;
		justify-content: center;
	}

	.field {
		background: var(--color-field-tint);
		color: var(--color-field);
	}

	.pennant {
		background: var(--color-pennant-tint);
		color: var(--color-pennant-dark);
	}
</style>

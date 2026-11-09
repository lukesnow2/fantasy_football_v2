<script lang="ts" module>
	export type TagVariant = 'clinched' | 'bye' | 'eliminated' | 'champion' | 'champion-strong';
</script>

<script lang="ts">
	/**
	 * A status tag (System board, "Status tags"). Clinched is field with white
	 * text, Bye is field-tint with clubhouse text, Eliminated is wash with ink-2,
	 * Champion is pennant-tint with pennant-dark. `champion-strong` is the pennant
	 * fill the after-the-final board draws for the winner.
	 *
	 * `compact` is the in-row size from the after-the-final standings: 2px 8px of
	 * padding and no letter spacing.
	 */
	import type { Snippet } from 'svelte';

	interface Props {
		variant: TagVariant;
		compact?: boolean;
		children: Snippet;
	}

	let { variant, compact = false, children }: Props = $props();
</script>

<span class="tag {variant}" class:compact>{@render children()}</span>

<style>
	.tag {
		display: inline-block;
		padding: 4px 10px;
		border-radius: var(--radius-tag);
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.04em;
		white-space: nowrap;
	}

	.compact {
		padding: 2px 8px;
		letter-spacing: normal;
	}

	.clinched {
		background: var(--color-field);
		color: var(--color-white);
	}

	.bye {
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
	}

	.eliminated {
		background: var(--color-wash);
		color: var(--color-ink-2);
	}

	.champion {
		background: var(--color-pennant-tint);
		color: var(--color-pennant-dark);
	}

	.champion-strong {
		background: var(--color-pennant);
		color: var(--color-white);
	}
</style>

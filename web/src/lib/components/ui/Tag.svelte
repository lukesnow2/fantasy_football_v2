<script lang="ts" module>
	export type TagVariant =
		| 'clinched'
		| 'bye'
		| 'eliminated'
		| 'champion'
		| 'champion-strong'
		| 'last-place';
</script>

<script lang="ts">
	/**
	 * A status tag (System board, "Status tags"). Clinched is field with white
	 * text, Bye is field-tint with clubhouse text, Eliminated is wash with ink-2,
	 * Champion is pennant-tint with pennant-dark. `champion-strong` is the pennant
	 * fill the after-the-final board draws for the winner, and `last-place` is
	 * the ink fill for the Last Place Trophy (WP4 S4.9's Finish tag).
	 *
	 * System's size is 4px 10px, 12px weight 700, 0.04em, at the normal line
	 * height whatever the row around it sets. This Season's rows draw two more
	 * (WP4 S4.5, S4.9): `size="row"` is 2px 8px with no letter spacing (Recent
	 * moves, the Finish tag), and `size="small"` is 1px 6px at 11px (the Bye tag
	 * in Playoff Field).
	 */
	import type { Snippet } from 'svelte';

	interface Props {
		variant: TagVariant;
		size?: 'default' | 'row' | 'small';
		children: Snippet;
	}

	let { variant, size = 'default', children }: Props = $props();
</script>

<span class="tag {variant} size-{size}">{@render children()}</span>

<style>
	.tag {
		display: inline-block;
		padding: 4px 10px;
		border-radius: var(--radius-tag);
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.04em;
		line-height: normal;
		white-space: nowrap;
	}

	.size-row {
		padding: 2px 8px;
		letter-spacing: normal;
	}

	.size-small {
		padding: 1px 6px;
		font-size: 11px;
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

	.last-place {
		background: var(--color-ink);
		color: var(--color-white);
	}
</style>

<script lang="ts" module>
	export type TagVariant =
		| 'clinched'
		| 'bye'
		| 'eliminated'
		| 'champion'
		| 'champion-strong'
		| 'last-place'
		| 'playoffs'
		| 'missed'
		| 'in-progress'
		| 'runner-up'
		| 'band-champion'
		| 'band'
		| 'final'
		| 'pending'
		| 'steal'
		| 'value'
		| 'letdown'
		| 'bust';
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
	 *
	 * The manager pages (WP6) add season outcomes, title-game results and the
	 * pill chips:
	 * - `playoffs` field-tint with clubhouse, `missed` wash with muted,
	 *   `in-progress` ground with ink-2 (Season history's Outcome), and
	 *   `runner-up` pennant-tint with pennant-dark (Title games)
	 * - `band-champion` pennant-light with clubhouse, and `band` clubhouse-2
	 *   with on-clubhouse and a 1px inset field-2 ring: chips on the clubhouse
	 *   card (the profile header)
	 * - sizes `chip` (6px 12px, 13px), `chip-small` (4px 10px, 12px) and
	 *   `chip-badge` (4px 10px, 13px) are 999px pills with no letter spacing
	 * - `caps`: uppercase at 0.06em (the Managers hero's tier chip)
	 * - `wrapOnPhone`: below 640px the text may wrap inside the tag, at a 16px
	 *   line height, as Season history's phone table draws "In progress · 61%
	 *   playoff odds"
	 *
	 * Trades and the draft (WP8) add:
	 * - `final` field-tint with clubhouse and `pending` wash with ink-2: a
	 *   trade's status in Recent trades, at `size="trade"` (2px 8px, 13px)
	 * - the draft board's four tags, each with a 1px border: `steal` field with
	 *   white, `value` card with field text and a field border, `letdown` card
	 *   with pennant-dark text and a pennant border, `bust` ink with white. Two
	 *   sizes, both with 4px corners: `sample` (1px 6px, 11px) inside a Show
	 *   tags chip, and `cell` (0 5px, 10px) on a board cell
	 */
	import type { Snippet } from 'svelte';

	interface Props {
		variant: TagVariant;
		size?:
			| 'default'
			| 'row'
			| 'small'
			| 'chip'
			| 'chip-small'
			| 'chip-badge'
			| 'trade'
			| 'sample'
			| 'cell';
		caps?: boolean;
		wrapOnPhone?: boolean;
		children: Snippet;
	}

	let { variant, size = 'default', caps = false, wrapOnPhone = false, children }: Props = $props();
</script>

<span class="tag {variant} size-{size}" class:caps class:wrap-phone={wrapOnPhone}
	>{@render children()}</span
>

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

	.size-chip,
	.size-chip-small,
	.size-chip-badge {
		border-radius: 999px;
		letter-spacing: normal;
	}

	.size-chip {
		padding: 6px 12px;
		font-size: 13px;
	}

	.size-chip-small {
		padding: 4px 10px;
		font-size: 12px;
	}

	.size-chip-badge {
		padding: 4px 10px;
		font-size: 13px;
	}

	.size-trade {
		padding: 2px 8px;
		font-size: 13px;
		letter-spacing: normal;
	}

	.size-sample,
	.size-cell {
		border: 1px solid transparent;
		border-radius: 4px;
		letter-spacing: normal;
	}

	.size-sample {
		padding: 1px 6px;
		font-size: 11px;
	}

	.size-cell {
		padding: 0 5px;
		font-size: 10px;
	}

	.caps {
		letter-spacing: 0.06em;
		text-transform: uppercase;
	}

	@media (max-width: 639.98px) {
		.wrap-phone {
			white-space: normal;
			line-height: 16px;
		}
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

	.playoffs {
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
	}

	.missed {
		background: var(--color-wash);
		color: var(--color-muted);
	}

	.in-progress {
		background: var(--color-ground);
		color: var(--color-ink-2);
	}

	.runner-up {
		background: var(--color-pennant-tint);
		color: var(--color-pennant-dark);
	}

	.band-champion {
		background: var(--color-pennant-light);
		color: var(--color-clubhouse);
	}

	.final {
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
	}

	.pending {
		background: var(--color-wash);
		color: var(--color-ink-2);
	}

	.steal {
		background: var(--color-field);
		border-color: var(--color-field);
		color: var(--color-white);
	}

	.value {
		background: var(--color-card);
		border-color: var(--color-field);
		color: var(--color-field);
	}

	.letdown {
		background: var(--color-card);
		border-color: var(--color-pennant);
		color: var(--color-pennant-dark);
	}

	.bust {
		background: var(--color-ink);
		border-color: var(--color-ink);
		color: var(--color-white);
	}

	.band {
		background: var(--color-clubhouse-2);
		color: var(--color-on-clubhouse);
		box-shadow: inset 0 0 0 1px var(--color-field-2);
	}
</style>

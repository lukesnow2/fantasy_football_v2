<script lang="ts">
	/**
	 * One stat: a 13px label, a slab value, a 14px ink-2 line under it.
	 *
	 * On its own it's the System board's "Stat cell": a 1px line box with 10px
	 * corners, 16px 18px of padding and a 44px value. Inside a StatStrip it takes
	 * This Season's strip measurements instead: no box of its own, 20px 22px of
	 * padding and a 38px value.
	 *
	 * Give it a `metricId` and the label becomes a StatTip.
	 *
	 * `compact` is This Season's phone strip (WP4 S4.10): value first in slab
	 * 24px, then the label in 12px muted, 12px 14px of padding.
	 *
	 * `tight` with `compact` is the Trade Center phone strip (WP8 S8.10): the
	 * value at line-height 1 and the label 4px under it.
	 *
	 * `small` is the Hall of Fame strip (WP7 S7.2), value first: 16px 20px of
	 * padding, a 30px value and the 13px ink-2 label 6px under it; on phones
	 * 12px 14px, a 24px value and the label in 12px, its StatTip a 44px tap
	 * target that doesn't grow the cell.
	 *
	 * `layout="avatar"` is the profile Head to head strip (WP6 S6.7): an
	 * optional 40px Avatar (`avatar`, a manager's name) 12px left of the text,
	 * 18px 22px of padding, a 28px value 4px under the label and the sub 4px
	 * under that; on phones 14px 16px and a 24px value. `tone` colors the value
	 * (field, pennant, pennant-dark or ink), and a null value is EmptyValue
	 * "None".
	 *
	 * League History (WP10 S10.5, S10.6) adds:
	 * - `dense`, value first: 18px 22px of padding, a 36px value at line
	 *   height 1 and the 14px ink-2 label 6px under it; on phones the compact
	 *   cell (12px 14px, a 24px value, the label in 12px muted)
	 * - `layout="record"`: a 36px Avatar 12px left of the text, 16px 20px of
	 *   padding, the 13px muted label, then the 24px value at line height 1.1
	 *   with `holder` after it in 14px weight 600 sans; on phones 12px 14px, a
	 *   32px Avatar 10px from the text, a 12px label and a 22px value
	 */
	import { getContext } from 'svelte';
	import Avatar from './Avatar.svelte';
	import EmptyValue from './EmptyValue.svelte';
	import StatTip from './StatTip.svelte';
	import { STAT_STRIP } from './stat-strip';

	interface Props {
		label: string;
		value: string | number | null;
		sub?: string;
		metricId?: string;
		valueFirst?: boolean;
		compact?: boolean;
		/** With compact: a line-height 1 value and the label 4px under it. */
		tight?: boolean;
		small?: boolean;
		/** Value first at League History's head to head strip sizes. */
		dense?: boolean;
		layout?: 'stack' | 'avatar' | 'record';
		avatar?: string | null;
		/** With layout="record": the manager after the value. */
		holder?: string;
		tone?: 'ink' | 'field' | 'pennant' | 'pennant-dark';
	}

	let {
		label,
		value,
		sub,
		metricId,
		valueFirst = false,
		compact = false,
		tight = false,
		small = false,
		dense = false,
		layout = 'stack',
		avatar,
		holder,
		tone
	}: Props = $props();

	const inStrip = getContext<boolean>(STAT_STRIP) === true;
</script>

{#snippet labelText()}
	{#if metricId}<StatTip
			{metricId}
			{label}
			phoneTap={small ? 'inline-small' : undefined}
		/>{:else}{label}{/if}
{/snippet}

{#snippet valueText()}
	{#if value == null}<EmptyValue />{:else}{value}{/if}
{/snippet}

{#if layout === 'record'}
	<div class="tile record-tile" class:in-strip={inStrip}>
		{#if avatar}<Avatar name={avatar} size={36} phoneSize={32} alt="" />{/if}
		<div class="text">
			<div class="record-label">{@render labelText()}</div>
			<div class="record-value">
				{@render valueText()}{#if holder}{' '}<span class="holder">{holder}</span>{/if}
			</div>
		</div>
	</div>
{:else if layout === 'avatar'}
	<div class="tile avatar-tile" class:in-strip={inStrip}>
		{#if avatar}<Avatar name={avatar} size={40} alt="" />{/if}
		<div class="text">
			<div class="label">{@render labelText()}</div>
			<div class="value" style:color={tone ? `var(--color-${tone})` : undefined}>
				{@render valueText()}
			</div>
			{#if sub}<div class="sub">{sub}</div>{/if}
		</div>
	</div>
{:else}
	<div
		class="tile"
		class:in-strip={inStrip}
		class:value-first={valueFirst || compact || small || dense}
		class:compact
		class:dense
		class:tight={compact && tight}
		class:small
	>
		{#if valueFirst || compact || small || dense}
			<div class="value" style:color={tone ? `var(--color-${tone})` : undefined}>
				{@render valueText()}
			</div>
			<div class="first-label">{@render labelText()}</div>
		{:else}
			<div class="label">{@render labelText()}</div>
			<div class="value" style:color={tone ? `var(--color-${tone})` : undefined}>
				{@render valueText()}
			</div>
		{/if}
		{#if sub}<div class="sub">{sub}</div>{/if}
	</div>
{/if}

<style>
	.tile {
		border: 1px solid var(--color-line);
		border-radius: 10px;
		padding: 16px 18px;
		min-width: 0;
	}

	.label {
		font-size: 13px;
		font-weight: 600;
		color: var(--color-muted);
	}

	.value {
		margin-top: 4px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 44px;
		line-height: 1;
		color: var(--color-ink);
	}

	.sub {
		margin-top: 4px;
		font-size: 14px;
		color: var(--color-ink-2);
	}

	/* This Season's strip. The divider trick: every tile draws a rule on its
	   left and top and is pulled back 1px over the tile before it, so the rule
	   lands on that tile's last pixel, where the board draws it. The strip clips
	   the rules on its own edge, so they sit only between tiles however the
	   columns wrap. */
	.tile.in-strip {
		border: 0;
		border-radius: 0;
		border-left: 1px solid var(--color-line);
		border-top: 1px solid var(--color-line);
		margin: -1px 0 0 -1px;
		padding: 20px 22px;
	}

	.in-strip .value {
		margin-top: 6px;
		font-size: 38px;
	}

	.in-strip .sub {
		margin-top: 6px;
	}

	.value-first .value {
		margin-top: 0;
		font-size: 40px;
	}

	.first-label {
		margin-top: 6px;
		font-size: 14px;
		color: var(--color-ink-2);
	}

	@media (max-width: 639.98px) {
		.value-first .value {
			font-size: 28px;
		}

		.value-first .first-label {
			font-size: 13px;
		}

		.in-strip.value-first {
			padding: 14px 16px;
		}
	}

	/* The profile Head to head strip. */
	.avatar-tile {
		display: flex;
		align-items: center;
		gap: 12px;
	}

	.avatar-tile .text {
		min-width: 0;
	}

	.tile.avatar-tile.in-strip {
		padding: 18px 22px;
	}

	.avatar-tile .value,
	.avatar-tile.in-strip .value {
		margin-top: 4px;
		font-size: 28px;
	}

	.avatar-tile .sub,
	.avatar-tile.in-strip .sub {
		margin-top: 4px;
	}

	@media (max-width: 639.98px) {
		/* One tile per row (StatStrip's stackOnPhones), each with a rule under
		   it but the last, as the phone board draws them. */
		.tile.avatar-tile.in-strip {
			margin: 0;
			border: 0;
			border-bottom: 1px solid var(--color-line);
			padding: 14px 16px;
		}

		.tile.avatar-tile.in-strip:last-child {
			border-bottom: 0;
		}

		.avatar-tile .value,
		.avatar-tile.in-strip .value {
			font-size: 24px;
		}
	}

	/* After the strip and phone rules above, at the same specificity, so it
	   wins at every width. */
	.tile.compact {
		padding: 12px 14px;
	}

	.compact .value {
		font-size: 24px;
		line-height: normal;
	}

	.compact .first-label {
		margin-top: 0;
		font-size: 12px;
		color: var(--color-muted);
	}

	.tight .value {
		line-height: 1;
	}

	.tight .first-label {
		margin-top: 4px;
	}

	/* League History's head to head strip. */
	.tile.dense {
		padding: 18px 22px;
	}

	.dense .value {
		font-size: 36px;
		line-height: 1;
	}

	.dense .first-label {
		margin-top: 6px;
		font-size: 14px;
		color: var(--color-ink-2);
	}

	@media (max-width: 639.98px) {
		.tile.dense {
			padding: 12px 14px;
		}

		.dense .value {
			font-size: 24px;
			line-height: normal;
		}

		.dense .first-label {
			margin-top: 0;
			font-size: 12px;
			color: var(--color-muted);
		}
	}

	/* League History's records strip. */
	.record-tile {
		display: flex;
		align-items: center;
		gap: 12px;
	}

	.tile.record-tile.in-strip {
		padding: 16px 20px;
	}

	.record-tile .text {
		min-width: 0;
	}

	.record-label {
		font-size: 13px;
		color: var(--color-muted);
	}

	.record-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 24px;
		line-height: 1.1;
		color: var(--color-ink);
	}

	.holder {
		font-family: var(--font-sans);
		font-size: 14px;
		font-weight: 600;
	}

	@media (max-width: 639.98px) {
		.record-tile {
			gap: 10px;
		}

		.tile.record-tile.in-strip {
			padding: 12px 14px;
		}

		.record-label {
			font-size: 12px;
		}

		.record-value {
			font-size: 22px;
		}
	}

	/* The Hall of Fame strip. */
	.tile.small {
		padding: 16px 20px;
	}

	.small .value {
		font-size: 30px;
	}

	.small .first-label {
		margin-top: 6px;
		font-size: 13px;
	}

	@media (max-width: 639.98px) {
		.tile.small {
			padding: 12px 14px;
		}

		.small .value {
			font-size: 24px;
			line-height: normal;
		}

		.small .first-label {
			margin-top: 0;
			font-size: 12px;
		}
	}
</style>

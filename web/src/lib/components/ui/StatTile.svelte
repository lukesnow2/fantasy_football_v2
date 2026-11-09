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
	 */
	import { getContext } from 'svelte';
	import StatTip from './StatTip.svelte';
	import { STAT_STRIP } from './stat-strip';

	interface Props {
		label: string;
		value: string | number;
		sub?: string;
		metricId?: string;
		valueFirst?: boolean;
	}

	let { label, value, sub, metricId, valueFirst = false }: Props = $props();

	const inStrip = getContext<boolean>(STAT_STRIP) === true;
</script>

{#snippet labelText()}
	{#if metricId}<StatTip {metricId} {label} />{:else}{label}{/if}
{/snippet}

<div class="tile" class:in-strip={inStrip} class:value-first={valueFirst}>
	{#if valueFirst}
		<div class="value">{value}</div>
		<div class="first-label">{@render labelText()}</div>
	{:else}
		<div class="label">{@render labelText()}</div>
		<div class="value">{value}</div>
	{/if}
	{#if sub}<div class="sub">{sub}</div>{/if}
</div>

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
</style>

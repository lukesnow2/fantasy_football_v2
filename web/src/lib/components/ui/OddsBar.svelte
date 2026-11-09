<script lang="ts">
	/**
	 * A percentage bar with its number to the right (System board, "Odds bar"):
	 * an 8px field-tint track with 4px corners, a field fill, 10px to a weight
	 * 600 number. This Season draws the teams below the playoff line in sage;
	 * that's `tone="sage"`.
	 */
	import { clampPercent, formatPercent } from './parts';

	interface Props {
		/** 0 to 100. Values outside are held to the ends. */
		percent: number;
		tone?: 'field' | 'sage';
		/** The number to print, when the page formats it itself. */
		text?: string;
	}

	let { percent, tone = 'field', text }: Props = $props();

	const width = $derived(clampPercent(percent));
</script>

<span class="odds">
	<span class="track" aria-hidden="true"
		><span class="fill {tone}" style:width="{width}%"></span></span
	>
	<span class="number">{text ?? formatPercent(percent)}</span>
</span>

<style>
	.odds {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	.track {
		flex: 1;
		height: 8px;
		border-radius: 4px;
		background: var(--color-field-tint);
	}

	.fill {
		display: block;
		height: 8px;
		border-radius: 4px;
	}

	.field {
		background: var(--color-field);
	}

	.sage {
		background: var(--color-sage);
	}

	.number {
		font-weight: 600;
	}
</style>

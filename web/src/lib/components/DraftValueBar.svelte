<script lang="ts">
	/**
	 * A draft value per season as a bar from the center (Draft Central's Career
	 * draft grades, WP8 S8.5): a 10px wash track with 5px corners and an ink
	 * tick at its middle; the fill runs right in field for a value of 0 or
	 * more, left in pennant below it, half the track per average pick and
	 * never past the end (draftValueBar in $lib/draft-display). The value sits
	 * in a 48px right-aligned cell, signed to two places.
	 */
	import { draftValueBar } from '$lib/draft-display';

	interface Props {
		value: number;
	}

	let { value }: Props = $props();

	const bar = $derived(draftValueBar(value));
</script>

<span class="value-bar">
	<span class="track" aria-hidden="true">
		<span
			class="fill"
			style:left="{bar.left}%"
			style:width="{bar.width}%"
			style:background="var(--color-{bar.tone})"
		></span>
		<span class="tick"></span>
	</span>
	<span class="number" style:color="var(--color-{bar.tone})">{bar.text}</span>
</span>

<style>
	.value-bar {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	.track {
		position: relative;
		display: block;
		flex: 1;
		height: 10px;
		border-radius: 5px;
		background: var(--color-wash);
	}

	.fill {
		position: absolute;
		top: 0;
		height: 10px;
		border-radius: 5px;
	}

	.tick {
		position: absolute;
		left: 50%;
		top: -3px;
		width: 1.5px;
		height: 16px;
		background: var(--color-ink);
	}

	.number {
		width: 48px;
		flex: none;
		text-align: right;
		font-weight: 600;
	}
</style>

<script lang="ts">
	/**
	 * A player's position as a chip (Draft Central).
	 *
	 * - `solid`: the position's `--color-pos-*` fill with white 12px 700 text,
	 *   3px 8px of padding, the tag radius (the selected pick)
	 * - `neutral`: wash with ink-2, 12px 700, 2px 6px of padding, 5px corners,
	 *   centered (Best value picks and Biggest busts, which don't use the
	 *   position colors)
	 *
	 * A position outside the six (the 2005 IDP picks) is muted on solid.
	 */
	import { positionKey } from '$lib/draft-display';

	interface Props {
		position: string | null | undefined;
		tone?: 'solid' | 'neutral';
	}

	let { position, tone = 'solid' }: Props = $props();

	const key = $derived(positionKey(position));
</script>

<span
	class="pos {tone}"
	style:background={tone === 'solid'
		? key
			? `var(--color-pos-${key})`
			: 'var(--color-muted)'
		: undefined}>{position ?? ''}</span
>

<style>
	.pos {
		display: inline-block;
		font-size: 12px;
		font-weight: 700;
		line-height: normal;
		white-space: nowrap;
	}

	.solid {
		padding: 3px 8px;
		border-radius: var(--radius-tag);
		color: var(--color-white);
	}

	.neutral {
		padding: 2px 6px;
		border-radius: 5px;
		background: var(--color-wash);
		color: var(--color-ink-2);
		text-align: center;
	}
</style>

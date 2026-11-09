<script lang="ts">
	/**
	 * One title's pennant (Home's banner wall): the board's 12 by 18 SVG, a
	 * 1.5px staff and a swallowtail flag. Decorative on its own; wrap a row of
	 * them in an element with role="img" and an aria-label ("3 titles").
	 *
	 * `staff` and `fill` take any CSS color, tokens by default. WP4's champion
	 * card uses white and pennant-light, drawn at 14 by 20.
	 */
	interface Props {
		staff?: string;
		fill?: string;
		width?: number;
		height?: number;
	}

	let {
		staff = 'var(--color-ink)',
		fill = 'var(--color-pennant)',
		width = 12,
		height = 18
	}: Props = $props();
</script>

<svg {width} {height} viewBox="0 0 12 18" aria-hidden="true" class="pennant">
	<path d="M1 1v16" style:stroke={staff} stroke-width="1.5" />
	<path d="M2 1.5h9L8 5.5l3 4H2z" {fill} />
</svg>

<style>
	.pennant {
		flex: none;
	}
</style>

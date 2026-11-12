<script lang="ts">
	/**
	 * Words that depend on the pointer (build spec, Phones): `hover` where the
	 * device can hover, `touch` on a touch screen, swapped by `(hover: none)`
	 * as the phone boards draw it.
	 *
	 *   <PointerCopy hover="Hover" touch="Tap" /> the ⓘ next to any stat
	 */
	interface Props {
		hover: string;
		touch: string;
	}

	let { hover, touch }: Props = $props();
</script>

<span class="hover">{hover}</span><span class="touch">{touch}</span>

<style>
	.touch {
		display: none;
	}

	@media (hover: none) {
		.hover {
			display: none;
		}

		.touch {
			display: inline;
		}
	}
</style>

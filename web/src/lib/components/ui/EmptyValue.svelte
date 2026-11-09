<script lang="ts">
	/**
	 * A missing value as a word in muted text, so no page prints a dash glyph
	 * for "nothing here" (build spec G6).
	 */
	interface Props {
		/** The word to show. "None" unless the board says something else. */
		text?: string;
	}

	let { text = 'None' }: Props = $props();
</script>

<span class="empty">{text}</span>

<style>
	.empty {
		color: var(--color-muted);
	}
</style>

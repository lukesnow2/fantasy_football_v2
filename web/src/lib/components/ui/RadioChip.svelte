<script lang="ts">
	/**
	 * A radio button as a pill chip (the Bet Board's "Commissioner's ruling",
	 * WP9 S9.6): a label around a native radio and its text, 6px 10px, a 1px
	 * line-strong border, 13px weight 600. The checked chip has a field border
	 * on field-tint. Below 640px it's at least 44px tall with 0 12px of padding,
	 * as the phone board draws it, content-box like the board's label.
	 *
	 *   <RadioChip name="outcome" value="push" label="Push" />
	 *
	 * Every other attribute (required, checked) goes through to the radio.
	 */
	import type { HTMLInputAttributes } from 'svelte/elements';

	type Props = {
		label: string;
		name: string;
		value: string;
	} & Omit<HTMLInputAttributes, 'type' | 'name' | 'value'>;

	let { label, name, value, ...rest }: Props = $props();
</script>

<label class="chip"><input type="radio" {name} {value} {...rest} /> {label}</label>

<style>
	.chip {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		padding: 6px 10px;
		border: 1px solid var(--color-line-strong);
		border-radius: 999px;
		font-size: 13px;
		font-weight: 600;
		color: var(--color-ink);
		cursor: pointer;
	}

	/* The native 13px radio with Chromium's own margins, as the board draws
	   it. The forms plugin's base styles redraw radios at 16px with no margin,
	   which made each chip 3px taller and wider than the board's. The chip's
	   own outline is the focus ring. */
	input {
		appearance: auto;
		width: 13px;
		height: 13px;
		margin: 3px 3px 0 5px;
		padding: 0;
		border: 0;
	}

	input:focus {
		box-shadow: none;
	}

	.chip:has(input:checked) {
		border-color: var(--color-field);
		background: var(--color-field-tint);
	}

	.chip:has(input:focus-visible) {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	@media (max-width: 639.98px) {
		.chip {
			box-sizing: content-box;
			min-height: 44px;
			padding: 0 12px;
		}
	}
</style>

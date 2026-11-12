<script lang="ts">
	/**
	 * A labeled text input, textarea or select for a form (the Bet Board's "Put
	 * it on the board", WP9 S9.2). The label wraps its control as a flex column:
	 * 14px weight 600 ink, 6px above the control. The control is 46px tall with
	 * 0 14px of padding (0 12px for a select), a 1px line-strong border, the
	 * control radius, card fill and 16px weight 400 ink text, with faint
	 * placeholders. A textarea is 12px 14px and resizes vertically. `helper`
	 * puts a 13px weight 500 muted line under the control.
	 *
	 *   <TextField label="Stake" name="stake" required placeholder="$20, or loser buys wings" />
	 *   <TextField kind="textarea" label="Terms" name="terms" rows={3} helper="Write it out properly now." />
	 *   <TextField kind="select" label="Against" name="counterpartyKey" bind:value options={[...]} />
	 *
	 * - `size="sm"`: a 13px label 5px above a 40px control at 14px, 0 12px
	 *   (a live bet's "What happened?"); below 640px the control is 44px at 16px
	 * - `hint`: a weight 400 muted line after the label text ("Optional")
	 * - `hideLabel`: the label is for screen readers only and the control runs
	 *   full width, border-box, as the board draws "Reasoning (optional)"
	 * - `wide`: spans every column of the form's grid
	 *
	 * Inputs and textareas are content-box, as the boards draw them, so their
	 * height is the min-height plus the border; a select is border-box, as
	 * Chromium draws it. Below 640px every control is 16px, so iOS Safari
	 * doesn't zoom when it takes focus. Every other attribute (name, required,
	 * maxlength, placeholder, type, min, max) goes through to the control.
	 */
	import type { HTMLInputAttributes } from 'svelte/elements';

	interface Option {
		value: string;
		label: string;
	}

	type Props = {
		label: string;
		kind?: 'input' | 'textarea' | 'select';
		size?: 'default' | 'sm';
		hint?: string;
		helper?: string;
		hideLabel?: boolean;
		wide?: boolean;
		options?: Option[];
		rows?: number;
		value?: string | number | null;
		class?: string;
	} & Omit<HTMLInputAttributes, 'value' | 'size' | 'class'>;

	let {
		label,
		kind = 'input',
		size = 'default',
		hint,
		helper,
		hideLabel = false,
		wide = false,
		options = [],
		rows,
		value = $bindable(),
		class: className = '',
		...rest
	}: Props = $props();
</script>

<label class="field size-{size} {className}" class:wide class:bare={hideLabel}>
	{#if hideLabel}
		<span class="sr-only">{label}</span>
	{:else}
		{label}
		{#if hint}<span class="hint">{hint}</span>{/if}
	{/if}
	{#if kind === 'textarea'}
		<textarea
			class="control textarea"
			{rows}
			bind:value
			{...rest as Record<string, unknown>}
		></textarea>
	{:else if kind === 'select'}
		<select class="control select" bind:value {...rest as Record<string, unknown>}>
			{#each options as option (option.value)}
				<option value={option.value}>{option.label}</option>
			{/each}
		</select>
	{:else}
		<input class="control input" bind:value {...rest} />
	{/if}
	{#if helper}<span class="helper">{helper}</span>{/if}
</label>

<style>
	.field {
		display: flex;
		flex-direction: column;
		gap: 6px;
		min-width: 0;
		font-size: 14px;
		font-weight: 600;
		color: var(--color-ink);
	}

	.field.wide {
		grid-column: 1 / -1;
	}

	.size-sm {
		gap: 5px;
		font-size: 13px;
	}

	.hint {
		font-weight: 400;
		color: var(--color-muted);
	}

	.helper {
		font-size: 13px;
		font-weight: 500;
		color: var(--color-muted);
	}

	.control {
		box-sizing: content-box;
		min-height: 46px;
		padding: 0 14px;
		border: 1px solid var(--color-line-strong);
		border-radius: var(--radius-control);
		background: var(--color-card);
		font-family: var(--font-sans);
		font-size: 16px;
		font-weight: 400;
		line-height: normal;
		color: var(--color-ink);
	}

	/* The forms plugin's focus ring; the control has its own outline. */
	.control:focus {
		box-shadow: none;
	}

	.control::placeholder {
		color: var(--color-faint);
		opacity: 1;
	}

	/* rows="3" measures lines at the font's normal line height, as the board's
	   textarea does, not the 1.5 the base styles give the page. */
	.textarea {
		min-height: 0;
		line-height: normal;
		padding: 12px 14px;
		resize: vertical;
	}

	/* The native control and its chevron, as the boards draw it (the base
	   styles turn appearance off for every select). */
	.select {
		box-sizing: border-box;
		appearance: auto;
		padding: 0 12px;
	}

	.size-sm .control {
		min-height: 40px;
		padding: 0 12px;
		font-size: 14px;
	}

	.bare .control {
		box-sizing: border-box;
		width: 100%;
	}

	.control:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	@media (max-width: 639.98px) {
		.size-sm .control {
			min-height: 44px;
			font-size: 16px;
		}
	}
</style>

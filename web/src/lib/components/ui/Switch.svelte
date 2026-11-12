<script lang="ts">
	/**
	 * An on/off switch (Settings board, Email notifications). A 48 by 28px pill
	 * track, field when on and line-strong when off, with a 22px white knob 3px
	 * from the top: at 23px from the left when on, 3px when off, on
	 * --shadow-thumb.
	 *
	 * The control is a real checkbox stretched over the track at 0 opacity, so
	 * it posts with its form, takes a click anywhere on the track and keeps the
	 * checkbox's keyboard and screen reader behavior. `label` is its accessible
	 * name; inside a <label> that already names it, pass the row's name anyway
	 * so the checkbox is named the same wherever it's used.
	 *
	 *   <Switch name="emailOnVoteResults" label="Vote results" bind:checked />
	 */
	import type { HTMLInputAttributes } from 'svelte/elements';

	type Props = {
		checked?: boolean;
		name?: string;
		label: string;
	} & Omit<HTMLInputAttributes, 'type' | 'checked' | 'name'>;

	let { checked = $bindable(false), name, label, class: className = '', ...rest }: Props = $props();
</script>

<span class="switch {className}" class:on={checked}>
	<input {...rest} type="checkbox" {name} aria-label={label} bind:checked />
	<span class="knob" aria-hidden="true"></span>
</span>

<style>
	.switch {
		position: relative;
		flex: none;
		display: inline-block;
		width: 48px;
		height: 28px;
		border-radius: 999px;
		background: var(--color-line-strong);
		transition: background-color 120ms ease;
	}

	.switch.on {
		background: var(--color-field);
	}

	input {
		position: absolute;
		inset: 0;
		width: 100%;
		height: 100%;
		margin: 0;
		opacity: 0;
		cursor: pointer;
	}

	input:disabled {
		cursor: default;
	}

	.switch:has(input:disabled) {
		opacity: 0.5;
	}

	.switch:has(input:focus-visible) {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.knob {
		position: absolute;
		top: 3px;
		left: 3px;
		width: 22px;
		height: 22px;
		border-radius: 50%;
		background: var(--color-white);
		box-shadow: var(--shadow-thumb);
		pointer-events: none;
		transition: left 120ms ease;
	}

	.on .knob {
		left: 23px;
	}

	@media (prefers-reduced-motion: reduce) {
		.switch,
		.knob {
			transition: none;
		}
	}
</style>

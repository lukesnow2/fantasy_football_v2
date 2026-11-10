<script lang="ts" generics="Value extends string | number">
	/**
	 * The season and week selector (Power Rankings board's week control; the
	 * System board's sample draws it 2px shorter): a wash well with 3px of
	 * padding, 10px corners and 2px gaps, holding 38px buttons, 44px below 640px
	 * so each is a full touch target. The chosen
	 * one is white with a 1px line border, ink, weight 700; the rest are ink-2 on
	 * transparent, weight 600.
	 *
	 * It's a radiogroup: Tab lands on the chosen option, the arrow keys move and
	 * choose, Home and End jump to the ends. `onchange` gets the new value, and
	 * `value` is bindable.
	 *
	 *   <Segmented label="Week" options={weeks} bind:value={week} />
	 *
	 * `size="toggle"` is the view switch This Season draws (Odds of, Order
	 * standings by): 34px buttons at 13px, every label in ink. Below 640px it
	 * grows to 44px at 14px, the phone boards' touch size.
	 */
	import { segmentedTarget } from './parts';

	interface Option {
		value: Value;
		label: string;
	}

	interface Props {
		options: Option[];
		value: Value;
		/** The group's accessible name, such as "Week". */
		label: string;
		onchange?: (value: Value) => void;
		size?: 'default' | 'toggle';
	}

	let { options, value = $bindable(), label, onchange, size = 'default' }: Props = $props();

	let buttons: HTMLButtonElement[] = $state([]);

	const selectedIndex = $derived(options.findIndex((option) => option.value === value));
	// With nothing chosen, the first option takes the tab stop.
	const tabStop = $derived(selectedIndex === -1 ? 0 : selectedIndex);

	function choose(index: number) {
		const next = options[index];
		if (!next) return;
		if (next.value !== value) {
			value = next.value;
			onchange?.(next.value);
		}
	}

	function onkeydown(event: KeyboardEvent, index: number) {
		const target = segmentedTarget(event.key, index, options.length);
		if (target === null) return;
		event.preventDefault();
		choose(target);
		buttons[target]?.focus();
	}
</script>

<div class="segmented size-{size}" role="radiogroup" aria-label={label}>
	{#each options as option, index (option.value)}
		{@const on = index === selectedIndex}
		<button
			bind:this={buttons[index]}
			type="button"
			role="radio"
			aria-checked={on}
			tabindex={index === tabStop ? 0 : -1}
			class:on
			onclick={() => choose(index)}
			onkeydown={(event) => onkeydown(event, index)}
		>
			{option.label}
		</button>
	{/each}
</div>

<style>
	.segmented {
		display: inline-flex;
		gap: 2px;
		padding: 3px;
		border-radius: 10px;
		background: var(--color-wash);
		width: max-content;
		max-width: 100%;
		overflow-x: auto;
	}

	button {
		flex: none;
		min-height: 38px;
		padding: 0 14px;
		border: 0;
		border-radius: var(--radius-control);
		background: transparent;
		color: var(--color-ink-2);
		font-family: var(--font-sans);
		font-size: 14px;
		font-weight: 600;
		cursor: pointer;
	}

	button.on {
		border: 1px solid var(--color-line);
		background: var(--color-card);
		color: var(--color-ink);
		font-weight: 700;
	}

	.size-toggle button {
		min-height: 34px;
		font-size: 13px;
		color: var(--color-ink);
	}

	@media (max-width: 639.98px) {
		button {
			min-height: 44px;
		}

		.size-toggle button {
			font-size: 14px;
		}
	}

	button:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 1px;
	}
</style>

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
	 *
	 * League History (WP10) adds:
	 * - an option's `sub`, a second part of its label in weight 500 muted
	 *   ("Early 05 to 12")
	 * - `size="compact"`: 36px buttons at 13px, ink-2 until chosen (the Seasons
	 *   and Measure controls); `minHeight` and `padX` set another height and
	 *   side padding in px (the View control: 34, 12)
	 * - `wrap`: the buttons wrap onto a second row when the well runs out of room
	 * - `phoneColumns`: below 640px the well fills the column as a grid of this
	 *   many equal columns (Seasons, 2 by 2; View, one row)
	 * - `bleed`: below 640px the well scrolls sideways inside a row that runs
	 *   this many px past each side, to a card's inner edges (Measure, 16)
	 */
	import { segmentedTarget } from './parts';

	interface Option {
		value: Value;
		label: string;
		/** A second part of the label in weight 500 muted. */
		sub?: string;
	}

	interface Props {
		options: Option[];
		value: Value;
		/** The group's accessible name, such as "Week". */
		label: string;
		onchange?: (value: Value) => void;
		size?: 'default' | 'toggle' | 'compact';
		/** Px: the buttons' min-height, when the board draws another. */
		minHeight?: number;
		/** Px: the buttons' side padding, when the board draws another. */
		padX?: number;
		wrap?: boolean;
		/** Below 640px, a grid of this many equal columns that fills the column. */
		phoneColumns?: number;
		/** Below 640px, scroll inside a row this many px wider on each side. */
		bleed?: number;
	}

	let {
		options,
		value = $bindable(),
		label,
		onchange,
		size = 'default',
		minHeight,
		padX,
		wrap = false,
		phoneColumns,
		bleed
	}: Props = $props();

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

{#snippet well()}
	<div
		class="segmented size-{size}"
		class:wrap
		class:phone-grid={phoneColumns != null}
		class:in-bleed={bleed != null}
		style:--seg-min-height={minHeight == null ? undefined : `${minHeight}px`}
		style:--seg-pad-x={padX == null ? undefined : `${padX}px`}
		style:--seg-phone-cols={phoneColumns}
		role="radiogroup"
		aria-label={label}
	>
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
				{option.label}{#if option.sub}{' '}<span class="sub">{option.sub}</span>{/if}
			</button>
		{/each}
	</div>
{/snippet}

{#if bleed != null}
	<div class="bleed" style:--seg-bleed="{bleed}px">{@render well()}</div>
{:else}
	{@render well()}
{/if}

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
		min-height: var(--seg-min-height, 38px);
		padding: 0 var(--seg-pad-x, 14px);
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
		min-height: var(--seg-min-height, 34px);
		font-size: 13px;
		color: var(--color-ink);
	}

	/* The boards' buttons take the browser's button font, so their digits
	   are proportional ("05 to 12"). */
	.size-compact button {
		min-height: var(--seg-min-height, 36px);
		font-size: 13px;
		font-variant-numeric: normal;
	}

	.sub {
		font-weight: 500;
		color: var(--color-muted);
	}

	.segmented.wrap {
		flex-wrap: wrap;
		width: auto;
	}

	@media (max-width: 639.98px) {
		.segmented button {
			min-height: 44px;
		}

		.size-toggle button {
			font-size: 14px;
		}

		.segmented.phone-grid {
			display: grid;
			grid-template-columns: repeat(var(--seg-phone-cols), minmax(0, 1fr));
			width: auto;
		}

		.phone-grid button {
			min-width: 0;
		}

		.bleed {
			margin: 0 calc(-1 * var(--seg-bleed));
			padding: 0 var(--seg-bleed);
			overflow-x: auto;
			scrollbar-width: none;
		}

		.bleed::-webkit-scrollbar {
			display: none;
		}

		.segmented.in-bleed {
			flex-wrap: nowrap;
			max-width: none;
			overflow: visible;
		}

		.in-bleed button {
			white-space: nowrap;
		}
	}

	button:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 1px;
	}
</style>

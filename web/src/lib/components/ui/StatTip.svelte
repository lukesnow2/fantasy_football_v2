<script lang="ts">
	/**
	 * The one stat tooltip (System board, "Stat tooltips"; Power Rankings draws
	 * it open beside Luck). The label is followed by a green ⓘ, and the two
	 * together are a real button with aria-describedby pointing at the card.
	 *
	 * The card is dark: clubhouse fill, 12px corners, 16px 18px of padding. It
	 * shows the metric's name and category pill, its one-line definition, the
	 * Calculation and How to read it blocks when the definition has them, and a
	 * link to the entry in the Data Dictionary. The copy comes from
	 * /api/meta-data through metricDefinitionsStore, the same data the Data
	 * Dictionary reads, so the two never disagree.
	 *
	 * It opens on hover and on tap, stays open when clicked, and closes with
	 * Escape or a click outside. It's placed in the viewport, so it stays on
	 * screen at 390 wide and isn't clipped by a scrolling table.
	 *
	 * A metric with no definition shows its label with no ⓘ and logs one console
	 * warning. It never shows an empty card.
	 *
	 *   <StatTip metricId="luck_factor" label="Luck" />
	 *
	 * A board that draws the open card somewhere other than under the label's
	 * left edge says where, and placeCard follows it inside the viewport:
	 * `endOffset` and `topOffset` for Power Rankings' Luck header (its right
	 * edge 40px past the label's, its top 26px below the label's top), `spanOf`
	 * for its phone table (the card stretched across that element).
	 *
	 * `chip` draws the label as a chip (the Data Dictionary's tip, WP11 S11.8):
	 * card fill, a 1px field-line border, 6px corners, 2px 8px of padding,
	 * weight 700. Below 640px its tap area grows to 44px tall without moving the
	 * line around it.
	 */
	import { onMount, tick, type Snippet } from 'svelte';
	import { placeCard } from './parts';
	import StatTipCard from './StatTipCard.svelte';
	import { cachedDefinition, loadDefinition, type StatTipDefinition } from './stat-tip-data';

	interface Props {
		metricId: string;
		/** The label. Pass children instead for a label with markup. */
		label?: string;
		children?: Snippet;
		/** Show the ⓘ. Only the old MetricHelp callers turn it off. */
		icon?: boolean;
		/** Hide the label from sight (screen readers still get it). */
		iconOnly?: boolean;
		/** Open the card. Bindable; a board that draws it open starts it open. */
		open?: boolean;
		/** Px the card's right edge sits past the label's right edge. */
		endOffset?: number;
		/** Px the card's top sits below the label's top, instead of 8px under it. */
		topOffset?: number;
		/** Stretch the card across this element's width. */
		spanOf?: HTMLElement;
		/**
		 * Below 640px, make the label a 44px touch target, as the phone boards
		 * draw a table header's StatTip. Off for a label inside a sentence.
		 */
		phoneTarget?: boolean;
		/**
		 * Below 640px, pad the label's tap area to 44px without moving the
		 * layout, as the phone boards draw an inline StatTip ("Expected ⓘ"):
		 * `inline` is 12px of padding and a 20px line for 14px and 15px text,
		 * `inline-small` 14px and a 16px line for 12px text.
		 */
		phoneTap?: 'inline' | 'inline-small';
		/** Draw the label as a chip. */
		chip?: boolean;
	}

	let {
		metricId,
		label,
		children,
		icon = true,
		iconOnly = false,
		open = $bindable(false),
		endOffset,
		topOffset,
		spanOf,
		phoneTarget = false,
		phoneTap,
		chip = false
	}: Props = $props();

	const id = $props.id();
	const cardId = `stattip-${id}`;

	// A definition already in the store (the Data Dictionary seeds every one)
	// renders with the first paint; anything else loads once mounted, and again
	// whenever metricId changes.
	const cached = cachedDefinition(metricId);
	let definition: StatTipDefinition | null = $state(cached);
	let status: 'loading' | 'ready' | 'missing' = $state(cached ? 'ready' : 'loading');

	/** Opened by the pointer resting on it, rather than by a click. */
	let hovering = $state(false);
	let root: HTMLSpanElement | undefined = $state();
	let trigger: HTMLButtonElement | undefined = $state();
	let card: HTMLSpanElement | undefined = $state();
	let position: { top: number; left: number; width?: number } | null = $state(null);
	let closeTimer: ReturnType<typeof setTimeout> | undefined;

	// 'ready' always comes with a definition.
	const showing = $derived((open || hovering) && status === 'ready');

	$effect(() => {
		const id = metricId;
		const known = cachedDefinition(id);
		if (known) {
			definition = known;
			status = 'ready';
			return;
		}
		definition = null;
		status = 'loading';
		let cancelled = false;
		loadDefinition(id).then((found) => {
			if (cancelled) return;
			definition = found;
			status = found ? 'ready' : 'missing';
		});
		return () => {
			cancelled = true;
		};
	});

	onMount(() => () => clearTimeout(closeTimer));

	function measure() {
		if (!trigger || !card) return null;
		const spanBox = spanOf?.getBoundingClientRect();
		return placeCard(
			trigger.getBoundingClientRect(),
			{ width: card.offsetWidth, height: card.offsetHeight },
			{ width: window.innerWidth, height: window.innerHeight },
			{
				endOffset,
				topOffset,
				span: spanBox ? { left: spanBox.left, width: spanBox.width } : undefined
			}
		);
	}

	async function place() {
		const placed = measure();
		if (!placed) return;
		position = placed;
		// A spanned card rewraps at its new width, so its height changes: measure
		// again at that width before deciding above or below.
		if (placed.width != null && card && card.offsetWidth !== placed.width) {
			await tick();
			position = measure() ?? placed;
		}
	}

	// Measure once the card is in the page, and follow the trigger while open.
	$effect(() => {
		if (!showing) {
			position = null;
			return;
		}
		tick().then(place);
		// One measurement per frame, however many scroll events arrive.
		let frame = 0;
		const follow = () => {
			if (frame) return;
			frame = requestAnimationFrame(() => {
				frame = 0;
				place();
			});
		};
		window.addEventListener('scroll', follow, true);
		window.addEventListener('resize', follow);
		return () => {
			cancelAnimationFrame(frame);
			window.removeEventListener('scroll', follow, true);
			window.removeEventListener('resize', follow);
		};
	});

	// Escape and a click outside close it.
	$effect(() => {
		if (!showing) return;
		const onKey = (event: KeyboardEvent) => {
			if (event.key !== 'Escape') return;
			const hadFocus = root?.contains(document.activeElement);
			close();
			if (hadFocus) trigger?.focus();
		};
		const onPointer = (event: PointerEvent) => {
			if (root && !root.contains(event.target as Node)) close();
		};
		document.addEventListener('keydown', onKey);
		document.addEventListener('pointerdown', onPointer);
		return () => {
			document.removeEventListener('keydown', onKey);
			document.removeEventListener('pointerdown', onPointer);
		};
	});

	function close() {
		clearTimeout(closeTimer);
		open = false;
		hovering = false;
	}

	// A click, tap or Enter toggles the pinned state. A card the pointer opened
	// isn't pinned yet, so the first click pins it rather than closing it.
	function onclick() {
		if (open) close();
		else open = true;
	}

	function onpointerenter(event: PointerEvent) {
		if (event.pointerType !== 'mouse') return;
		clearTimeout(closeTimer);
		hovering = true;
	}

	function onpointerleave(event: PointerEvent) {
		if (event.pointerType !== 'mouse') return;
		// A short grace period lets the pointer cross the gap into the card.
		closeTimer = setTimeout(() => (hovering = false), 150);
	}

	function onfocusout(event: FocusEvent) {
		const next = event.relatedTarget as Node | null;
		if (next && root?.contains(next)) return;
		if (next) close();
	}
</script>

{#snippet labelText()}
	{#if children}{@render children()}{:else}{label}{/if}
{/snippet}

{#if status === 'missing'}
	<span class:sr-only={iconOnly}>{@render labelText()}</span>
{:else}
	<span
		class="stattip"
		bind:this={root}
		{onpointerenter}
		{onpointerleave}
		{onfocusout}
		role="presentation"
	>
		<button
			bind:this={trigger}
			type="button"
			class="trigger"
			class:phone-target={phoneTarget}
			class:tap-inline={phoneTap === 'inline'}
			class:tap-inline-small={phoneTap === 'inline-small'}
			class:chip
			aria-describedby={definition ? cardId : undefined}
			aria-expanded={showing}
			{onclick}
		>
			<span class:sr-only={iconOnly}>{@render labelText()}</span>{#if icon}{' '}<span
					class="i"
					aria-hidden="true">ⓘ</span
				>{/if}
		</button>
		{#if definition}
			<StatTipCard
				bind:element={card}
				{definition}
				id={cardId}
				floating
				hidden={!showing}
				top={position?.top ?? null}
				left={position?.left ?? null}
				width={position?.width ?? null}
			/>
		{/if}
	</span>
{/if}

<style>
	.stattip {
		display: inline;
	}

	.trigger {
		padding: 0;
		border: 0;
		background: transparent;
		cursor: help;
		font: inherit;
		color: inherit;
		letter-spacing: inherit;
		text-transform: inherit;
		text-align: inherit;
	}

	@media (max-width: 639.98px) {
		.trigger.phone-target {
			min-height: 44px;
		}

		.trigger.tap-inline,
		.trigger.tap-inline-small {
			display: inline-block;
			white-space: nowrap;
		}

		.trigger.tap-inline {
			padding: 12px 0;
			margin: -12px 0;
			line-height: 20px;
		}

		.trigger.tap-inline-small {
			padding: 14px 0;
			margin: -14px 0;
			line-height: 16px;
		}
	}

	.trigger.chip {
		position: relative;
		padding: 2px 8px;
		border: 1px solid var(--color-field-line);
		border-radius: var(--radius-tag);
		background: var(--color-card);
		font-weight: 700;
		line-height: normal;
		color: var(--color-ink);
	}

	/* The desktop board draws the chip as an inline span, which leaves the
	   line's height alone; a button's padding and border would add 3px. */
	@media (min-width: 640px) {
		.trigger.chip {
			margin: -3px 0;
		}
	}

	@media (max-width: 639.98px) {
		.trigger.chip::before {
			content: '';
			position: absolute;
			inset: -9px 0;
		}
	}

	.trigger:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
		border-radius: 2px;
	}

	.i {
		color: var(--color-field);
	}
</style>

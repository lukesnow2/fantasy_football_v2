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
	}

	let {
		metricId,
		label,
		children,
		icon = true,
		iconOnly = false,
		open = $bindable(false)
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
	let position: { top: number; left: number } | null = $state(null);
	let closeTimer: ReturnType<typeof setTimeout> | undefined;

	const showing = $derived((open || hovering) && status === 'ready' && definition !== null);

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

	async function place() {
		if (!trigger || !card) return;
		const box = trigger.getBoundingClientRect();
		position = placeCard(
			box,
			{ width: card.offsetWidth, height: card.offsetHeight },
			{ width: window.innerWidth, height: window.innerHeight }
		);
	}

	// Measure once the card is in the page, and follow the trigger while open.
	$effect(() => {
		if (!showing) {
			position = null;
			return;
		}
		tick().then(place);
		const follow = () => place();
		window.addEventListener('scroll', follow, true);
		window.addEventListener('resize', follow);
		return () => {
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

	function onclick() {
		// A click on a card the pointer already opened keeps it open; otherwise
		// a click (or a tap, or Enter) toggles it.
		if (hovering && !open) {
			open = true;
		} else if (open) {
			close();
		} else {
			open = true;
		}
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

	.trigger:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
		border-radius: 2px;
	}

	.i {
		color: var(--color-field);
	}
</style>

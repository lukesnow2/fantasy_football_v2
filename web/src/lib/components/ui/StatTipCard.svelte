<script lang="ts">
	/**
	 * The StatTip card on its own (System board, "Stat tooltips"): clubhouse
	 * fill, 12px corners, 16px 18px of padding, the tip shadow. The name in slab
	 * 700 18px beside the category pill, the definition, then Calculation and How
	 * to read it when the definition has them, and the link to the Data
	 * Dictionary entry.
	 *
	 * StatTip floats it next to a label as its disclosure panel, and passes
	 * `onclose` for the × in the top right corner. A page that draws the card
	 * standing open, as the System board does, can render it directly (no ×)
	 * and it fills its column.
	 *
	 * It's a group named by the metric, never a tooltip: it holds a link, so it
	 * has to be reachable with Tab and read in full, which a tooltip isn't. The
	 * category pill is plain text.
	 */
	import { dictionaryHref, type StatTipDefinition } from './stat-tip-data';

	interface Props {
		definition: StatTipDefinition;
		id?: string;
		/** Fixed to the viewport at `top` and `left`, as StatTip places it. */
		floating?: boolean;
		top?: number | null;
		left?: number | null;
		/** A floating card's width in px, when it spans an element. */
		width?: number | null;
		hidden?: boolean;
		element?: HTMLSpanElement;
		/** Draws the × close control, and is called when it's pressed. */
		onclose?: () => void;
	}

	let {
		definition,
		id,
		floating = false,
		top = null,
		left = null,
		width = null,
		hidden = false,
		element = $bindable(),
		onclose
	}: Props = $props();

	const uid = $props.id();
	const nameId = `stattip-name-${uid}`;
</script>

<span
	bind:this={element}
	{id}
	role="group"
	aria-labelledby={nameId}
	class="card"
	class:floating
	{hidden}
	style:top={floating && top !== null ? `${top}px` : undefined}
	style:left={floating && left !== null ? `${left}px` : undefined}
	style:width={floating && width !== null ? `${width}px` : undefined}
	style:visibility={floating && top === null ? 'hidden' : undefined}
>
	<span class="name-row">
		<span class="name" id={nameId}>{definition.metricName}</span>
		{#if definition.categoryName}
			<span class="pill">{definition.categoryName}</span>
		{/if}
		{#if onclose}
			<button type="button" class="close" aria-label="Close" onclick={onclose}
				><span aria-hidden="true">×</span></button
			>
		{/if}
	</span>
	<span class="text first">{definition.shortDescription}</span>
	{#if definition.calculationFormula}
		<span class="heading">Calculation</span>
		<code>{definition.calculationFormula}</code>
	{/if}
	{#if definition.interpretationGuide}
		<span class="heading">How to read it</span>
		<span class="text">{definition.interpretationGuide}</span>
	{/if}
	<a class="more" href={dictionaryHref(definition.metricId)}
		>Full definition, worked example and limits →</a
	>
</span>

<style>
	.card {
		display: block;
		box-sizing: border-box;
		padding: 16px 18px;
		border-radius: var(--radius-card);
		background: var(--color-clubhouse);
		color: var(--color-white);
		box-shadow: var(--shadow-tip);
		font-family: var(--font-sans);
		font-size: 13px;
		font-weight: 400;
		line-height: normal;
		letter-spacing: normal;
		text-align: left;
		text-transform: none;
		white-space: normal;
	}

	/* Power Rankings' open card is 340px; at 390 wide it keeps 16px each side. */
	.floating {
		position: fixed;
		z-index: 60;
		width: min(340px, calc(100vw - 32px));
	}

	.card[hidden] {
		display: none;
	}

	.name-row {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.name {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 18px;
	}

	.close {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		flex: none;
		width: 28px;
		height: 28px;
		margin: -6px -8px -6px auto;
		padding: 0;
		border: 0;
		border-radius: 6px;
		background: transparent;
		color: var(--color-on-clubhouse);
		font: inherit;
		font-size: 20px;
		line-height: 1;
		cursor: pointer;
	}

	.close:hover {
		background: var(--color-clubhouse-2);
		color: var(--color-white);
	}

	.close:focus-visible {
		outline: 2px solid var(--color-pennant-light);
		outline-offset: 0;
	}

	/* A full touch target on phones, without growing the name row. */
	@media (max-width: 639.98px) {
		.close {
			width: 44px;
			height: 44px;
			margin: -14px -14px -14px auto;
		}
	}

	/* The pill holds its line; a long name wraps beside it instead. */
	.pill {
		flex: none;
		white-space: nowrap;
		padding: 2px 7px;
		border-radius: 999px;
		background: var(--color-clubhouse-2);
		color: var(--color-on-clubhouse-2);
		font-size: 11px;
		font-weight: 700;
	}

	.text {
		display: block;
		margin-top: 3px;
		font-size: 13px;
		line-height: 1.5;
		color: var(--color-on-clubhouse);
	}

	.text.first {
		margin-top: 6px;
	}

	.heading {
		display: block;
		margin-top: 10px;
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-pennant-light);
	}

	code {
		display: block;
		margin-top: 3px;
		padding: 6px 8px;
		border-radius: 6px;
		background: var(--color-clubhouse-2);
		font-size: 12px;
	}

	.more {
		display: inline-block;
		margin-top: 10px;
		font-size: 13px;
		font-weight: 700;
		color: var(--color-pennant-light);
	}

	/* A full touch target on phones (Power Rankings phone board). */
	@media (max-width: 639.98px) {
		.more {
			display: inline-flex;
			align-items: center;
			min-height: 44px;
			margin-top: 2px;
		}
	}

	/* The page's link hover (field) would vanish on the clubhouse fill. */
	.more:hover {
		color: var(--color-pennant-light);
		text-decoration: underline;
	}
</style>

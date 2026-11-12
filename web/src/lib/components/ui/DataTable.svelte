<script lang="ts" module>
	export interface Column {
		key: string;
		label: string;
		/** Right-aligns the header and cells, for numbers. */
		numeric?: boolean;
		/** A CSS width for the column, such as "56px". The rest share what's left. */
		width?: string;
		/** An accessible name for the header when its text is a symbol ("Wk Δ"). */
		ariaLabel?: string;
		/**
		 * Hold the column to `width` even when its header text is wider, letting
		 * the text run over, as a board's grid column does ("Playoffs" in the
		 * Hall of Fame's 64px column).
		 */
		fixedHeader?: boolean;
		/**
		 * Let the header text wrap inside `width`, as a board's grid cell does
		 * ("Grade ⓘ" in Draft Central's 60px column puts the ⓘ on a second line).
		 */
		wrapHeader?: boolean;
	}
</script>

<script lang="ts" generics="Row extends Record<string, unknown>">
	/**
	 * The one table (System board, "Table row"; This Season, Standings). Header
	 * cells are 12px, 700, uppercase, 0.06em, ink-2, with a 2px ink rule under the
	 * header row. Body rows are 15px with a 1px line-soft rule, and numeric
	 * columns align right.
	 *
	 * A row can be highlighted (pennant-wash, a title season or a won final) or
	 * selected (field-tint). `divider` puts a labeled rule before a row, such as
	 * the playoff line after 6th on the standings. `dividerCell` draws that row's
	 * content instead of the rule and label, for a divider with more in it (the
	 * Hall of Fame's tier rows: a name, a count and the tier's numbers on the
	 * right); the snippet brings its own padding. `maxHeight` scrolls the rows
	 * under a header that stays put (League History's chart table).
	 *
	 *   <DataTable {columns} {rows} divider={(_, i) => (i === 6 ? 'Playoff line' : null)}>
	 *     {#snippet cell(row, column)}...{/snippet}
	 *   </DataTable>
	 */
	import type { Snippet } from 'svelte';
	import EmptyValue from './EmptyValue.svelte';

	interface Props {
		columns: Column[];
		rows: Row[];
		/** An accessible name for the table. */
		label?: string;
		/** Vertical padding of a body row in px: 9 on System, 12 on This Season. */
		rowPadding?: number;
		/** Space between columns in px: 10 on System, 14 on This Season. */
		columnGap?: number;
		/**
		 * Keep the 2px header rule but hide the header text from sight, as the
		 * System board's lone table row draws it. Screen readers still get it.
		 */
		hideHeader?: boolean;
		/** Below this width the table scrolls sideways instead of squeezing. */
		minWidth?: string;
		/** Body text size in px: 15 on the desktop boards, 14 on the phone boards. */
		fontSize?: number;
		/**
		 * A header row this many px tall with no padding, its text centered, as
		 * the phone boards draw it so a StatTip label is a 44px target.
		 */
		headerHeight?: number;
		rowKey?: (row: Row, index: number) => string | number;
		highlighted?: (row: Row, index: number) => boolean;
		selected?: (row: Row, index: number) => boolean;
		/** A label for a rule drawn before this row, or null for none. */
		divider?: (row: Row, index: number) => string | null;
		/** Draws a divider row's content in place of the labeled rule. */
		dividerCell?: Snippet<[string, Row & Record<string, unknown>, number]>;
		/**
		 * Draws a body cell. Without it a cell shows row[column.key], or "None"
		 * when that's null or undefined (G6).
		 */
		cell?: Snippet<[Row & Record<string, unknown>, Column, number]>;
		/** Draws a header cell, such as a label with a StatTip. */
		header?: Snippet<[Column]>;
		/**
		 * Keep the first column in place while the rest scroll sideways, with
		 * a 1px line-soft rule on its right edge (the profile's phone Head to
		 * head table).
		 */
		stickyFirst?: boolean;
		/** Draw the 1px rule on the sticky column's edge (Draft Central's phone table draws none). */
		stickyRule?: boolean;
		/**
		 * Px of padding on the first column's left and the last column's right,
		 * for a table that runs flush to its card's sides but keeps its text
		 * inset (Draft Central's phone table: 16).
		 */
		edgeInset?: number;
		/** Where header text sits when the header row is two lines tall. */
		headerAlign?: 'top' | 'middle' | 'bottom';
		/**
		 * Scroll the rows inside this height with the header held at the top
		 * (League History's chart table: 320px).
		 */
		maxHeight?: string;
	}

	let {
		columns,
		rows,
		label,
		rowPadding = 9,
		columnGap = 10,
		minWidth,
		fontSize = 15,
		headerHeight,
		hideHeader = false,
		rowKey = (_row, index) => index,
		highlighted,
		selected,
		divider,
		dividerCell,
		cell,
		header,
		stickyFirst = false,
		stickyRule = true,
		edgeInset,
		headerAlign,
		maxHeight
	}: Props = $props();
</script>

<div class="scroll" class:capped={maxHeight != null} style:max-height={maxHeight}>
	<table
		class:sticky-first={stickyFirst}
		class:no-sticky-rule={!stickyRule}
		class:inset={edgeInset != null}
		style:--edge-inset={edgeInset != null ? `${edgeInset}px` : undefined}
		aria-label={label}
		style:min-width={minWidth}
		style:font-size="{fontSize}px"
		style:--row-pad="{rowPadding}px"
		style:--col-gap="{columnGap}px"
	>
		<thead>
			<tr>
				{#each columns as column (column.key)}
					<th
						scope="col"
						class:num={column.numeric}
						class:bare={hideHeader}
						class:fixed-height={headerHeight != null}
						class:wrap={column.wrapHeader}
						style:vertical-align={headerAlign}
						style:width={column.width}
						style:height={headerHeight != null ? `${headerHeight}px` : undefined}
						aria-label={column.ariaLabel}
					>
						<span
							class:sr-only={hideHeader}
							class:fixed-head={column.fixedHeader && column.width}
							style:width={column.fixedHeader ? column.width : undefined}
							>{#if header}{@render header(column)}{:else}{column.label}{/if}</span
						>
					</th>
				{/each}
			</tr>
		</thead>
		<tbody>
			{#each rows as row, index (rowKey(row, index))}
				{@const rule = divider?.(row, index)}
				{#if rule && dividerCell}
					<tr class="divider custom">
						<td colspan={columns.length}>{@render dividerCell(rule, row, index)}</td>
					</tr>
				{:else if rule}
					<tr class="divider">
						<td colspan={columns.length}>
							<span class="divider-line">
								<span class="rule" aria-hidden="true"></span>{rule}<span
									class="rule"
									aria-hidden="true"
								></span>
							</span>
						</td>
					</tr>
				{/if}
				<tr class:highlighted={highlighted?.(row, index)} class:selected={selected?.(row, index)}>
					{#each columns as column (column.key)}
						<td class:num={column.numeric}>
							{#if cell}{@render cell(
									row,
									column,
									index
								)}{:else if row[column.key] == null}<EmptyValue />{:else}{row[column.key]}{/if}
						</td>
					{/each}
				</tr>
			{/each}
		</tbody>
	</table>
</div>

<style>
	/* Relative, so screen-reader text (absolutely placed) scrolls with the
	   table instead of widening the page. */
	.scroll {
		position: relative;
		overflow-x: auto;
	}

	table {
		width: 100%;
		border-collapse: collapse;
	}

	/* The header stays put while the rows scroll under it; its 2px rule is a
	   shadow, since a collapsed border doesn't stick with its cell. */
	.scroll.capped {
		overflow-y: auto;
	}

	.capped th {
		position: sticky;
		top: 0;
		z-index: 2;
		background: var(--color-card);
		border-bottom: 0;
		box-shadow: inset 0 -2px 0 var(--color-ink);
	}

	th {
		padding: 8px 0;
		border-bottom: 2px solid var(--color-ink);
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-ink-2);
		text-align: left;
		white-space: nowrap;
	}

	th.wrap {
		white-space: normal;
	}

	th.bare,
	th.fixed-height {
		padding-top: 0;
		padding-bottom: 0;
	}

	th.fixed-height {
		vertical-align: middle;
	}

	td {
		padding: var(--row-pad) 0;
		border-bottom: 1px solid var(--color-line-soft);
		vertical-align: middle;
	}

	/* A column's width is its content, and the gap sits outside it, as in the
	   boards' grids. */
	th,
	td {
		box-sizing: content-box;
	}

	th + th,
	td + td {
		padding-left: var(--col-gap);
	}

	.num {
		text-align: right;
	}

	.sticky-first th:first-child,
	.sticky-first td:first-child {
		position: sticky;
		left: 0;
		z-index: 1;
		background: var(--color-card);
		box-shadow: inset -1px 0 0 var(--color-line-soft);
	}

	.no-sticky-rule th:first-child,
	.no-sticky-rule td:first-child {
		box-shadow: none;
	}

	.inset th:first-child,
	.inset td:first-child {
		padding-left: var(--edge-inset);
	}

	.inset th:last-child,
	.inset td:last-child {
		padding-right: var(--edge-inset);
	}

	tr.highlighted td {
		background: var(--color-pennant-wash);
	}

	tr.selected td {
		background: var(--color-field-tint);
	}

	/* The sticky cell keeps the row's fill. */
	.sticky-first tr.highlighted td:first-child {
		background: var(--color-pennant-wash);
	}

	.sticky-first tr.selected td:first-child {
		background: var(--color-field-tint);
	}

	tr.divider td {
		padding: 8px 0;
		border-bottom: 0;
	}

	.fixed-head {
		display: block;
	}

	tr.divider.custom td {
		padding: 0;
	}

	.divider-line {
		display: flex;
		align-items: center;
		gap: 12px;
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-field);
	}

	.rule {
		flex: 1;
		border-top: 2px solid var(--color-field);
	}
</style>

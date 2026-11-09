<script lang="ts" module>
	export interface Column {
		key: string;
		label: string;
		/** Right-aligns the header and cells, for numbers. */
		numeric?: boolean;
		/** A CSS width for the column, such as "56px". The rest share what's left. */
		width?: string;
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
	 * the playoff line after 6th on the standings.
	 *
	 *   <DataTable {columns} {rows} divider={(_, i) => (i === 6 ? 'Playoff line' : null)}>
	 *     {#snippet cell(row, column)}...{/snippet}
	 *   </DataTable>
	 */
	import type { Snippet } from 'svelte';

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
		rowKey?: (row: Row, index: number) => string | number;
		highlighted?: (row: Row, index: number) => boolean;
		selected?: (row: Row, index: number) => boolean;
		/** A label for a rule drawn before this row, or null for none. */
		divider?: (row: Row, index: number) => string | null;
		/** Draws a body cell. Without it a cell shows row[column.key]. */
		cell?: Snippet<[Row & Record<string, unknown>, Column, number]>;
		/** Draws a header cell, such as a label with a StatTip. */
		header?: Snippet<[Column]>;
	}

	let {
		columns,
		rows,
		label,
		rowPadding = 9,
		columnGap = 10,
		minWidth,
		hideHeader = false,
		rowKey = (_row, index) => index,
		highlighted,
		selected,
		divider,
		cell,
		header
	}: Props = $props();
</script>

<div class="scroll">
	<table
		aria-label={label}
		style:min-width={minWidth}
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
						style:width={column.width}
					>
						<span class:sr-only={hideHeader}
							>{#if header}{@render header(column)}{:else}{column.label}{/if}</span
						>
					</th>
				{/each}
			</tr>
		</thead>
		<tbody>
			{#each rows as row, index (rowKey(row, index))}
				{@const rule = divider?.(row, index)}
				{#if rule}
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
							{#if cell}{@render cell(row, column, index)}{:else}{row[column.key] ?? ''}{/if}
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
		font-size: 15px;
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

	th.bare {
		padding-top: 0;
		padding-bottom: 0;
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

	tr.highlighted td {
		background: var(--color-pennant-wash);
	}

	tr.selected td {
		background: var(--color-field-tint);
	}

	tr.divider td {
		padding: 8px 0;
		border-bottom: 0;
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

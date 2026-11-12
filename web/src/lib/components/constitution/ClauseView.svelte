<script lang="ts">
	/**
	 * One clause and its sub-clauses, as a list item (WP11 S11.2). Top-level
	 * clauses sit on a 44px label column with a line-soft rule under each;
	 * depth 1 ("a.") on 28px, depth 2 ("i.") on 32px behind a field-tint rule.
	 * Children render recursively.
	 *
	 * In propose mode (S11.4) the clause under the pointer, or holding focus,
	 * gets a ground fill and an inset ring, and three buttons: propose an edit,
	 * a sub-clause, or its removal. They show on hover and on focus, never on
	 * hover alone: until then they're out of sight but still in the tab order,
	 * so tabbing onto one brings them up. On a touch screen every clause shows
	 * its buttons under its text.
	 */
	import { Button } from '$lib/components/ui';
	import { clauseLabel } from '$lib/constitution/display';
	import Self from './ClauseView.svelte';

	export let clause: {
		clauseKey: number;
		clauseUid: string;
		depth: number;
		label: string;
		body: string;
		children: any[];
	};
	export let sectionId: string;
	export let editMode = false;
	export let onPropose: (input: {
		type: 'edit_clause' | 'add_clause' | 'delete_clause';
		sectionId: string;
		clauseUid: string | null;
		currentText: string;
	}) => void;

	$: depth = Math.min(clause.depth, 2);
	$: hasKids = clause.children.length > 0;
</script>

<li class="clause depth-{depth}" class:editable={editMode} class:kids={hasKids}>
	<span class="label">{clauseLabel(clause.depth, clause.label)}</span>
	<div class="text">
		<div class="row">
			<div class="body">{clause.body}</div>
			{#if editMode}
				<span class="actions">
					<Button
						variant="icon"
						aria-label="Propose an edit to this clause"
						onclick={() =>
							onPropose({
								type: 'edit_clause',
								sectionId,
								clauseUid: clause.clauseUid,
								currentText: clause.body
							})}
					>
						<svg
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
							stroke-linecap="round"
							stroke-linejoin="round"
							aria-hidden="true"><path d="M4 20h4L19 9l-4-4L4 16z"></path></svg
						>
					</Button>
					<Button
						variant="icon"
						aria-label="Propose a sub-clause"
						onclick={() =>
							onPropose({
								type: 'add_clause',
								sectionId,
								clauseUid: clause.clauseUid,
								currentText: ''
							})}
					>
						<svg
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
							stroke-linecap="round"
							aria-hidden="true"><path d="M12 5v14M5 12h14"></path></svg
						>
					</Button>
					<Button
						variant="icon"
						tone="pennant"
						aria-label="Propose removing this clause"
						onclick={() =>
							onPropose({
								type: 'delete_clause',
								sectionId,
								clauseUid: clause.clauseUid,
								currentText: clause.body
							})}
					>
						<svg
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
							stroke-linecap="round"
							stroke-linejoin="round"
							aria-hidden="true"><path d="M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13"></path></svg
						>
					</Button>
				</span>
			{/if}
		</div>
		{#if hasKids}
			<ol class="children level-{Math.min(depth + 1, 2)}">
				{#each clause.children as child (child.clauseKey)}
					<Self clause={child} {sectionId} {editMode} {onPropose} />
				{/each}
			</ol>
		{/if}
	</div>
</li>

<style>
	.clause {
		display: grid;
	}

	.depth-0 {
		grid-template-columns: 44px minmax(0, 1fr);
		gap: 0 8px;
		padding: 10px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.depth-0 > .label {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 17px;
		color: var(--color-field);
	}

	.depth-0 > .text > .row > .body {
		font-size: 16px;
		line-height: 1.55;
	}

	.depth-1 {
		grid-template-columns: 28px minmax(0, 1fr);
		gap: 0 6px;
		padding: 4px 0;
	}

	.depth-1 > .label {
		font-weight: 700;
		color: var(--color-muted);
	}

	.depth-1 > .text > .row > .body {
		font-size: 15px;
		line-height: 1.55;
		color: var(--color-ink);
	}

	.depth-2 {
		grid-template-columns: 32px minmax(0, 1fr);
		gap: 0 4px;
		padding: 3px 0 3px 8px;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	.depth-2 > .label {
		font-weight: 600;
	}

	.children {
		margin: 0;
		padding: 0;
		list-style: none;
	}

	ol.level-1 {
		margin-top: 6px;
	}

	ol.level-2 {
		margin-top: 4px;
		padding-left: 4px;
		border-left: 2px solid var(--color-field-tint);
	}

	/* Propose mode. The active clause is the innermost one holding focus or,
	   with a mouse, under the pointer, so a sub-clause lights up without its
	   parent. A touch screen leaves :hover behind after a tap, so there only
	   focus lights a clause. */
	.editable {
		position: relative;
	}

	.row {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.body {
		flex: 1;
		min-width: 0;
	}

	.kids > .text > .row {
		align-items: flex-start;
	}

	/* Out of sight but focusable until the clause is active. */
	.actions {
		position: absolute;
		width: 1px;
		height: 1px;
		overflow: hidden;
		clip-path: inset(50%);
		white-space: nowrap;
	}

	.depth-0.editable:focus-within:not(:has(.clause:focus-within)),
	.depth-1.editable:focus-within:not(:has(.clause:focus-within)),
	.depth-2.editable:focus-within:not(:has(.clause:focus-within)) {
		margin: 0 -12px;
		border-radius: var(--radius-control);
		background: var(--color-ground);
		box-shadow: inset 0 0 0 1px var(--color-line-strong);
	}

	.depth-0.editable:focus-within:not(:has(.clause:focus-within)) {
		padding: 10px 12px;
	}

	.depth-1.editable:focus-within:not(:has(.clause:focus-within)) {
		padding: 4px 12px;
	}

	.depth-2.editable:focus-within:not(:has(.clause:focus-within)) {
		padding: 3px 12px 3px 20px;
	}

	.editable:focus-within:not(:has(.clause:focus-within)) > .text > .row > .actions {
		position: static;
		width: auto;
		height: auto;
		overflow: visible;
		clip-path: none;
		display: flex;
		flex: none;
		gap: 4px;
	}

	/* The label sits centered on the 34px row of buttons, as the board draws it. */
	.depth-0.editable:focus-within:not(:has(.clause:focus-within)) > .label {
		padding-top: calc((34px - 1lh) / 2);
	}

	@media (hover: hover) {
	.depth-0.editable:hover:not(:has(.clause:hover)),
	.depth-1.editable:hover:not(:has(.clause:hover)),
	.depth-2.editable:hover:not(:has(.clause:hover)) {
		margin: 0 -12px;
		border-radius: var(--radius-control);
		background: var(--color-ground);
		box-shadow: inset 0 0 0 1px var(--color-line-strong);
	}

	.depth-0.editable:hover:not(:has(.clause:hover)) {
		padding: 10px 12px;
	}

	.depth-1.editable:hover:not(:has(.clause:hover)) {
		padding: 4px 12px;
	}

	.depth-2.editable:hover:not(:has(.clause:hover)) {
		padding: 3px 12px 3px 20px;
	}

	.editable:hover:not(:has(.clause:hover)) > .text > .row > .actions {
		position: static;
		width: auto;
		height: auto;
		overflow: visible;
		clip-path: none;
		display: flex;
		flex: none;
		gap: 4px;
	}

	/* The label sits centered on the 34px row of buttons, as the board draws it. */
	.depth-0.editable:hover:not(:has(.clause:hover)) > .label {
		padding-top: calc((34px - 1lh) / 2);
	}
	}

	@media (hover: none) {
		.editable > .text > .row {
			flex-direction: column;
			align-items: flex-start;
			gap: 8px;
		}

		.editable > .text > .row > .body {
			align-self: stretch;
		}

		.editable > .text > .row > .actions {
			position: static;
			width: auto;
			height: auto;
			overflow: visible;
			clip-path: none;
			display: flex;
			gap: 8px;
		}

		.depth-0.editable:focus-within:not(:has(.clause:focus-within)) > .label {
			padding-top: 0;
		}
	}

	@media (max-width: 639.98px) {
		.depth-0 {
			grid-template-columns: 36px minmax(0, 1fr);
		}

		.depth-0 > .label {
			line-height: 1.4;
		}

		.depth-0 > .text > .row > .body {
			font-size: 15px;
		}

		.depth-1 {
			grid-template-columns: 24px minmax(0, 1fr);
		}

		.depth-2 {
			grid-template-columns: 28px minmax(0, 1fr);
			padding: 3px 0 3px 6px;
		}
	}
</style>

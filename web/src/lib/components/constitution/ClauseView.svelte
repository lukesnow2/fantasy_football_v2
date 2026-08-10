<script lang="ts">
	import { Edit3, Plus, Trash2 } from 'lucide-svelte';
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

	// Indent by depth rather than baking padding into the text, so the tree
	// structure is real markup instead of leading spaces in a string.
	const INDENT = ['', 'ml-6', 'ml-12'];
	$: indent = INDENT[Math.min(clause.depth, 2)];
</script>

<div class={indent}>
	<div class="group flex items-start gap-2 py-1">
		<span class="mt-0.5 shrink-0 font-mono text-xs text-amber-500/70">{clause.label}.</span>
		<p class="flex-1 text-slate-300">{clause.body}</p>

		{#if editMode}
			<div class="flex shrink-0 gap-1 opacity-0 transition-opacity group-hover:opacity-100">
				<button
					type="button"
					title="Propose a change to this clause"
					class="rounded p-1 text-slate-400 hover:bg-slate-700 hover:text-amber-400"
					on:click={() =>
						onPropose({
							type: 'edit_clause',
							sectionId,
							clauseUid: clause.clauseUid,
							currentText: clause.body
						})}
				>
					<Edit3 class="h-3.5 w-3.5" />
				</button>
				<button
					type="button"
					title="Propose a sub-clause under this one"
					class="rounded p-1 text-slate-400 hover:bg-slate-700 hover:text-amber-400"
					on:click={() =>
						onPropose({
							type: 'add_clause',
							sectionId,
							clauseUid: clause.clauseUid,
							currentText: ''
						})}
				>
					<Plus class="h-3.5 w-3.5" />
				</button>
				<button
					type="button"
					title="Propose removing this clause"
					class="rounded p-1 text-slate-400 hover:bg-slate-700 hover:text-red-400"
					on:click={() =>
						onPropose({
							type: 'delete_clause',
							sectionId,
							clauseUid: clause.clauseUid,
							currentText: clause.body
						})}
				>
					<Trash2 class="h-3.5 w-3.5" />
				</button>
			</div>
		{/if}
	</div>

	{#each clause.children as child (child.clauseKey)}
		<Self clause={child} {sectionId} {editMode} {onPropose} />
	{/each}
</div>

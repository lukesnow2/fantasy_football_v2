<script lang="ts">
	import { enhance } from '$app/forms';
	import { X } from 'lucide-svelte';

	export let draft: {
		type: 'edit_clause' | 'add_clause' | 'delete_clause';
		sectionId: string;
		clauseUid: string | null;
		currentText: string;
	};
	export let sectionTitle: string;
	export let categoryLabels: Record<string, string>;
	export let members: Array<{ managerKey: number; displayName: string | null }> = [];
	export let error: string | null = null;
	export let onClose: () => void;

	let category = 'general';
	let proposedLanguage = draft.type === 'edit_clause' ? draft.currentText : '';
	let submitting = false;

	const nextSeason = new Date().getFullYear() + 1;

	const TYPE_TITLE = {
		edit_clause: 'Propose a change',
		add_clause: 'Propose a new clause',
		delete_clause: 'Propose removing a clause'
	} as const;
</script>

<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
	<div class="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-xl border border-slate-700 bg-slate-800 p-6">
		<div class="mb-4 flex items-start justify-between">
			<div>
				<h3 class="text-lg font-semibold text-white">{TYPE_TITLE[draft.type]}</h3>
				<p class="text-sm text-slate-400">{sectionTitle}</p>
			</div>
			<button type="button" on:click={onClose} class="rounded p-1 text-slate-400 hover:bg-slate-700">
				<X class="h-5 w-5" />
			</button>
		</div>

		<form
			method="POST"
			action="?/propose"
			use:enhance={() => {
				submitting = true;
				return async ({ update }) => {
					await update();
					submitting = false;
				};
			}}
			class="space-y-4"
		>
			<input type="hidden" name="proposalType" value={draft.type} />
			<input type="hidden" name="affectedSection" value={draft.sectionId} />
			<input type="hidden" name="targetClauseUid" value={draft.clauseUid ?? ''} />
			<input type="hidden" name="currentLanguage" value={draft.currentText} />

			{#if draft.currentText}
				<div>
					<span class="mb-1 block text-sm font-medium text-slate-300">Current wording</span>
					<p class="rounded bg-slate-900 p-3 text-sm text-slate-400">{draft.currentText}</p>
				</div>
			{/if}

			<div>
				<label for="title" class="mb-1 block text-sm font-medium text-slate-300">Title</label>
				<input
					id="title"
					name="title"
					placeholder="Short name for this proposal"
					class="w-full rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white placeholder-slate-500"
				/>
			</div>

			<!--
				The category is chosen, never inferred. Keyword-matching the proposal
				text meant a rationale that mentioned "scoring" silently dropped a
				constitutional amendment to a simple majority.
			-->
			<div>
				<label for="category" class="mb-1 block text-sm font-medium text-slate-300">
					What kind of change is this?
				</label>
				<select
					id="category"
					name="category"
					bind:value={category}
					required
					class="w-full rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white"
				>
					{#each Object.entries(categoryLabels) as [value, label]}
						<option {value}>{label}</option>
					{/each}
				</select>
				<p class="mt-1 text-xs text-slate-500">
					This sets the vote threshold under Article 8. Pick honestly — it decides how many votes
					your proposal needs.
				</p>
			</div>

			{#if category === 'manager_removal'}
				<div>
					<label for="subject" class="mb-1 block text-sm font-medium text-slate-300">
						Which manager?
					</label>
					<select
						id="subject"
						name="subjectManagerKey"
						required
						class="w-full rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white"
					>
						<option value="">Choose…</option>
						{#each members as member}
							<option value={member.managerKey}>{member.displayName}</option>
						{/each}
					</select>
					<p class="mt-1 text-xs text-slate-500">
						They're excluded from the vote and from the count (Article 8, IV).
					</p>
				</div>
			{/if}

			{#if draft.type !== 'delete_clause'}
				<div>
					<label for="proposedLanguage" class="mb-1 block text-sm font-medium text-slate-300">
						Proposed wording
					</label>
					<textarea
						id="proposedLanguage"
						name="proposedLanguage"
						bind:value={proposedLanguage}
						rows="4"
						required
						class="w-full rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white placeholder-slate-500"
						placeholder="Write the clause exactly as it should read"
					></textarea>
				</div>
			{/if}

			<div>
				<label for="rationale" class="mb-1 block text-sm font-medium text-slate-300">
					Why is this needed?
				</label>
				<textarea
					id="rationale"
					name="rationale"
					rows="3"
					required
					class="w-full rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white placeholder-slate-500"
					placeholder="This goes in the email to the league and into the amendment record"
				></textarea>
			</div>

			<div>
				<label for="effectiveSeason" class="mb-1 block text-sm font-medium text-slate-300">
					Effective season
				</label>
				<input
					id="effectiveSeason"
					name="effectiveSeason"
					type="number"
					value={nextSeason}
					min={new Date().getFullYear()}
					max={nextSeason + 5}
					required
					class="w-full rounded border border-slate-600 bg-slate-900 px-3 py-2 text-white"
				/>
			</div>

			{#if error}
				<p class="text-sm text-red-400">{error}</p>
			{/if}

			<div class="flex gap-2 pt-2">
				<button
					disabled={submitting}
					class="rounded bg-amber-500 px-4 py-2 font-semibold text-slate-900 hover:bg-amber-400 disabled:opacity-50"
				>
					{submitting ? 'Saving…' : 'Save as draft'}
				</button>
				<button
					type="button"
					on:click={onClose}
					class="rounded border border-slate-600 px-4 py-2 text-slate-300 hover:bg-slate-700"
				>
					Cancel
				</button>
			</div>
			<p class="text-xs text-slate-500">
				Saved as a draft first. Nobody sees it until you open it for voting.
			</p>
		</form>
	</div>
</div>

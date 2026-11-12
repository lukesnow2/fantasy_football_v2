<script lang="ts">
	/**
	 * The Propose a change dialog (WP11 S11.5): what you're changing, what kind
	 * of change it is and the vote that sets, the new wording, why, and the
	 * season it takes effect. It saves a draft only you can see.
	 */
	import { enhance } from '$app/forms';
	import { Button, Dialog, TextField } from '$lib/components/ui';
	import { articleParts, thresholdHint } from '$lib/constitution/display';

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

	const thisYear = new Date().getFullYear();
	const nextSeason = thisYear + 1;

	const TYPE_TITLE = {
		edit_clause: 'Propose a change',
		add_clause: 'Propose a new clause',
		delete_clause: 'Propose removing a clause'
	} as const;

	$: parts = articleParts({ title: sectionTitle });
	$: kicker = parts.numeral ? `${parts.cite} · ${parts.name}` : parts.name;

	// "Other constitutional change" first and chosen, then the rest in their
	// stored order.
	$: categoryOptions = [
		...Object.entries(categoryLabels).filter(([value]) => value === 'general'),
		...Object.entries(categoryLabels).filter(([value]) => value !== 'general')
	].map(([value, label]) => ({ value, label }));

	$: memberOptions = [
		{ value: '', label: 'Choose…' },
		...members.map((m) => ({ value: String(m.managerKey), label: m.displayName ?? 'Unknown' }))
	];
</script>

<Dialog {kicker} title={TYPE_TITLE[draft.type]} {onClose}>
	<form
		method="POST"
		action="?/propose"
		use:enhance={() => {
			submitting = true;
			return async ({ result, update }) => {
				await update();
				submitting = false;
				// Saved: the draft is on the page behind, so the dialog's done.
				if (result.type === 'success') onClose();
			};
		}}
	>
		<input type="hidden" name="proposalType" value={draft.type} />
		<input type="hidden" name="affectedSection" value={draft.sectionId} />
		<input type="hidden" name="targetClauseUid" value={draft.clauseUid ?? ''} />
		<input type="hidden" name="currentLanguage" value={draft.currentText} />

		{#if draft.currentText}
			<div class="well">
				<div class="well-label">Current wording</div>
				<div class="well-text">{draft.currentText}</div>
			</div>
		{/if}

		<TextField
			label="Title"
			hint="Optional"
			phoneHintInline
			name="title"
			placeholder="Short name for this proposal"
			focusBorder
		/>

		<!--
			The category is chosen, never inferred. Keyword-matching the proposal
			text meant a rationale that mentioned "scoring" silently dropped a
			constitutional amendment to a simple majority.
		-->
		<TextField
			kind="select"
			label="What kind of change is this?"
			name="category"
			required
			options={categoryOptions}
			bind:value={category}
			helper="This sets how many votes it needs under Article VIII. {thresholdHint(category)}"
			helperWeight={400}
			focusBorder
		/>

		{#if category === 'manager_removal'}
			<TextField
				kind="select"
				label="Which manager?"
				name="subjectManagerKey"
				required
				options={memberOptions}
				helper="They're excluded from the vote and from the count (Article VIII, IV)."
				helperWeight={400}
				focusBorder
			/>
		{/if}

		{#if draft.type !== 'delete_clause'}
			<TextField
				kind="textarea"
				label="Proposed wording"
				name="proposedLanguage"
				rows={3}
				required
				placeholder="Write the clause exactly as it should read"
				bind:value={proposedLanguage}
				focusBorder
			/>
		{/if}

		<TextField
			kind="textarea"
			label="Why is this needed?"
			name="rationale"
			rows={3}
			required
			placeholder="This goes in the email to the league and in the amendment record."
			focusBorder
		/>

		<TextField
			label="Takes effect"
			name="effectiveSeason"
			type="number"
			value={nextSeason}
			min={thisYear}
			max={nextSeason + 5}
			required
			focusBorder
			class="season"
		/>

		{#if error}
			<p class="error" role="alert">{error}</p>
		{/if}

		<div class="footer">
			<Button type="submit" height={46} padX={20} bold disabled={submitting}>
				{submitting ? 'Saving…' : 'Save as draft'}
			</Button>
			<Button variant="secondary" height={46} onclick={onClose}>Cancel</Button>
			<span class="note">Saved as a draft first. Nobody sees it until you open it for voting.</span>
		</div>
	</form>
</Dialog>

<style>
	form {
		display: flex;
		flex-direction: column;
		gap: 16px;
		padding: 22px 26px;
	}

	.well {
		padding: 12px 14px;
		border-radius: 10px;
		background: var(--color-ground);
	}

	.well-label {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-muted);
	}

	.well-text {
		margin-top: 4px;
		font-size: 15px;
	}

	form :global(.season) {
		max-width: 200px;
	}

	.error {
		margin: 0;
		font-size: 14px;
		color: var(--color-pennant-dark);
	}

	.footer {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 10px;
		padding-top: 6px;
		border-top: 1px solid var(--color-line-soft);
	}

	.note {
		font-size: 13px;
		color: var(--color-muted);
	}

	@media (max-width: 639.98px) {
		form {
			padding: 18px 16px calc(24px + env(safe-area-inset-bottom));
		}

		.well-text {
			line-height: 1.5;
		}

		form :global(.helper) {
			line-height: 1.45;
		}

		form :global(.control) {
			box-sizing: border-box;
			width: 100%;
		}

		.footer {
			padding-top: 12px;
		}

		.footer :global(.btn) {
			flex: 1 1 0;
		}

		.note {
			flex: 1 1 100%;
			line-height: 1.45;
		}
	}
</style>

<script lang="ts">
	import { enhance } from '$app/forms';
	import { Check, ChevronDown, ChevronRight, Clock, Send, X } from 'lucide-svelte';

	export let proposal: any;

	// Per-card state. The bug this replaces was a single module-level `voteData`
	// object bound by every proposal's radio group, so choosing "no" on one
	// proposal visibly changed the selection on all the others.
	let expanded = false;
	let selectedVote: string = proposal.myVote ?? '';
	let comment = '';
	let submitting = false;

	$: outcome = proposal.outcome;
	$: isOpen = outcome.state === 'open';
	$: progress = Math.min(
		100,
		Math.round((proposal.yesVotes / Math.max(proposal.threshold.requiredYes, 1)) * 100)
	);

	const STATUS_STYLE: Record<string, string> = {
		draft: 'bg-slate-700 text-slate-300',
		active: 'bg-amber-900/40 text-amber-300 border border-amber-600/40',
		passed: 'bg-green-900/40 text-green-300 border border-green-600/40',
		rejected: 'bg-red-900/30 text-red-300 border border-red-600/30',
		withdrawn: 'bg-slate-800 text-slate-400',
		superseded: 'bg-slate-800 text-slate-400'
	};

	function closesIn(date: string | Date | null): string {
		if (!date) return '';
		const ms = new Date(date).getTime() - Date.now();
		if (ms <= 0) return 'closing now';
		const days = Math.floor(ms / 86_400_000);
		if (days >= 1) return `${days} day${days === 1 ? '' : 's'} left`;
		const hours = Math.max(1, Math.floor(ms / 3_600_000));
		return `${hours} hour${hours === 1 ? '' : 's'} left`;
	}
</script>

<div class="rounded-lg border border-slate-700 bg-slate-800/50 p-4">
	<button
		type="button"
		class="flex w-full items-start justify-between gap-3 text-left"
		on:click={() => (expanded = !expanded)}
	>
		<div class="min-w-0 flex-1">
			<div class="mb-1.5 flex flex-wrap items-center gap-2">
				<span class="rounded px-2 py-0.5 text-xs font-medium {STATUS_STYLE[proposal.status] ?? ''}">
					{proposal.status}
				</span>
				<span class="text-xs text-slate-500">{proposal.categoryLabel}</span>
				{#if isOpen && proposal.votingEndDate}
					<span class="flex items-center gap-1 text-xs text-slate-500">
						<Clock class="h-3 w-3" />{closesIn(proposal.votingEndDate)}
					</span>
				{/if}
			</div>
			<h4 class="truncate font-medium text-white">{proposal.title}</h4>
			<p class="mt-0.5 text-xs text-slate-400">
				by {proposal.authorName ?? 'Unknown'} · effective {proposal.effectiveSeason}
			</p>
		</div>

		<div class="flex shrink-0 items-center gap-3">
			<div class="text-right">
				<div class="text-sm font-semibold text-white">
					{proposal.yesVotes}<span class="text-slate-500">/{proposal.threshold.requiredYes}</span>
				</div>
				<div class="text-xs text-slate-500">{proposal.threshold.rule.replace('_', ' ')}</div>
			</div>
			{#if expanded}
				<ChevronDown class="h-4 w-4 text-slate-400" />
			{:else}
				<ChevronRight class="h-4 w-4 text-slate-400" />
			{/if}
		</div>
	</button>

	<div class="mt-3 h-1.5 overflow-hidden rounded-full bg-slate-700">
		<div
			class="h-full transition-all {outcome.state === 'passed'
				? 'bg-green-500'
				: outcome.state === 'rejected'
					? 'bg-red-500/60'
					: 'bg-amber-500'}"
			style="width: {progress}%"
		></div>
	</div>

	{#if expanded}
		<div class="mt-4 space-y-4 border-t border-slate-700 pt-4">
			{#if proposal.currentLanguage}
				<div>
					<p class="mb-1 text-xs font-medium text-slate-400">Current wording</p>
					<p class="rounded bg-slate-900/60 p-2 text-sm text-slate-400 line-through">
						{proposal.currentLanguage}
					</p>
				</div>
			{/if}

			{#if proposal.proposedLanguage}
				<div>
					<p class="mb-1 text-xs font-medium text-slate-400">
						{proposal.proposalType === 'delete_clause' ? 'To be removed' : 'Proposed wording'}
					</p>
					<p class="rounded border-l-2 border-amber-500 bg-slate-900/60 p-2 text-sm text-slate-200">
						{proposal.proposedLanguage}
					</p>
				</div>
			{/if}

			<div>
				<p class="mb-1 text-xs font-medium text-slate-400">Rationale</p>
				<p class="text-sm text-slate-300">{proposal.rationale}</p>
			</div>

			<p class="text-xs text-slate-500">
				{proposal.threshold.label} per {proposal.threshold.citation}.
				{#if isOpen}
					Not voting counts the same as voting no.
				{/if}
			</p>

			{#if proposal.status === 'draft' && proposal.isAuthor}
				<div class="flex gap-2">
					<form method="POST" action="?/open" use:enhance>
						<input type="hidden" name="proposalKey" value={proposal.proposalKey} />
						<button
							class="flex items-center gap-1.5 rounded bg-amber-500 px-3 py-1.5 text-sm font-semibold text-slate-900 hover:bg-amber-400"
						>
							<Send class="h-3.5 w-3.5" /> Put it to the league
						</button>
					</form>
					<form method="POST" action="?/withdraw" use:enhance>
						<input type="hidden" name="proposalKey" value={proposal.proposalKey} />
						<button class="rounded border border-slate-600 px-3 py-1.5 text-sm text-slate-300 hover:bg-slate-700">
							Discard
						</button>
					</form>
				</div>
				<p class="text-xs text-slate-500">
					Nobody else can see this until you open it for voting.
				</p>
			{/if}

			{#if proposal.canVote}
				<form
					method="POST"
					action="?/vote"
					use:enhance={() => {
						submitting = true;
						return async ({ update }) => {
							await update();
							submitting = false;
						};
					}}
					class="space-y-3 rounded-lg bg-slate-900/40 p-3"
				>
					<input type="hidden" name="proposalKey" value={proposal.proposalKey} />

					<div class="flex flex-wrap gap-2">
						{#each ['yes', 'no', 'abstain'] as option}
							<label
								class="cursor-pointer rounded border px-3 py-1.5 text-sm capitalize transition-colors
									{selectedVote === option
									? 'border-amber-500 bg-amber-500/20 text-amber-300'
									: 'border-slate-600 text-slate-300 hover:border-slate-500'}"
							>
								<input
									type="radio"
									name="vote"
									value={option}
									bind:group={selectedVote}
									class="sr-only"
								/>
								{option}
							</label>
						{/each}
					</div>

					<input
						name="comment"
						bind:value={comment}
						placeholder="Comment (optional)"
						class="w-full rounded border border-slate-600 bg-slate-900 px-3 py-1.5 text-sm text-white placeholder-slate-500"
					/>

					<button
						disabled={!selectedVote || submitting}
						class="rounded bg-amber-500 px-4 py-1.5 text-sm font-semibold text-slate-900 hover:bg-amber-400 disabled:opacity-50"
					>
						{proposal.myVote ? 'Change my vote' : 'Cast vote'}
					</button>

					{#if proposal.myVote}
						<p class="text-xs text-slate-500">
							You voted <strong class="text-slate-300">{proposal.myVote}</strong>. You can change it
							while voting is open.
						</p>
					{/if}
				</form>
			{:else if proposal.subjectManagerKey && !proposal.canVote && proposal.status === 'active'}
				<p class="text-xs text-amber-400/80">
					You're the subject of this proposal, so you don't vote on it (Article 8, IV).
				</p>
			{/if}

			{#if proposal.ballots.length > 0}
				<div>
					<p class="mb-2 text-xs font-medium text-slate-400">
						Ballots ({proposal.ballots.length})
					</p>
					<ul class="space-y-1">
						{#each proposal.ballots as ballot}
							<li class="flex items-start gap-2 text-sm">
								{#if ballot.vote === 'yes'}
									<Check class="mt-0.5 h-3.5 w-3.5 shrink-0 text-green-400" />
								{:else if ballot.vote === 'no'}
									<X class="mt-0.5 h-3.5 w-3.5 shrink-0 text-red-400" />
								{:else}
									<span class="mt-0.5 h-3.5 w-3.5 shrink-0 text-center text-xs text-slate-500">–</span>
								{/if}
								<span class="text-slate-300">{ballot.managerName ?? 'Unknown'}</span>
								{#if ballot.comment}
									<span class="text-slate-500">— {ballot.comment}</span>
								{/if}
							</li>
						{/each}
					</ul>
				</div>
			{/if}
		</div>
	{/if}
</div>

<script lang="ts">
	/**
	 * One proposal (WP11 S11.6, S11.7): a card whose header is a button that
	 * opens it, with the status, the deadline, the title, who wrote it, which
	 * clause it touches and the count. Open, it shows the old and new wording,
	 * the reason, the bar it needs, your ballot and everyone else's.
	 */
	import { enhance } from '$app/forms';
	import { Avatar, Button, Card, OddsBar, RadioChip, Tag, TextField } from '$lib/components/ui';
	import {
		ballotRows,
		citationLabel,
		clauseRef,
		closesIn,
		ruleName,
		statusTag,
		voteProgress
	} from '$lib/constitution/display';
	import { managerSlug } from '$lib/history-display';

	interface Props {
		proposal: any;
		/** The document's sections, to cite the clause it touches. */
		sections?: any[];
		/** Active members, for the ballot list. */
		members?: Array<{ managerKey: number; displayName: string | null }>;
		/** Ballots are only loaded for a signed-in member. */
		showBallots?: boolean;
	}

	let { proposal, sections = [], members = [], showBallots = false }: Props = $props();

	const SETTLED = ['passed', 'rejected', 'withdrawn', 'superseded'];
	const VOTES = [
		{ value: 'yes', label: 'Yes' },
		{ value: 'no', label: 'No' },
		{ value: 'abstain', label: 'Abstain' }
	];

	// Per-card state. The bug this replaces was a single module-level `voteData`
	// object bound by every proposal's radio group, so choosing "no" on one
	// proposal visibly changed the selection on all the others.
	// Open proposals start expanded; drafts and settled ones start collapsed.
	let expanded = $state(proposal.status === 'active');
	let selectedVote: string = $state(proposal.myVote ?? '');
	let comment = $state('');
	let submitting = $state(false);

	const id = $props.id();
	const bodyId = `proposal-${id}`;

	const isOpen = $derived(proposal.outcome?.state === 'open');
	const settled = $derived(SETTLED.includes(proposal.status));
	const tag = $derived(statusTag(proposal.status));
	const progress = $derived(voteProgress(proposal.yesVotes, proposal.threshold.requiredYes));
	const ref = $derived(clauseRef(proposal, sections));
	const meta = $derived(
		[
			`by ${proposal.authorName ?? 'Unknown'}`,
			`takes effect in ${proposal.effectiveSeason}`,
			ref
		]
			.filter(Boolean)
			.join(' · ')
	);
	const rows = $derived(ballotRows(members, proposal.ballots ?? [], proposal.subjectManagerKey ?? null));
	// Signed in, the proposal open and no vote allowed: the only reason is that
	// it's about you.
	const isSubject = $derived(
		showBallots && proposal.status === 'active' && proposal.subjectManagerKey != null && !proposal.canVote
	);
	const barLine = $derived(
		`${proposal.threshold.label} per ${citationLabel(proposal.threshold.citation)}.` +
			(isOpen ? ' Not voting counts the same as voting no.' : '')
	);
	const isRemoval = $derived(proposal.proposalType === 'delete_clause');
	const showCurrent = $derived(proposal.proposalType !== 'add_clause' && !!proposal.currentLanguage);
	const showProposed = $derived(!isRemoval && !!proposal.proposedLanguage);
</script>

<Card flush class="proposal">
	<button
		type="button"
		class="head"
		aria-expanded={expanded}
		aria-controls={bodyId}
		onclick={() => (expanded = !expanded)}
	>
		<span class="line">
			<Tag variant={tag.variant} size="status">{tag.label}</Tag>
			<Tag variant="pending" size="status">{proposal.categoryLabel}</Tag>
			{#if isOpen && proposal.votingEndDate}
				<span class="closes">{closesIn(proposal.votingEndDate)}</span>
			{/if}
		</span>
		<span class="title">{proposal.title}</span>
		<span class="meta">{meta}</span>
		<span class="count">
			<OddsBar percent={progress} height={10} track="wash" gap={12} phoneGap={10}>
				{#if settled}
					<span class="tally"
						>{proposal.yesVotes} yes · {proposal.noVotes} no · {proposal.abstainVotes} abstain</span
					>
				{:else}
					<span class="tally">{proposal.yesVotes} of {proposal.threshold.requiredYes} yes</span>
				{/if}
				<span class="rule">{ruleName(proposal.threshold.label)}</span>
			</OddsBar>
		</span>
	</button>

	{#if expanded}
		<div class="body" id={bodyId}>
			<div class="proposal-col">
				{#if showCurrent || showProposed}
					<div class="wording" class:pair={showCurrent && showProposed}>
						{#if showCurrent}
							<div class="well current">
								<div class="well-label">{isRemoval ? 'To be removed' : 'Current wording'}</div>
								<div class="well-text">{proposal.currentLanguage}</div>
							</div>
						{/if}
						{#if showProposed}
							<div class="well proposed">
								<div class="well-label">Proposed wording</div>
								<div class="well-text">{proposal.proposedLanguage}</div>
							</div>
						{/if}
					</div>
				{/if}

				{#if proposal.rationale}
					<div class="why">
						<div class="why-label">Why</div>
						<div class="why-text">{proposal.rationale}</div>
					</div>
				{/if}

				<p class="bar-line">{barLine}</p>

				{#if proposal.status === 'draft' && proposal.isAuthor}
					<div class="draft-actions">
						<form method="POST" action="?/open" use:enhance>
							<input type="hidden" name="proposalKey" value={proposal.proposalKey} />
							<Button type="submit" bold padX={20}>Put it to the league</Button>
						</form>
						<form method="POST" action="?/withdraw" use:enhance>
							<input type="hidden" name="proposalKey" value={proposal.proposalKey} />
							<Button type="submit" variant="secondary">Discard</Button>
						</form>
						<span class="note">Nobody else can see this until you open it for voting.</span>
					</div>
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
					>
						<input type="hidden" name="proposalKey" value={proposal.proposalKey} />
						<fieldset class="ballot">
							<legend>Your vote</legend>
							<div class="pills">
								{#each VOTES as option (option.value)}
									<RadioChip
										size="vote"
										name="vote"
										value={option.value}
										label={option.label}
										checked={selectedVote === option.value}
										onchange={() => (selectedVote = option.value)}
									/>
								{/each}
							</div>
							<div class="comment">
								<TextField
									kind="textarea"
									size="sm"
									label="Comment"
									hideLabel
									name="comment"
									rows={2}
									placeholder="Add a comment (optional)"
									bind:value={comment}
								/>
							</div>
							<div class="submit">
								<Button type="submit" bold padX={20} disabled={!selectedVote || submitting}>
									{proposal.myVote ? 'Change my vote' : 'Cast vote'}
								</Button>
								{#if proposal.myVote}
									<span class="voted"
										>You voted {proposal.myVote}. You can change it while voting is open.</span
									>
								{/if}
							</div>
						</fieldset>
					</form>
				{:else if isSubject}
					<p class="subject">
						You're the subject of this proposal, so you don't vote on it (Article VIII, IV).
					</p>
				{/if}
			</div>

			{#if showBallots}
				<div class="ballots">
					<div class="ballots-head">Ballots ({proposal.ballots?.length ?? 0})</div>
					{#each rows as row (row.managerKey)}
						<div class="ballot-row" class:waiting={!row.voted}>
							<Avatar name={row.name} size={26} alt="" class="ballot-photo" />
							<div class="who">
								<a class="name" href="/managers/{managerSlug(row.name)}">{row.name}</a>
								{#if row.comment}<span class="ballot-comment">{row.comment}</span>{/if}
							</div>
							<span class="mark tone-{row.tone}">{row.mark}</span>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</Card>

<style>
	:global(.card.proposal) {
		overflow: hidden;
	}

	.head {
		display: block;
		width: 100%;
		padding: 20px 24px;
		border: 0;
		background: var(--color-card);
		color: var(--color-ink);
		font: inherit;
		text-align: left;
		cursor: pointer;
	}

	.head:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: -2px;
	}

	.line {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px;
	}

	.closes {
		margin-left: auto;
		font-size: 13px;
		font-weight: 700;
		color: var(--color-pennant-dark);
	}

	.title {
		display: block;
		margin-top: 10px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 26px;
	}

	.meta {
		display: block;
		margin-top: 4px;
		font-size: 14px;
		color: var(--color-muted);
	}

	.count {
		display: block;
		margin-top: 14px;
	}

	.tally {
		font-size: 14px;
		font-weight: 700;
	}

	.rule {
		font-size: 13px;
		color: var(--color-muted);
	}

	.body {
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		padding: 0 24px 24px;
	}

	.proposal-col {
		flex: 2 1 420px;
		min-width: 0;
	}

	.ballots {
		flex: 1 1 260px;
		min-width: 0;
	}

	.wording {
		display: grid;
		gap: 12px;
	}

	.wording.pair {
		grid-template-columns: repeat(2, minmax(0, 1fr));
	}

	.well {
		padding: 14px 16px;
		border-radius: 10px;
	}

	.current {
		background: var(--color-ground);
	}

	.proposed {
		background: var(--color-field-tint);
	}

	.well-label,
	.why-label {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-muted);
	}

	.proposed .well-label {
		color: var(--color-field);
	}

	.well-text {
		margin-top: 6px;
		font-size: 15px;
		line-height: 1.5;
	}

	.current .well-text {
		color: var(--color-muted);
		text-decoration: line-through;
	}

	.proposed .well-text {
		font-weight: 600;
	}

	.why {
		margin-top: 14px;
	}

	.why-text {
		margin-top: 4px;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	.bar-line {
		margin: 14px 0 0;
		font-size: 13px;
		color: var(--color-muted);
	}

	.draft-actions {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 10px;
		margin-top: 18px;
	}

	.note {
		font-size: 13px;
		color: var(--color-muted);
	}

	.ballot {
		margin: 18px 0 0;
		padding: 16px;
		border: 1px solid var(--color-line);
		border-radius: 10px;
		min-width: 0;
	}

	legend {
		padding: 0 6px;
		font-size: 13px;
		font-weight: 700;
	}

	.pills {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
	}

	/* The board's textarea is inline, so its line leaves 4px under it; the
	   phone board's is a block. */
	.comment {
		margin-top: 10px;
		padding-bottom: 4px;
	}

	.submit {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 12px;
		margin-top: 10px;
	}

	.voted {
		font-size: 13px;
		color: var(--color-ink-2);
	}

	.subject {
		margin: 18px 0 0;
		font-size: 14px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	.ballots-head {
		font-size: 13px;
		font-weight: 700;
	}

	.ballot-row {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 7px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 14px;
	}

	.waiting :global(.ballot-photo) {
		opacity: 0.6;
	}

	.who {
		flex: 1;
		min-width: 0;
		display: flex;
		flex-direction: column;
	}

	.name {
		font-weight: 600;
		color: var(--color-ink);
	}

	.waiting .name {
		color: var(--color-ink-2);
	}

	.name:hover {
		color: var(--color-field);
	}

	.ballot-comment {
		font-size: 13px;
		color: var(--color-ink-2);
	}

	.mark {
		font-size: 13px;
		font-weight: 700;
	}

	.tone-yes {
		color: var(--color-field);
	}

	.tone-no {
		color: var(--color-pennant-dark);
	}

	.tone-abstain {
		color: var(--color-ink-2);
	}

	.tone-pending,
	.tone-excluded {
		color: var(--color-muted);
	}

	@media (max-width: 639.98px) {
		.head {
			padding: 16px;
		}

		.closes {
			margin-left: 0;
		}

		.title {
			font-size: 22px;
			line-height: 1.2;
		}

		.meta {
			line-height: 1.45;
		}

		.body {
			flex-direction: column;
			padding: 0 16px 16px;
		}

		.wording.pair {
			grid-template-columns: minmax(0, 1fr);
			gap: 10px;
		}

		.well {
			padding: 12px 14px;
		}

		.bar-line {
			line-height: 1.45;
		}

		.ballot {
			padding: 14px;
		}

		.pills {
			display: grid;
			grid-template-columns: repeat(3, minmax(0, 1fr));
		}

		.comment {
			padding-bottom: 0;
		}

		.submit {
			display: block;
		}

		.submit :global(.btn) {
			width: 100%;
		}

		.voted {
			display: block;
			margin-top: 8px;
			line-height: 1.45;
		}

		.ballots-head {
			padding-bottom: 4px;
		}

		/* Content-box, as the board draws it: 44px plus the rule. */
		.ballot-row {
			box-sizing: content-box;
			min-height: 44px;
			padding: 0;
		}

		.who {
			align-self: stretch;
			justify-content: center;
		}

		.name {
			display: flex;
			align-items: center;
			flex: 1;
		}
	}
</style>

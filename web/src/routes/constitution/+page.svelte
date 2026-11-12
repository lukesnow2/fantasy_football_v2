<script lang="ts">
	import { onMount, tick } from 'svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import { Button, Card, Tag } from '$lib/components/ui';
	import ClauseView from '$lib/components/constitution/ClauseView.svelte';
	import ProposalCard from '$lib/components/constitution/ProposalCard.svelte';
	import ProposalForm from '$lib/components/constitution/ProposalForm.svelte';
	import { amendmentTally, articleParts, longDate, shortDate } from '$lib/constitution/display';
	import type { ActionData, PageData } from './$types';

	export let data: PageData;
	export let form: ActionData;

	const LOGIN = '/login?redirect=/constitution';

	const THRESHOLDS = [
		{ what: 'Scoring or roster size', needs: 'Simple majority' },
		{ what: 'Any other change', needs: 'Super-majority' },
		{ what: 'League size', needs: 'Unanimous' },
		{ what: 'Removing a manager for anything but a rule violation', needs: 'Unanimous, minus that manager' }
	];

	let expanded: Record<string, boolean> = {};
	let editMode = false;
	let draft: {
		type: 'edit_clause' | 'add_clause' | 'delete_clause';
		sectionId: string;
		clauseUid: string | null;
		currentText: string;
	} | null = null;

	$: openProposals = data.proposals.filter((p) => p.status === 'active');
	$: myDrafts = data.proposals.filter((p) => p.status === 'draft');
	$: settled = data.proposals.filter((p) =>
		['passed', 'rejected', 'withdrawn', 'superseded'].includes(p.status)
	);

	// Rendered in UTC. effective_at is a UTC timestamp, and formatting it in the
	// viewer's zone slides a midnight date back a day: "January 2025" became
	// "December 2024" for anyone west of Greenwich.
	$: kicker = data.version
		? `League · Version ${data.version.versionNo} · in effect since ${longDate(data.version.effectiveAt)}`
		: 'League · Not yet seeded';

	$: articles = data.sections.map((section) => ({ section, ...articleParts(section) }));

	function proposalsFor(sectionId: string) {
		return openProposals.filter((p) => p.affectedSection === sectionId);
	}

	function startProposal(input: typeof draft) {
		draft = input;
	}

	/** Articles with an open proposal start expanded, and the one you came from. */
	function openProposeMode(focusSection?: string) {
		const next: Record<string, boolean> = {};
		for (const s of data.sections) {
			next[s.sectionId] = s.sectionId === focusSection || proposalsFor(s.sectionId).length > 0;
		}
		expanded = next;
		editMode = true;
	}

	function toggleProposeMode() {
		if (editMode) editMode = false;
		else openProposeMode();
	}

	async function proposeAmendment(sectionId: string) {
		openProposeMode(sectionId);
		await tick();
		document.getElementById(sectionId)?.scrollIntoView({ block: 'start' });
	}

	function versionWhat(v: { versionNo: number; note: string | null; amendmentKey: number | null }) {
		return v.amendmentKey == null ? 'original' : (v.note ?? 'amendment');
	}

	// Contents: the current item is the last article whose top has scrolled
	// past the top of the viewport, or the first one before any has. A Contents
	// link lands an article its scroll margin (16px) below the top, so that
	// counts as past it.
	const SCROLL_MARGIN = 16;
	let current = '';

	function updateCurrent() {
		let found = data.sections[0]?.sectionId ?? '';
		for (const s of data.sections) {
			const el = document.getElementById(s.sectionId);
			if (el && el.getBoundingClientRect().top <= SCROLL_MARGIN + 1) found = s.sectionId;
		}
		current = found;
	}

	$: current = current || data.sections[0]?.sectionId || '';

	// The observer only says when an article crosses the top edge; which one
	// is current is then read off every article's position.
	let observer: IntersectionObserver | null = null;

	function observeAll() {
		if (!observer) return;
		observer.disconnect();
		for (const s of data.sections) {
			const el = document.getElementById(s.sectionId);
			if (el) observer.observe(el);
		}
		updateCurrent();
	}

	// The reading layout's articles are new elements each time propose mode ends.
	$: if (!editMode) tick().then(observeAll);

	onMount(() => {
		observer = new IntersectionObserver(updateCurrent, {
			rootMargin: '0px 0px -100% 0px',
			threshold: [0, 1]
		});
		observeAll();
		const onScroll = () => requestAnimationFrame(updateCurrent);
		window.addEventListener('scroll', onScroll, { passive: true });
		return () => {
			observer?.disconnect();
			observer = null;
			window.removeEventListener('scroll', onScroll);
		};
	});
</script>

<svelte:head>
	<title>Constitution | The League</title>
</svelte:head>

<PageHeader {kicker} title="Constitution" introWidth={640} phoneActionFull phoneKickerLineHeight={1.4}>
	The rules this league runs on. Every clause is versioned, and changes go to a vote.
	{#snippet action()}
		{#if data.canPropose}
			<Button size="lg" padX={20} aria-pressed={editMode} onclick={toggleProposeMode}>
				{editMode ? 'Done proposing' : 'Propose a change'}
			</Button>
		{:else}
			<Button size="lg" padX={20} href={LOGIN}>Propose a change</Button>
		{/if}
	{/snippet}
</PageHeader>

{#if editMode}
	<div class="strip hint">
		<span class="hover-copy">Hover any clause to edit it, add a sub-clause, or propose removing it.</span>
		<span class="touch-copy"
			>Tap the buttons under any clause to edit it, add a sub-clause, or propose removing it.</span
		>
	</div>
{/if}

{#if form?.error}
	<div class="strip error" role="alert">{form.error}</div>
{/if}

{#if myDrafts.length > 0}
	<section class="proposals" aria-labelledby="drafts-title">
		<h2 id="drafts-title">Your drafts <span class="n">({myDrafts.length})</span></h2>
		<div class="stack">
			{#each myDrafts as proposal (proposal.proposalKey)}
				<ProposalCard {proposal} sections={data.sections} members={data.members} showBallots={data.canPropose} />
			{/each}
		</div>
	</section>
{/if}

{#if openProposals.length > 0}
	<section class="proposals" aria-labelledby="open-title">
		<h2 id="open-title">Open for voting <span class="n">({openProposals.length})</span></h2>
		<div class="stack">
			{#each openProposals as proposal (proposal.proposalKey)}
				<ProposalCard {proposal} sections={data.sections} members={data.members} showBallots={data.canPropose} />
			{/each}
		</div>
	</section>
{/if}

{#if data.sections.length === 0}
	<div class="body">
		<Card>
			<p class="empty">
				The constitution hasn't been seeded yet. <span class="muted"
					>Run <code>npm run seed:constitution</code>.</span
				>
			</p>
		</Card>
	</div>
{:else if editMode}
	<div class="body propose">
		{#each articles as { section, numeral, name } (section.sectionKey)}
			{@const pending = proposalsFor(section.sectionId)}
			{@const open = expanded[section.sectionId]}
			<Card flush as="section" id={section.sectionId} class="accordion">
				<h2 class="acc-title">
					<button
						type="button"
						class="acc-head"
						class:open
						aria-expanded={!!open}
						aria-controls="{section.sectionId}-clauses"
						onclick={() => (expanded = { ...expanded, [section.sectionId]: !open })}
					>
						<span class="acc-name"><span class="numeral">{numeral}</span> {name}</span>
						{#if pending.length > 0}
							<Tag variant="champion" size="pill">{pending.length} proposed</Tag>
						{/if}
						<span class="arrow" aria-hidden="true">{open ? '▴' : '▾'}</span>
					</button>
				</h2>
				{#if open}
					<div id="{section.sectionId}-clauses">
						<ol class="clauses propose-list">
							{#each section.clauses as clause (clause.clauseKey)}
								<ClauseView {clause} sectionId={section.sectionId} {editMode} onPropose={startProposal} />
							{/each}
						</ol>
						<div class="new-clause">
							<Button
								variant="dashed"
								onclick={() =>
									startProposal({
										type: 'add_clause',
										sectionId: section.sectionId,
										clauseUid: null,
										currentText: ''
									})}
							>
								+ Propose a new clause in this article
							</Button>
						</div>
					</div>
				{/if}
			</Card>
		{/each}
	</div>
{:else}
	<div class="body reading">
		<Card as="nav" label="Articles" padding="14px" class="toc-card">
			<div class="contents-label">Contents</div>
			{#each articles as { section, numeral, name } (section.sectionKey)}
				<a
					href="#{section.sectionId}"
					class="toc"
					class:current={current === section.sectionId}
					aria-current={current === section.sectionId ? 'location' : undefined}
				>
					<span class="toc-n">{numeral}</span><span>{name}</span>
				</a>
			{/each}
		</Card>

		<div class="articles">
			{#each articles as { section, numeral, name } (section.sectionKey)}
				<Card
					as="article"
					id={section.sectionId}
					class="article"
					title={name}
					titleLead={numeral}
					titleSize={30}
					phoneTitleLineHeight={1.2}
					padding="26px 28px"
					headRule
					headRuleGap={12}
					phoneHeadRuleGap={0}
					headSpacing="8px 16px"
					phoneHeadStack
					phoneHeadStackGap={0}
				>
					{#snippet action()}
						{#if data.canPropose}
							<Button variant="link" size="sm" onclick={() => proposeAmendment(section.sectionId)}>
								Propose an amendment
							</Button>
						{:else}
							<Button variant="link" size="sm" href={LOGIN}>Propose an amendment</Button>
						{/if}
					{/snippet}
					<ol class="clauses">
						{#each section.clauses as clause (clause.clauseKey)}
							<ClauseView {clause} sectionId={section.sectionId} onPropose={startProposal} />
						{/each}
					</ol>
				</Card>
			{/each}
		</div>

		<aside class="aside">
			<Card tone="pennant" title="What it takes to change" titleSize={22} headGap={8} padding="20px 22px">
				{#each THRESHOLDS as row (row.what)}
					<div class="need">
						<div class="need-what">{row.what}</div>
						<div class="need-vote">{row.needs}</div>
					</div>
				{/each}
				<div class="source">From Article VIII.</div>
			</Card>
			{#if data.versions.length > 0}
				<Card title="Versions" titleSize={22} headGap={6} padding="20px 22px">
					{#each data.versions as v (v.versionNo)}
						{@const ahead = new Date(v.effectiveAt).getTime() > Date.now()}
						<div class="version">
							<span><b>Version {v.versionNo}</b> · {versionWhat(v)}</span>
							<span class="date">{ahead ? 'Takes effect ' : ''}{shortDate(v.effectiveAt)}</span>
						</div>
					{/each}
					<p class="versions-note">
						Every amendment that passes becomes a new version, listed here with the date it took
						effect.
					</p>
				</Card>
			{/if}
		</aside>
	</div>
{/if}

{#if data.amendments.length > 0 || settled.length > 0}
	<div class="history">
		{#if data.amendments.length > 0}
			<Card title="Amendment history" titleSize={22} headGap={8} padding="20px 24px" class="history-card">
				<ul class="amendments">
					{#each data.amendments as amendment (amendment.amendmentKey)}
						<li>
							<div class="amend-title">
								{amendment.title} <span class="year">{amendment.year}</span>
							</div>
							{#if amendment.description}<p class="amend-desc">{amendment.description}</p>{/if}
							{#if amendment.voteResults}
								<p class="amend-tally">
									{amendmentTally(amendment.voteResults, amendment.effectiveSeason)}
								</p>
							{/if}
						</li>
					{/each}
				</ul>
			</Card>
		{/if}
		{#if settled.length > 0}
			<Card title="Past proposals" titleSize={22} headGap={12} padding="20px 24px" class="history-card">
				<div class="stack">
					{#each settled as proposal (proposal.proposalKey)}
						<ProposalCard {proposal} sections={data.sections} members={data.members} showBallots={data.canPropose} />
					{/each}
				</div>
			</Card>
		{/if}
	</div>
{/if}

{#if draft}
	<ProposalForm
		{draft}
		sectionTitle={data.sections.find((s) => s.sectionId === draft?.sectionId)?.title ?? ''}
		categoryLabels={data.categoryLabels}
		members={data.members}
		error={form?.error ?? null}
		onClose={() => (draft = null)}
	/>
{/if}

<style>
	.strip {
		margin-top: 16px;
		padding: 12px 16px;
		border-radius: 10px;
		font-size: 14px;
	}

	.hint {
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
	}

	.error {
		background: var(--color-pennant-tint);
		color: var(--color-pennant-dark);
	}

	.touch-copy {
		display: none;
	}

	@media (hover: none) {
		.hover-copy {
			display: none;
		}

		.touch-copy {
			display: inline;
		}
	}

	.proposals {
		margin-top: 24px;
	}

	.proposals h2 {
		margin: 0 0 12px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 26px;
	}

	.n {
		font-family: var(--font-sans);
		font-size: 16px;
		color: var(--color-muted);
	}

	.stack {
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.body {
		margin-top: 28px;
	}

	.reading {
		display: flex;
		flex-wrap: wrap;
		align-items: flex-start;
		gap: 20px;
	}

	.reading :global(.toc-card) {
		flex: 1 1 240px;
	}

	.contents-label {
		padding: 4px 10px 10px;
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-muted);
	}

	.toc {
		display: flex;
		gap: 10px;
		padding: 9px 10px;
		border-radius: var(--radius-control);
		font-size: 15px;
		font-weight: 500;
	}

	.toc.current {
		background: var(--color-field-tint);
		font-weight: 700;
	}

	.toc-n {
		width: 26px;
		flex: none;
		font-family: var(--font-slab);
		font-weight: 700;
		color: var(--color-field);
	}

	.articles {
		flex: 999 1 600px;
		min-width: 0;
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.articles :global(.article) {
		scroll-margin-top: 16px;
	}

	.clauses {
		margin: 8px 0 0;
		padding: 0;
		list-style: none;
	}

	.aside {
		flex: 1 1 260px;
		min-width: 0;
		display: flex;
		flex-direction: column;
		gap: 16px;
	}

	.need {
		padding: 9px 0;
		border-bottom: 1px solid var(--color-pennant-line);
	}

	.need-what {
		font-size: 14px;
		color: var(--color-ink-2);
	}

	.need-vote {
		margin-top: 2px;
		font-size: 15px;
		font-weight: 700;
	}

	.source {
		margin-top: 10px;
		font-size: 13px;
		color: var(--color-ink-brown);
	}

	.version {
		display: flex;
		justify-content: space-between;
		padding: 8px 0;
		font-size: 15px;
	}

	.date {
		color: var(--color-muted);
		white-space: nowrap;
	}

	.versions-note {
		margin: 6px 0 0;
		font-size: 14px;
		line-height: 1.45;
		color: var(--color-muted);
	}

	.empty {
		margin: 0;
		font-size: 16px;
	}

	.muted {
		color: var(--color-muted);
	}

	code {
		font-family: var(--font-mono);
		font-size: 14px;
	}

	/* Propose mode */
	.propose {
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.propose :global(.accordion) {
		scroll-margin-top: 16px;
	}

	.acc-title {
		margin: 0;
	}

	.acc-head {
		display: flex;
		align-items: center;
		gap: 12px;
		width: 100%;
		padding: 18px 24px;
		border: 0;
		background: transparent;
		color: var(--color-ink);
		font: inherit;
		text-align: left;
		cursor: pointer;
	}

	.acc-head.open {
		border-bottom: 2px solid var(--color-ink);
	}

	.acc-head:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: -2px;
	}

	.acc-name {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 26px;
	}

	.numeral {
		color: var(--color-field);
	}

	/* The board's arrow sits at a button's default 13.33px. */
	.arrow {
		margin-left: auto;
		font-size: 13.333px;
		color: var(--color-muted);
	}

	.propose-list {
		margin: 0;
		padding: 8px 24px 12px;
	}

	.new-clause {
		padding: 0 24px 18px;
	}

	/* Amendment history and past proposals */
	.history {
		margin-top: 24px;
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
	}

	.history :global(.history-card) {
		flex: 1 1 340px;
	}

	.amendments {
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.amendments li {
		padding: 10px 0;
	}

	.amendments li + li {
		border-top: 1px solid var(--color-line-soft);
	}

	.amend-title {
		font-size: 15px;
		font-weight: 600;
	}

	.year {
		font-weight: 400;
		color: var(--color-muted);
	}

	.amend-desc {
		margin: 2px 0 0;
		font-size: 14px;
		color: var(--color-ink-2);
	}

	.amend-tally {
		margin: 4px 0 0;
		font-size: 13px;
		color: var(--color-muted);
	}

	@media (max-width: 639.98px) {
		.strip {
			padding: 12px 14px;
			line-height: 1.45;
		}

		.proposals h2 {
			font-size: 22px;
		}

		.body {
			margin-top: 24px;
		}

		.reading,
		.propose {
			flex-direction: column;
			align-items: stretch;
			gap: 16px;
		}

		.reading :global(.toc-card) {
			flex: none;
		}

		.toc {
			align-items: center;
			min-height: 44px;
			padding: 0 10px;
		}

		.articles {
			flex: none;
			gap: 16px;
		}

		.aside {
			flex: none;
		}

		.version {
			gap: 12px;
		}

		.acc-head {
			min-height: 60px;
			gap: 10px;
			padding: 14px 16px;
		}

		.acc-name {
			font-size: 22px;
		}

		.propose-list {
			padding: 8px 16px 12px;
		}

		.new-clause {
			padding: 0 16px 16px;
		}

		.history {
			flex-direction: column;
			gap: 16px;
		}

		.history :global(.history-card) {
			flex: none;
		}
	}
</style>

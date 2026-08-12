<script lang="ts">
	import {
		BookOpen,
		Calendar,
		ChevronDown,
		ChevronRight,
		Edit3,
		FileText,
		Plus,
		Target,
		Trophy,
		Users,
		Vote,
		Zap
	} from 'lucide-svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import ClauseView from '$lib/components/constitution/ClauseView.svelte';
	import ProposalCard from '$lib/components/constitution/ProposalCard.svelte';
	import ProposalForm from '$lib/components/constitution/ProposalForm.svelte';
	import type { ActionData, PageData } from './$types';

	export let data: PageData;
	export let form: ActionData;

	// Icon names are stored as strings in the database and resolved here, so the
	// seed data stays plain JSON with no component references in it.
	const ICONS: Record<string, any> = {
		BookOpen, Calendar, FileText, Target, Trophy, Users, Zap
	};

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
	// viewer's zone slides a midnight date back a day — "January 2025" became
	// "December 2024" for anyone west of Greenwich.
	$: lastUpdated = data.version
		? new Date(data.version.effectiveAt).toLocaleDateString('en-US', {
				month: 'long',
				year: 'numeric',
				timeZone: 'UTC'
			})
		: 'not yet seeded';

	function proposalsFor(sectionId: string) {
		return openProposals.filter((p) => p.affectedSection === sectionId);
	}

	function startProposal(input: typeof draft) {
		draft = input;
	}
</script>

<svelte:head>
	<title>Constitution | The League</title>
</svelte:head>

<!-- No inner container: the root layout already supplies the page gutter and max
     width, so wrapping again double-padded this page relative to every other. -->
<div class="space-y-8">
	<PageHeader icon={BookOpen} title="League Constitution" accent="amber">
		The rules this league runs on. Every clause is versioned, and changes go to a vote.
	</PageHeader>

	<!-- Body keeps a reading width; the header spans the page like everywhere else. -->
	<div class="mx-auto max-w-4xl space-y-8">
	<header class="text-center">
		<div class="flex flex-wrap items-center justify-center gap-4 text-sm">
			<span class="flex items-center gap-2 text-slate-400">
				<FileText class="h-4 w-4" />
				{#if data.version}
					Version {data.version.versionNo} · updated {lastUpdated}
				{:else}
					Not yet seeded
				{/if}
			</span>

			{#if data.canPropose}
				<button
					class="flex items-center gap-2 rounded-lg bg-amber-500 px-4 py-2 font-semibold text-slate-900 transition-colors hover:bg-amber-400"
					on:click={() => (editMode = !editMode)}
				>
					<Edit3 class="h-4 w-4" />
					{editMode ? 'Done proposing' : 'Propose a change'}
				</button>
			{:else}
				<a href="/login?redirect=/constitution" class="text-amber-400 underline hover:text-amber-300">
					Sign in to propose changes
				</a>
			{/if}
		</div>
		{#if editMode}
			<p class="mt-3 text-sm text-amber-400">
				Hover any clause to edit it, add a sub-clause, or propose removing it.
			</p>
		{/if}
	</header>

	{#if form?.error}
		<p class="rounded-lg border border-red-600/40 bg-red-900/20 p-3 text-sm text-red-300">
			{form.error}
		</p>
	{/if}

	{#if myDrafts.length > 0}
		<section class="space-y-3 rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
			<h2 class="flex items-center gap-2 font-semibold text-slate-300">
				<FileText class="h-5 w-5" /> Your drafts ({myDrafts.length})
			</h2>
			{#each myDrafts as proposal (proposal.proposalKey)}
				<ProposalCard {proposal} />
			{/each}
		</section>
	{/if}

	{#if openProposals.length > 0}
		<section class="space-y-3 rounded-xl border border-amber-600/30 bg-amber-900/10 p-5">
			<h2 class="flex items-center gap-2 font-semibold text-amber-400">
				<Vote class="h-5 w-5" /> Open for voting ({openProposals.length})
			</h2>
			{#each openProposals as proposal (proposal.proposalKey)}
				<ProposalCard {proposal} />
			{/each}
		</section>
	{/if}

	{#if data.sections.length === 0}
		<p class="rounded-xl border border-slate-700 bg-slate-800/50 p-6 text-center text-slate-400">
			The constitution hasn't been seeded yet. Run <code class="text-amber-400">npm run seed:constitution</code>.
		</p>
	{/if}

	{#each data.sections as section (section.sectionKey)}
		{@const Icon = ICONS[section.icon ?? 'FileText'] ?? FileText}
		{@const pending = proposalsFor(section.sectionId)}
		<section class="overflow-hidden rounded-xl border border-slate-700 bg-slate-800/50">
			<button
				class="flex w-full items-center justify-between p-5 text-left transition-colors hover:bg-slate-800"
				on:click={() => (expanded[section.sectionId] = !expanded[section.sectionId])}
			>
				<span class="flex items-center gap-3">
					<Icon class="h-5 w-5 text-amber-400" />
					<span class="font-semibold text-white">{section.title}</span>
					{#if pending.length > 0}
						<span class="rounded-full bg-amber-900/40 px-2 py-0.5 text-xs text-amber-300">
							{pending.length} proposed
						</span>
					{/if}
				</span>
				{#if expanded[section.sectionId]}
					<ChevronDown class="h-5 w-5 text-slate-400" />
				{:else}
					<ChevronRight class="h-5 w-5 text-slate-400" />
				{/if}
			</button>

			{#if expanded[section.sectionId]}
				<div class="space-y-1 border-t border-slate-700 px-5 py-4">
					{#each section.clauses as clause (clause.clauseKey)}
						<ClauseView
							{clause}
							sectionId={section.sectionId}
							{editMode}
							onPropose={startProposal}
						/>
					{/each}

					{#if editMode}
						<button
							class="mt-3 flex items-center gap-1.5 text-sm text-amber-400 hover:text-amber-300"
							on:click={() =>
								startProposal({
									type: 'add_clause',
									sectionId: section.sectionId,
									clauseUid: null,
									currentText: ''
								})}
						>
							<Plus class="h-4 w-4" /> Propose a new clause in this article
						</button>
					{/if}
				</div>
			{/if}
		</section>
	{/each}

	{#if data.amendments.length > 0}
		<section class="rounded-xl border border-slate-700 bg-slate-800/50 p-5">
			<h2 class="mb-4 flex items-center gap-2 font-semibold text-white">
				<Calendar class="h-5 w-5 text-amber-400" /> Amendment history
			</h2>
			<ul class="space-y-3">
				{#each data.amendments as amendment (amendment.amendmentKey)}
					<li class="border-l-2 border-amber-500/50 pl-4">
						<div class="flex items-baseline gap-2">
							<span class="font-medium text-white">{amendment.title}</span>
							<span class="text-xs text-slate-500">{amendment.year}</span>
						</div>
						<p class="text-sm text-slate-400">{amendment.description}</p>
						{#if amendment.voteResults}
							<p class="mt-0.5 text-xs text-slate-500">
								Passed {amendment.voteResults.yes}–{amendment.voteResults.no}
								{#if amendment.voteResults.abstain}
									({amendment.voteResults.abstain} abstained)
								{/if}
								· effective {amendment.effectiveSeason}
							</p>
						{/if}
					</li>
				{/each}
			</ul>
		</section>
	{/if}

	{#if settled.length > 0}
		<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
			<h2 class="mb-3 font-semibold text-slate-400">Past proposals</h2>
			<div class="space-y-3">
				{#each settled as proposal (proposal.proposalKey)}
					<ProposalCard {proposal} />
				{/each}
			</div>
		</section>
	{/if}
	</div>
</div>

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

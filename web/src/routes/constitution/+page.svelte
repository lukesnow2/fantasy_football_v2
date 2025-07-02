<script lang="ts">
	import { 
		BookOpen, 
		FileText, 
		Users, 
		Trophy, 
		Calendar, 
		Target, 
		Zap, 
		ChevronDown, 
		ChevronRight,
		Edit3,
		Plus,
		Vote,
		MessageSquare,
		X,
		Check,
		Clock
	} from 'lucide-svelte';
	import { onMount } from 'svelte';

	// Editing state
	let editMode = false;
	let editingRule: { sectionId: string; ruleIndex: number } | null = null;
	let proposalText = '';
	let proposalType: 'edit' | 'add' = 'edit';
	let proposalRationale = '';
	let effectiveSeason = new Date().getFullYear() + 1;
	let showProposalForm = false;
	let pendingProposals: any[] = [];

	// Table of contents
	const tableOfContents = [
		{ id: 'article1', title: 'Article 1: League Makeup and Construction', page: 2 },
		{ id: 'article2', title: 'Article 2: Trades', page: 3 },
		{ id: 'article3', title: 'Article 3: Waiver wire', page: 3 },
		{ id: 'article4', title: 'Article 4: The Draft', page: 3 },
		{ id: 'article5', title: 'Article 5: Commissionership', page: 4 },
		{ id: 'article6', title: 'Article 6: Collusion', page: 4 },
		{ id: 'article7', title: 'Article 7: Line Up rules', page: 5 },
		{ id: 'article8', title: 'Article 8: Amendments to the Constitution', page: 5 },
		{ id: 'article9', title: 'Article 9: Miscellaneous rules', page: 5 },
		{ id: 'appendix1', title: 'Appendix 1', page: 7 }
	];

	// Track expanded sections
	let expandedSections: Record<string, boolean> = {};
	
	// Toggle section expansion
	function toggleSection(sectionId: string) {
		expandedSections[sectionId] = !expandedSections[sectionId];
		expandedSections = expandedSections; // Trigger reactivity
	}

	// Toggle edit mode
	function toggleEditMode() {
		editMode = !editMode;
		if (!editMode) {
			cancelEdit();
		}
	}

	// Start editing a specific rule
	function startEdit(sectionId: string, ruleIndex: number, currentText: string) {
		editingRule = { sectionId, ruleIndex };
		proposalText = currentText;
		proposalType = 'edit';
		showProposalForm = true;
	}

	// Start adding a new rule to a section
	function startAdd(sectionId: string) {
		editingRule = { sectionId, ruleIndex: -1 };
		proposalText = '';
		proposalType = 'add';
		showProposalForm = true;
	}

	// Cancel editing
	function cancelEdit() {
		editingRule = null;
		proposalText = '';
		proposalRationale = '';
		showProposalForm = false;
	}

	// Submit proposal
	async function submitProposal() {
		if (!editingRule || !proposalText.trim() || !proposalRationale.trim()) {
			return;
		}

		const section = constitutionSections.find(s => s.id === editingRule!.sectionId);
		if (!section) return;

		const proposal = {
			id: `proposal-${Date.now()}`,
			sectionId: editingRule.sectionId,
			sectionTitle: section.title,
			ruleIndex: editingRule.ruleIndex,
			type: proposalType,
			currentText: proposalType === 'edit' ? section.content[editingRule.ruleIndex] : null,
			proposedText: proposalText,
			rationale: proposalRationale,
			effectiveSeason,
			submittedBy: 'Current User', // TODO: Get from auth
			submittedAt: new Date().toISOString(),
			status: 'pending',
			votes: { yes: 0, no: 0, abstain: 0 },
			comments: []
		};

		// Add to pending proposals
		pendingProposals = [...pendingProposals, proposal];

		// Store in localStorage for demo
		localStorage.setItem('constitution-proposals', JSON.stringify(pendingProposals));

		cancelEdit();
	}

	// Load pending proposals
	onMount(() => {
		const saved = localStorage.getItem('constitution-proposals');
		if (saved) {
			pendingProposals = JSON.parse(saved);
		}
	});

	// Vote on proposal
	function voteOnProposal(proposalId: string, vote: 'yes' | 'no' | 'abstain') {
		pendingProposals = pendingProposals.map(p => {
			if (p.id === proposalId) {
				return {
					...p,
					votes: {
						...p.votes,
						[vote]: p.votes[vote] + 1
					}
				};
			}
			return p;
		});
		localStorage.setItem('constitution-proposals', JSON.stringify(pendingProposals));
	}

	// Get proposals for a specific rule
	function getProposalsForRule(sectionId: string, ruleIndex: number) {
		return pendingProposals.filter(p => 
			p.sectionId === sectionId && 
			p.ruleIndex === ruleIndex && 
			p.status === 'pending'
		);
	}

	// Get proposals for adding to a section
	function getAddProposalsForSection(sectionId: string) {
		return pendingProposals.filter(p => 
			p.sectionId === sectionId && 
			p.type === 'add' && 
			p.status === 'pending'
		);
	}

	// Constitution sections with actual content
	const constitutionSections = [
		{
			id: 'article1',
			title: "Article 1: League Makeup and Construction",
			icon: Trophy,
			content: [
				"10 team league, no divisions",
				"League buy-in: $100",
				"Payout: 60% First, 30% Second, 10% Third",
				"Roster: QB, WR, WR, RB, RB, TE, W/R/T, K, DEF, IR, IR",
				"Playoffs: Weeks 14-16, 6 teams, top 2 get byes",
				"New owners require 2/3 approval (minimum 5 votes)",
				"Co-managing requires 50% approval before draft",
				"The Snow Rule: 10th place team can leave for one season or negotiate co-management"
			]
		},
		{
			id: 'article2',
			title: "Article 2: Trades",
			icon: Target,
			content: [
				"The Rowley Rule: Only players for players - no cash/favors",
				"Penalties: Loss of 3rd round pick to expulsion",
				"Trade vetoes under commissioner's jurisdiction",
				"Appeals: 24 hours in League Lounge with logical reasoning",
				"Appeal success: Majority vote within 48 hours",
				"Trade deadline: After Week 10"
			]
		},
		{
			id: 'article3',
			title: "Article 3: Waiver wire",
			icon: Zap,
			content: [
				"FAAB system: $100 budget per player",
				"Tiebreakers: Waiver priority (opposite of draft order)",
				"Free agents go on waivers at game time",
				"Follows normal waiver wire rules"
			]
		},
		{
			id: 'article4',
			title: "Article 4: The Draft",
			icon: Calendar,
			content: [
				"Date: Weekend before Labor Day weekend",
				"Draft city: Selected by league champion",
				"15-round snake draft, 90 seconds per pick",
				"Draft order: Random selection by independent third party",
				"No trading draft picks or pick order",
				"Late arrivals: Picks skipped, added to supplemental round",
				"Proxy drafting allowed with commissioner approval"
			]
		},
		{
			id: 'article5',
			title: "Article 5: Commissionership",
			icon: Users,
			content: [
				"Two-year term starting 2012 season",
				"Election: Simple majority or plurality",
				"Unopposed incumbent: Must receive one non-self vote",
				"Impeachment: Super-majority (7 votes)"
			]
		},
		{
			id: 'article6',
			title: "Article 6: Collusion",
			icon: FileText,
			content: [
				"Collusion defined as cooperative action by at least two managers",
				"Collusion will not be tolerated",
				"Penalties: 2nd round pick to exclusion from the league",
				"Commissioner's discretion on penalties",
				"If commissioner accused: League votes on actions"
			]
		},
		{
			id: 'article7',
			title: "Article 7: Line Up rules",
			icon: FileText,
			content: [
				"No OUT, BYE, or empty spots in lineup",
				"Penalty: $20 per infraction",
				"24-hour grace period before game",
				"Revenue used for draft expense aid"
			]
		},
		{
			id: 'article8',
			title: "Article 8: Amendments to the Constitution",
			icon: FileText,
			content: [
				"Changing scoring or roster size: Simple majority",
				"Any other changes: Super-majority",
				"Changes to league size: Unanimous vote",
				"Removing managers for non-rule violations: Unanimous vote minus the manager"
			]
		},
		{
			id: 'article9',
			title: "Article 9: Miscellaneous rules",
			icon: FileText,
			content: [
				"Draft tradition: All members take whiskey shot (paid by Snow Bowl Loser)",
				"Last Place Trophy: Must be posted to social media",
				"Champion photo: Posted as League Lounge banner",
				"All members supply alcohol for draft and league functions",
				"Kicker record bonus: 50 points for longest kick record"
			]
		}
	];

	const scoringSystem = [
		{ category: "Passing", rules: [
			"Yards: 20 yards per point; 1pt at 150yds, 2pts at 250yds, 3pts at 350yds",
			"Touchdowns: 4 points",
			"Interceptions: -2 points"
		]},
		{ category: "Rushing", rules: [
			"Yards: 10 yards per point; 1pt at 100yds, 2pts at 200yds, 3pts at 300yds",
			"Touchdowns: 6 points"
		]},
		{ category: "Receiving", rules: [
			"Receptions: 1 point (PPR)",
			"Yards: 10 yards per point; 1pt at 100yds, 2pts at 200yds, 3pts at 250yds",
			"Touchdowns: 6 points"
		]},
		{ category: "Kicking", rules: [
			"Field Goals: 3 points (0-49 yards), 4 points (50+)",
			"Missed FGs: -2 (0-19), -1 (20-49), 0 (50+)",
			"Extra Points: 1 point, -2 if missed"
		]},
		{ category: "Defense", rules: [
			"Sacks: 1 point, Interceptions: 2 points, Fumble Recovery: 2 points",
			"Touchdowns: 6 points, Safety: 4 points, Block Kick: 2 points",
			"Points Allowed: 12 (0), 9 (1-6), 5 (7-13), 2 (14-20), 0 (21-27), -1 (28-34), -4 (35+)"
		]}
	];

	const specialRules = [
		"The Snow Rule: 10th place team can leave for one season or negotiate co-management",
		"Collusion penalties: 2nd round pick to expulsion (commissioner's discretion)",
		"Constitution changes: Simple majority for scoring/roster, super-majority for others",
		"League size changes: Unanimous vote required",
		"Draft tradition: All members take whiskey shot (paid by Snow Bowl Loser)",
		"Last Place Trophy: Must be posted to social media",
		"Champion photo: Posted as League Lounge banner",
		"Kicker record bonus: 50 points for longest kick record"
	];

	const seasonOptions = Array.from({ length: 5 }, (_, i) => new Date().getFullYear() + i + 1);
</script>

<div class="max-w-6xl mx-auto px-4 py-8 space-y-8">
	<!-- Header with Edit Toggle -->
	<div class="text-center space-y-4">
		<BookOpen class="h-16 w-16 text-amber-400 mx-auto" />
		<h1 class="text-4xl font-bold text-white">League Constitution</h1>
		<p class="text-xl text-slate-300 max-w-3xl mx-auto">
			The official rules, bylaws, and traditions that govern our fantasy football dynasty. 
			{#if editMode}
				<span class="text-amber-400">Click on any rule to propose changes.</span>
			{:else}
				This document ensures fair play and competitive balance across all seasons.
			{/if}
		</p>
		<div class="flex items-center justify-center space-x-4">
			<div class="flex items-center space-x-2 text-slate-400">
				<FileText class="h-5 w-5" />
				<span>Last Updated: January 2025</span>
			</div>
			<button 
				class="flex items-center space-x-2 px-4 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg font-semibold transition-colors"
				on:click={toggleEditMode}
			>
				<Edit3 class="h-4 w-4" />
				<span>{editMode ? 'Exit Proposal Mode' : 'Propose Changes'}</span>
			</button>
		</div>
	</div>

	<!-- Pending Proposals Summary -->
	{#if pendingProposals.length > 0}
		<div class="bg-amber-900/20 border border-amber-600/30 rounded-xl p-6">
			<h3 class="text-lg font-semibold text-amber-400 mb-4 flex items-center space-x-2">
				<Vote class="h-5 w-5" />
				<span>Pending Rule Proposals ({pendingProposals.length})</span>
			</h3>
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each pendingProposals as proposal}
					<div class="bg-slate-800/50 border border-slate-600 rounded-lg p-3">
						<div class="text-sm text-slate-400 mb-1">{proposal.sectionTitle}</div>
						<div class="text-white font-medium mb-2">
							{proposal.type === 'add' ? 'Add New Rule' : 'Edit Rule'}
						</div>
						<div class="text-sm text-slate-300 mb-3 line-clamp-2">{proposal.proposedText}</div>
						<div class="flex items-center justify-between text-xs">
							<span class="text-slate-400">Effective {proposal.effectiveSeason}</span>
							<div class="flex items-center space-x-2">
								<span class="text-green-400">{proposal.votes.yes}✓</span>
								<span class="text-red-400">{proposal.votes.no}✗</span>
								<span class="text-slate-400">{proposal.votes.abstain}~</span>
							</div>
						</div>
					</div>
				{/each}
			</div>
		</div>
	{/if}

	<!-- Constitution Sections -->
	<div class="space-y-4">
		{#each constitutionSections as section}
			<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700/50 overflow-hidden">
				<button 
					class="w-full p-6 flex items-center justify-between hover:bg-slate-700/30 transition-colors"
					on:click={() => toggleSection(section.id)}
				>
					<div class="flex items-center space-x-3">
						<svelte:component this={section.icon} class="h-6 w-6 text-amber-400" />
						<h2 class="text-xl font-bold text-white">{section.title}</h2>
					</div>
					<div class="flex items-center space-x-2">
						{#if editMode}
							<button 
								class="p-2 bg-amber-500/20 hover:bg-amber-500/30 text-amber-400 rounded-lg transition-colors"
								on:click|stopPropagation={() => startAdd(section.id)}
							>
								<Plus class="h-4 w-4" />
							</button>
						{/if}
						<svelte:component 
							this={expandedSections[section.id] ? ChevronDown : ChevronRight} 
							class="h-5 w-5 text-slate-400" 
						/>
					</div>
				</button>
				
				{#if expandedSections[section.id]}
					<div class="px-6 pb-6 border-t border-slate-700/50">
						<ul class="space-y-3 mt-4">
							{#each section.content as rule, ruleIndex}
								<li class="text-slate-300 flex items-start space-x-3 group">
									<span class="text-amber-400 mt-0.5 flex-shrink-0">•</span>
									<div class="flex-grow">
										<div 
											class="leading-relaxed {editMode ? 'hover:bg-slate-700/30 p-2 rounded cursor-pointer' : ''} transition-colors relative"
											on:click={() => editMode && startEdit(section.id, ruleIndex, rule)}
										>
											{rule}
											{#if editMode}
												<Edit3 class="h-4 w-4 text-amber-400 opacity-0 group-hover:opacity-100 transition-opacity absolute right-2 top-2" />
											{/if}
										</div>
										
										<!-- Show proposals for this rule -->
										{#each getProposalsForRule(section.id, ruleIndex) as proposal}
											<div class="mt-3 bg-amber-900/10 border border-amber-600/20 rounded-lg p-3">
												<div class="flex items-center justify-between mb-2">
													<span class="text-xs text-amber-400 font-medium">PROPOSED CHANGE</span>
													<span class="text-xs text-slate-400">{proposal.submittedBy}</span>
												</div>
												<div class="text-sm text-amber-300 mb-2">{proposal.proposedText}</div>
												<div class="text-xs text-slate-400 mb-3">{proposal.rationale}</div>
												<div class="flex items-center justify-between">
													<div class="flex space-x-2">
														<button 
															class="px-2 py-1 bg-green-600/20 text-green-400 rounded text-xs hover:bg-green-600/30"
															on:click={() => voteOnProposal(proposal.id, 'yes')}
														>
															Yes ({proposal.votes.yes})
														</button>
														<button 
															class="px-2 py-1 bg-red-600/20 text-red-400 rounded text-xs hover:bg-red-600/30"
															on:click={() => voteOnProposal(proposal.id, 'no')}
														>
															No ({proposal.votes.no})
														</button>
														<button 
															class="px-2 py-1 bg-slate-600/20 text-slate-400 rounded text-xs hover:bg-slate-600/30"
															on:click={() => voteOnProposal(proposal.id, 'abstain')}
														>
															Abstain ({proposal.votes.abstain})
														</button>
													</div>
													<span class="text-xs text-slate-500">Effective {proposal.effectiveSeason}</span>
												</div>
											</div>
										{/each}
									</div>
								</li>
							{/each}
						</ul>
						
						<!-- Show add proposals for this section -->
						{#each getAddProposalsForSection(section.id) as proposal}
							<div class="mt-4 bg-blue-900/10 border border-blue-600/20 rounded-lg p-3">
								<div class="flex items-center justify-between mb-2">
									<span class="text-xs text-blue-400 font-medium">PROPOSED NEW RULE</span>
									<span class="text-xs text-slate-400">{proposal.submittedBy}</span>
								</div>
								<div class="text-sm text-blue-300 mb-2">• {proposal.proposedText}</div>
								<div class="text-xs text-slate-400 mb-3">{proposal.rationale}</div>
								<div class="flex items-center justify-between">
									<div class="flex space-x-2">
										<button 
											class="px-2 py-1 bg-green-600/20 text-green-400 rounded text-xs hover:bg-green-600/30"
											on:click={() => voteOnProposal(proposal.id, 'yes')}
										>
											Yes ({proposal.votes.yes})
										</button>
										<button 
											class="px-2 py-1 bg-red-600/20 text-red-400 rounded text-xs hover:bg-red-600/30"
											on:click={() => voteOnProposal(proposal.id, 'no')}
										>
											No ({proposal.votes.no})
										</button>
										<button 
											class="px-2 py-1 bg-slate-600/20 text-slate-400 rounded text-xs hover:bg-slate-600/30"
											on:click={() => voteOnProposal(proposal.id, 'abstain')}
										>
											Abstain ({proposal.votes.abstain})
										</button>
									</div>
									<span class="text-xs text-slate-500">Effective {proposal.effectiveSeason}</span>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		{/each}
	</div>

	<!-- Appendix 1: Scoring System -->
	<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700/50 overflow-hidden">
		<button 
			class="w-full p-6 flex items-center justify-between hover:bg-slate-700/30 transition-colors"
			on:click={() => toggleSection('appendix1')}
		>
			<div class="flex items-center space-x-3">
				<Target class="h-6 w-6 text-amber-400" />
				<h2 class="text-xl font-bold text-white">Appendix 1: Scoring System</h2>
			</div>
			<div class="flex items-center space-x-2">
				{#if editMode}
					<button 
						class="p-2 bg-amber-500/20 hover:bg-amber-500/30 text-amber-400 rounded-lg transition-colors"
						on:click|stopPropagation={() => startAdd('appendix1')}
					>
						<Plus class="h-4 w-4" />
					</button>
				{/if}
				<svelte:component 
					this={expandedSections['appendix1'] ? ChevronDown : ChevronRight} 
					class="h-5 w-5 text-slate-400" 
				/>
			</div>
		</button>
		
		{#if expandedSections['appendix1']}
			<div class="px-6 pb-6 border-t border-slate-700/50">
				<div class="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-4">
					{#each scoringSystem as category}
						<div class="space-y-3">
							<h3 class="text-lg font-semibold text-amber-400 border-b border-slate-600 pb-2">
								{category.category}
							</h3>
							<ul class="space-y-3">
								{#each category.rules as rule, ruleIndex}
									<li class="text-slate-300 flex items-start space-x-3 group">
										<span class="text-amber-400 mt-0.5 flex-shrink-0">•</span>
										<div 
											class="leading-relaxed {editMode ? 'hover:bg-slate-700/30 p-2 rounded cursor-pointer' : ''} transition-colors relative"
											on:click={() => editMode && startEdit('appendix1', ruleIndex, rule)}
										>
											{rule}
											{#if editMode}
												<Edit3 class="h-4 w-4 text-amber-400 opacity-0 group-hover:opacity-100 transition-opacity absolute right-2 top-2" />
											{/if}
										</div>
									</li>
								{/each}
							</ul>
						</div>
					{/each}
				</div>
			</div>
		{/if}
	</div>

	<!-- Amendment History -->
	<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700/50 overflow-hidden">
		<button 
			class="w-full p-6 flex items-center justify-between hover:bg-slate-700/30 transition-colors"
			on:click={() => toggleSection('amendments')}
		>
			<div class="flex items-center space-x-3">
				<FileText class="h-6 w-6 text-amber-400" />
				<h2 class="text-xl font-bold text-white">Amendment History</h2>
			</div>
			<svelte:component 
				this={expandedSections['amendments'] ? ChevronDown : ChevronRight} 
				class="h-5 w-5 text-slate-400" 
			/>
		</button>
		
		{#if expandedSections['amendments']}
			<div class="px-6 pb-6 border-t border-slate-700/50">
				<div class="space-y-4 mt-4">
					<div class="border-l-4 border-amber-400 pl-4">
						<div class="flex justify-between items-start">
							<h3 class="font-semibold text-white">2012</h3>
							<span class="text-slate-400 text-sm">Original Constitution</span>
						</div>
						<p class="text-slate-300 mt-1">League constitution established with 10-team format</p>
					</div>
					<div class="border-l-4 border-slate-600 pl-4">
						<div class="flex justify-between items-start">
							<h3 class="font-semibold text-white">2013</h3>
							<span class="text-slate-400 text-sm">Snow Rule Added</span>
						</div>
						<p class="text-slate-300 mt-1">Relegation policy implemented for charter member return</p>
					</div>
					<div class="border-l-4 border-slate-600 pl-4">
						<div class="flex justify-between items-start">
							<h3 class="font-semibold text-white">2023</h3>
							<span class="text-slate-400 text-sm">Draft Cities Updated</span>
						</div>
						<p class="text-slate-300 mt-1">Draft city list updated: Denver, Portland, Seattle, Boise, Tahoe</p>
					</div>
				</div>
			</div>
		{/if}
	</div>
</div>

<!-- Proposal Form Modal -->
{#if showProposalForm}
	<div class="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center p-4 z-50">
		<div class="bg-slate-800 rounded-xl border border-slate-700 p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
			<div class="flex items-center justify-between mb-6">
				<h3 class="text-xl font-bold text-white">
					{proposalType === 'add' ? 'Propose New Rule' : 'Propose Rule Change'}
				</h3>
				<button 
					class="text-slate-400 hover:text-white transition-colors"
					on:click={cancelEdit}
				>
					<X class="h-6 w-6" />
				</button>
			</div>
			
			<div class="space-y-4">
				{#if proposalType === 'edit' && editingRule}
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Current Rule</label>
						<div class="bg-slate-900/50 border border-slate-600 rounded-lg p-3 text-slate-300 text-sm">
							{constitutionSections.find(s => s.id === editingRule.sectionId)?.content[editingRule.ruleIndex]}
						</div>
					</div>
				{/if}
				
				<div>
					<label class="block text-sm font-medium text-slate-300 mb-2">
						{proposalType === 'add' ? 'New Rule Text' : 'Proposed Changes'}
					</label>
					<textarea 
						bind:value={proposalText}
						rows="4"
						class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
						placeholder={proposalType === 'add' ? 'Enter the new rule...' : 'Enter your proposed changes...'}
					></textarea>
				</div>
				
				<div>
					<label class="block text-sm font-medium text-slate-300 mb-2">Rationale</label>
					<textarea 
						bind:value={proposalRationale}
						rows="3"
						class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
						placeholder="Why is this change needed?"
					></textarea>
				</div>
				
				<div>
					<label class="block text-sm font-medium text-slate-300 mb-2">Effective Season</label>
					<select 
						bind:value={effectiveSeason}
						class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
					>
						{#each seasonOptions as season}
							<option value={season}>{season}</option>
						{/each}
					</select>
				</div>
			</div>
			
			<div class="flex justify-end space-x-4 mt-6">
				<button 
					class="px-4 py-2 text-slate-300 hover:text-white transition-colors"
					on:click={cancelEdit}
				>
					Cancel
				</button>
				<button 
					class="px-6 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg font-semibold transition-colors flex items-center space-x-2"
					on:click={submitProposal}
					disabled={!proposalText.trim() || !proposalRationale.trim()}
				>
					<Vote class="h-4 w-4" />
					<span>Submit Proposal</span>
				</button>
			</div>
		</div>
	</div>
{/if} 
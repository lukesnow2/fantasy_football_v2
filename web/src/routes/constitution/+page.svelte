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
		Clock,
		Trash2
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
	let amendments: any[] = [];
	let loading = false;
	
	// Mock current user (in real app, this would come from auth)
	const currentUser = 1; // Manager key instead of name
	const currentUserId = 'user-1';

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

	// Load data from API
	async function loadData() {
		loading = true;
		try {
					// Load proposals
		const proposalsResponse = await fetch('/api/rule-proposals');
		const proposalsData = await proposalsResponse.json();
		const proposals = proposalsData.proposals || proposalsData; // Handle both formats
		pendingProposals = proposals.filter((p: any) => p.status === 'pending');
			
			// Load amendments
			const amendmentsResponse = await fetch('/api/rule-proposals?type=amendments');
			const amendmentsData = await amendmentsResponse.json();
			amendments = amendmentsData;
		} catch (error) {
			console.error('Error loading data:', error);
		} finally {
			loading = false;
		}
	}

	// Submit proposal
	async function submitProposal() {
		if (!editingRule || !proposalText.trim() || !proposalRationale.trim()) {
			return;
		}

		const section = constitutionSections.find(s => s.id === editingRule!.sectionId);
		if (!section) return;

		loading = true;
		try {
			const response = await fetch('/api/rule-proposals', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					action: 'create',
					type: proposalType,
					sectionId: editingRule.sectionId,
					sectionTitle: section.title,
					ruleIndex: editingRule.ruleIndex >= 0 ? editingRule.ruleIndex : undefined,
					originalText: proposalType === 'edit' ? section.content[editingRule.ruleIndex] : undefined,
					proposedText: proposalText,
					rationale: proposalRationale,
					effectiveSeason,
					submittedBy: currentUser
				})
			});

			if (response.ok) {
				await loadData(); // Refresh data
				cancelEdit();
			}
		} catch (error) {
			console.error('Error submitting proposal:', error);
		} finally {
			loading = false;
		}
	}

	// Vote on proposal
	async function voteOnProposal(proposalId: string, vote: 'yes' | 'no' | 'abstain') {
		loading = true;
		try {
			const response = await fetch('/api/rule-proposals', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					action: 'vote',
					proposalId,
					vote,
					voterId: currentUserId
				})
			});

			if (response.ok) {
				await loadData(); // Refresh data
			}
		} catch (error) {
			console.error('Error voting on proposal:', error);
		} finally {
			loading = false;
		}
	}

	// Delete proposal
	async function deleteProposal(proposalKey: number) {
		if (!confirm('Are you sure you want to delete this proposal? This action cannot be undone.')) {
			return;
		}

		try {
			const response = await fetch('/api/rule-proposals', {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					proposalKey
				})
			});

			if (response.ok) {
				await loadData(); // Refresh proposals
			}
		} catch (error) {
			console.error('Error deleting proposal:', error);
		}
	}

	// Load data on mount
	onMount(() => {
		loadData();
	});

	// Get proposals for a specific rule (show all edit proposals for this section)
	function getProposalsForRule(sectionId: string, ruleIndex: number) {
		return pendingProposals.filter(p => 
			p.sectionId === sectionId && 
			p.ruleIndex === ruleIndex &&
			p.type === 'edit' && 
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
				"I. 10 team",
				"II. No divisions", 
				"III. League buy-in is $100",
				"IV. Payout structure:",
				"    a. 60% First Place",
				"    b. 30% Second Place", 
				"    c. 10% Third Place",
				"V. Scoring system",
				"    a. See Appendix 1",
				"VI. Roster Spots",
				"    a. QB, WR, WR, RB, RB, TE, W/R/T, K, DEF, IR, IR",
				"VII. Adding owners to the league",
				"    a. Requires a 2/3 approval of new owner, with a minimum of 5 affirmative votes.",
				"    b. Co-managing is acceptable; however, the co-manager must be approved by 50% of the members before the first pick of the draft.",
				"    c. The Snow Rule: In order to reintroduce Israel Flores, charter member, into the 2013 Fantasy Season, the league will implement a relegation policy, where the 10th place team, as determined by the consolation bracket, will have the option to",
				"        i. Leave the league for one season",
				"        ii. Or negotiate a co-management with another manager",
				"        iii. If no managers can reach an agreement with the said 10th place member. The 9th place manager will be forced to co-manager with the 10th",
				"        iv. This system will be abolished as soon as it becomes no longer necessary.",
				"VIII. Playoffs go from week 14-16, 6 teams, with the first two seeds getting byes."
			]
		},
		{
			id: 'article2',
			title: "Article 2: Trades",
			icon: Target,
			content: [
				"I. The Rowley Rule: Trades cannot be completed for anything other than other players. Cash money, favors, or anything other than another legal NFL player cannot be accepted.",
				"    a. Penalties range from a loss of a third-round draft pick to expulsion from the league, at the commissioner's discretion.",
				"II. Trade vetoes will be under the commissioner's jurisdiction.",
				"    a. In any case, a member of the league can appeal an acceptance or rejection of a trade within 24 hours, in the League Lounge, with logical reasoning that refrains from appeals to emotion. The appeal will stand if a majority of the league members agree within 48 hours, with the voting pool excluding the commissioner and the trading parties.",
				"III. The trade deadline starts after week 10."
			]
		},
		{
			id: 'article3',
			title: "Article 3: Waiver wire",
			icon: Zap,
			content: [
				"I. Wavier wires follow a FAAB system with each player starting the year with a $100 budget.",
				"II. For tiebreakers, waiver priority is initially set as the opposite of the draft order and will determine who gets a player in the case of a tie in the bid.",
				"III. Free agent players will go on waivers at their respective game times and will follow normal waiver wire rules."
			]
		},
		{
			id: 'article4',
			title: "Article 4: The Draft",
			icon: Calendar,
			content: [
				"I. The draft will take place the weekend before Labor Day weekend.",
				"II. The champion of the league will select the draft city from the following list of cities that constitute home cities for the members of the league. As of 2023, this list is as follows: Denver, Portland, Seattle, Boise, and Tahoe.",
				"III. The draft will be a closed draft, only league members will be admitted. After the draft, there will be an open party with food and drink. There will be no breaks for official meals during the draft.",
				"IV. 15-round snake draft",
				"V. The draft order will be selected randomly with an independent third party selecting names out of a bowl. When a manager's name is selected he will then be eligible to select his pick in the draft.",
				"VI. No trading draft pick order or individual picks",
				"VII. 90 seconds per pick.",
				"VIII. For drafting a player that has already been drafted, the first offense will be met with a warning. On the second offense, the manager in question will lose one spot in the draft snake.",
				"IX. If a manager is late to the draft, their draft pick will be skipped over and added to the end of the draft in a supplemental round. This will occur until the manager arrives.",
				"    a. Exceptions can be made at the commissioner's discretion",
				"X. If a manager presents serious or compelling reason(s) to the commissioner to miss the draft, the commissioner's discretion may be used to accept this application. It is the responsibility of the absent manager to present a proxy to the commissioner and for the commissioner to accept the candidate."
			]
		},
		{
			id: 'article5',
			title: "Article 5: Commissionership",
			icon: Users,
			content: [
				"I. The commissioner will have a two-year term, which will start at the 2012 season. At the end of the two years, there will be an open election, with the winner being selected by a simple majority or a plurality. If the incumbent runs unopposed, they must receive one vote that is not their own.",
				"II. The commissioner can be impeached by a super-majority with seven votes voting to impeach the commissioner."
			]
		},
		{
			id: 'article6',
			title: "Article 6: Collusion",
			icon: FileText,
			content: [
				"I. Collusion is defined as cooperative action by at least two managers working to undermine the fair competitiveness of the league.",
				"II. Collusion will not be tolerated.",
				"    a. Penalties: 2nd round pick to exclusion from the league. Commissioner's discretion.",
				"III. If the commissioner is accused of collusion, the league will vote for the necessary actions. Simple majority for draft pick loss, a super-majority for expulsion."
			]
		},
		{
			id: 'article7',
			title: "Article 7: Line Up rules",
			icon: FileText,
			content: [
				"I. No players listed as out, on a bye, or an empty spot may be legally left in the lineup.",
				"    a. The player is penalized $20 per infraction with a 24-hour grace period before the game to accommodate for last-minute calls. The infraction will be removed from the winnings if the player placed, or added to the next year's buy-in if they did not. The revenue will be used for need-based draft expense aid."
			]
		},
		{
			id: 'article8',
			title: "Article 8: Amendments to the Constitution",
			icon: FileText,
			content: [
				"I. Changing scoring or roster size will be a simple majority",
				"II. Any other changes to the constitution will be a super-majority",
				"III. Changes to league size require a unanimous vote.",
				"IV. Removing managers for non-rule violations requires a unanimous vote minus the manager in question."
			]
		},
		{
			id: 'article9',
			title: "Article 9: Miscellaneous rules",
			icon: FileText,
			content: [
				"I. At the beginning of the draft, all members will take a shot of whiskey, to be paid for by Snow Bowl Loser. The League champion will select the whiskey of choice for the year, the cost of which cannot exceed $80.",
				"II. The Last Place Trophy will be shipped to the loser at the loser's expense and must be posted to Facebook (and all social media).",
				"III. The League Champion will take a photo with the trophy and submit it to the commissioner to be posted as the League Lounge banner photo.",
				"IV. All members of the League must help supply the alcohol, bringing their favorite six-pack of beer to the draft and all other officially sanctioned league functions unless otherwise recommended by the commissioner or host.",
				"V. If a kicker sets the record for longest kick, the team with that kicker gets a 50 point bonus."
			]
		}
	];

	const scoringSystem = [
		{ category: "Passing", rules: [
			"Passing Yards: 20 yards per point; 1 point at 150 yards; 2 points at 250 yards; 3 points at 350 yards",
			"Passing Touchdowns: 4",
			"Interceptions: -2"
		]},
		{ category: "Rushing", rules: [
			"Rushing Yards: 10 yards per point; 1 point at 100 yards; 2 points at 200 yards; 3 points at 300 yards",
			"Rushing Touchdowns: 6"
		]},
		{ category: "Receiving", rules: [
			"Receptions: 1",
			"Reception Yards: 10 yards per point; 1 point at 100 yards; 2 points at 200 yards; 3 points at 250 yards",
			"Reception Touchdowns: 6"
		]},
		{ category: "Other Offense", rules: [
			"Return Touchdowns: 6",
			"2-Point Conversions: 2",
			"Fumbles Lost: -2",
			"Offensive Fumble Return TD: 6"
		]},
		{ category: "Kicking", rules: [
			"Field Goals 0-19 Yards: 3",
			"Field Goals 20-29 Yards: 3",
			"Field Goals 30-39 Yards: 3",
			"Field Goals 40-49 Yards: 3",
			"Field Goals 50+ Yards: 4",
			"Field Goals Missed 0-19 Yards: -2",
			"Field Goals Missed 20-29 Yards: -1",
			"Field Goals Missed 30-39 Yards: -1",
			"Field Goals Missed 40-49 Yards: -1",
			"Field Goals Missed 50+ Yards: 0",
			"Point After Attempt Made: 1",
			"Point After Attempt Missed: -2"
		]},
		{ category: "Defense", rules: [
			"Sack: 1",
			"Interception: 2",
			"Fumble Recovery: 2",
			"Touchdown: 6",
			"Safety: 4",
			"Block Kick: 2",
			"Kickoff and Punt Return Touchdowns: 6",
			"Points Allowed 0 points: 12",
			"Points Allowed 1-6 points: 9",
			"Points Allowed 7-13 points: 5",
			"Points Allowed 14-20 points: 2",
			"Points Allowed 21-27 points: 0",
			"Points Allowed 28-34 points: -1",
			"Points Allowed 35+ points: -4"
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
			<div class="text-sm text-amber-300 mb-4">
				<strong>Vote Requirements:</strong> Simple majority (6 votes) for scoring/roster changes, 
				Super-majority (7 votes) for constitutional changes, Unanimous (10 votes) for league size changes
			</div>
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each pendingProposals as proposal}
					<div class="bg-slate-800/50 border border-slate-600 rounded-lg p-3 relative">
						<button 
							class="absolute top-2 right-2 p-1 rounded text-red-300 bg-red-900/30 hover:text-red-200 hover:bg-red-900/50 transition-colors"
							on:click={() => deleteProposal(proposal.proposalKey)}
							title="Delete proposal"
						>
							<Trash2 class="h-4 w-4" />
						</button>
						<div class="text-sm text-slate-400 mb-1 pr-8">{proposal.affectedSection || proposal.sectionId}</div>
						<div class="text-white font-medium mb-2">
							{proposal.type === 'add' ? 'Add New Rule' : 'Edit Rule'}
						</div>
						<div class="text-sm text-slate-300 mb-3 line-clamp-2">{proposal.proposedText}</div>
						<div class="flex items-center justify-between text-xs">
							<span class="text-slate-400">Effective {proposal.effectiveSeason}</span>
							<div class="flex items-center space-x-2">
								<span class="text-green-400">{proposal.yesVotes}✓</span>
								<span class="text-red-400">{proposal.noVotes}✗</span>
								<span class="text-slate-400">{proposal.abstainVotes}~</span>
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
						<div class="space-y-2 mt-4">
							{#each section.content as rule, ruleIndex}
								{@const isMainItem = rule.match(/^[IVX]+\./)}
								{@const isSubItem = rule.match(/^\s+[a-z]\./)}
								{@const isSubSubItem = rule.match(/^\s+[ivx]+\./)}
								{@const indentLevel = isSubSubItem ? 'ml-16' : isSubItem ? 'ml-8' : ''}
								
								<div class="text-slate-300 group {indentLevel}">
									<div 
										class="leading-relaxed {editMode ? 'hover:bg-slate-700/30 p-2 rounded cursor-pointer' : ''} transition-colors relative"
										on:click={() => editMode && startEdit(section.id, ruleIndex, rule)}
									>
										{#if isMainItem}
											<span class="text-amber-400 font-semibold">{isMainItem[0]}</span>
											<span class="ml-2">{rule.replace(/^[IVX]+\.\s*/, '')}</span>
										{:else if isSubItem}
											{@const subMatch = rule.trim().match(/^[a-z]\./)}
											<span class="text-amber-400 font-semibold">{subMatch?.[0] ?? ''}</span>
											<span class="ml-2">{rule.trim().replace(/^[a-z]\.\s*/, '')}</span>
										{:else if isSubSubItem}
											{@const subSubMatch = rule.trim().match(/^[ivx]+\./)}
											<span class="text-amber-400 font-semibold">{subSubMatch?.[0] ?? ''}</span>
											<span class="ml-2">{rule.trim().replace(/^[ivx]+\.\s*/, '')}</span>
										{:else}
											{rule}
										{/if}
										{#if editMode}
											<Edit3 class="h-4 w-4 text-amber-400 opacity-0 group-hover:opacity-100 transition-opacity absolute right-2 top-2" />
										{/if}
									</div>
									
									<!-- Show proposals for this rule -->
									{#each getProposalsForRule(section.id, ruleIndex) as proposal}
										<div class="mt-3 bg-amber-900/10 border border-amber-600/20 rounded-lg p-3 relative">
											<button 
												class="absolute top-2 right-2 p-1 rounded text-red-300 bg-red-900/30 hover:text-red-200 hover:bg-red-900/50 transition-colors"
												on:click={() => deleteProposal(proposal.proposalKey)}
												title="Delete proposal"
											>
												<Trash2 class="h-3 w-3" />
											</button>
											<div class="flex items-center justify-between mb-2 pr-8">
												<span class="text-xs text-amber-400 font-medium">PROPOSED CHANGE</span>
												<span class="text-xs text-slate-400">{proposal.submittedByManager || 'Manager ' + proposal.submittedBy}</span>
											</div>
											<div class="text-sm text-amber-300 mb-2">{proposal.proposedText}</div>
											<div class="text-xs text-slate-400 mb-3">{proposal.rationale}</div>
											<div class="flex items-center justify-between">
												<div class="flex space-x-2">
													<button 
														class="px-2 py-1 bg-green-600/20 text-green-400 rounded text-xs hover:bg-green-600/30 disabled:opacity-50"
														on:click={() => voteOnProposal(proposal.proposalKey, 'yes')}
														disabled={loading}
													>
														Yes ({proposal.yesVotes})
													</button>
													<button 
														class="px-2 py-1 bg-red-600/20 text-red-400 rounded text-xs hover:bg-red-600/30 disabled:opacity-50"
														on:click={() => voteOnProposal(proposal.proposalKey, 'no')}
														disabled={loading}
													>
														No ({proposal.noVotes})
													</button>
													<button 
														class="px-2 py-1 bg-slate-600/20 text-slate-400 rounded text-xs hover:bg-slate-600/30 disabled:opacity-50"
														on:click={() => voteOnProposal(proposal.proposalKey, 'abstain')}
														disabled={loading}
													>
														Abstain ({proposal.abstainVotes})
													</button>
												</div>
												<div class="text-right">
													<div class="text-xs text-slate-500">Effective {proposal.effectiveSeason}</div>
													{#if proposal.status === 'approved'}
														<div class="text-xs text-green-400">✓ Approved</div>
													{:else if proposal.status === 'rejected'}
														<div class="text-xs text-red-400">✗ Rejected</div>
													{:else}
														<div class="text-xs text-amber-400">⏳ Pending</div>
													{/if}
												</div>
											</div>
										</div>
									{/each}
								</div>
							{/each}
						</div>
						
						<!-- Show add proposals for this section -->
						{#each getAddProposalsForSection(section.id) as proposal}
							<div class="mt-4 bg-blue-900/10 border border-blue-600/20 rounded-lg p-3 relative">
								<button 
									class="absolute top-2 right-2 p-1 rounded text-red-300 bg-red-900/30 hover:text-red-200 hover:bg-red-900/50 transition-colors"
									on:click={() => deleteProposal(proposal.proposalKey)}
									title="Delete proposal"
								>
									<Trash2 class="h-3 w-3" />
								</button>
								<div class="flex items-center justify-between mb-2 pr-8">
									<span class="text-xs text-blue-400 font-medium">PROPOSED NEW RULE</span>
									<span class="text-xs text-slate-400">{proposal.submittedByManager || 'Manager ' + proposal.submittedBy}</span>
								</div>
								<div class="text-sm text-blue-300 mb-2">• {proposal.proposedText}</div>
								<div class="text-xs text-slate-400 mb-3">{proposal.rationale}</div>
								<div class="flex items-center justify-between">
									<div class="flex space-x-2">
										<button 
											class="px-2 py-1 bg-green-600/20 text-green-400 rounded text-xs hover:bg-green-600/30 disabled:opacity-50"
											on:click={() => voteOnProposal(proposal.proposalKey, 'yes')}
											disabled={loading}
										>
											Yes ({proposal.yesVotes})
										</button>
										<button 
											class="px-2 py-1 bg-red-600/20 text-red-400 rounded text-xs hover:bg-red-600/30 disabled:opacity-50"
											on:click={() => voteOnProposal(proposal.proposalKey, 'no')}
											disabled={loading}
										>
											No ({proposal.noVotes})
										</button>
										<button 
											class="px-2 py-1 bg-slate-600/20 text-slate-400 rounded text-xs hover:bg-slate-600/30 disabled:opacity-50"
											on:click={() => voteOnProposal(proposal.proposalKey, 'abstain')}
											disabled={loading}
										>
											Abstain ({proposal.abstainVotes})
										</button>
									</div>
									<div class="text-right">
										<div class="text-xs text-slate-500">Effective {proposal.effectiveSeason}</div>
										{#if proposal.status === 'approved'}
											<div class="text-xs text-green-400">✓ Approved</div>
										{:else if proposal.status === 'rejected'}
											<div class="text-xs text-red-400">✗ Rejected</div>
										{:else}
											<div class="text-xs text-amber-400">⏳ Pending</div>
										{/if}
									</div>
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
							<div class="space-y-2">
								{#each category.rules as rule, ruleIndex}
									<div class="text-slate-300 group">
										<div 
											class="leading-relaxed {editMode ? 'hover:bg-slate-700/30 p-2 rounded cursor-pointer' : ''} transition-colors relative"
											on:click={() => editMode && startEdit('appendix1', ruleIndex, rule)}
										>
											{#if rule.includes(':')}
												{@const parts = rule.split(':')}
												<span class="text-amber-400 font-semibold">{parts[0]}:</span>
												<span class="ml-1">{parts[1]}</span>
											{:else}
												{rule}
											{/if}
											{#if editMode}
												<Edit3 class="h-4 w-4 text-amber-400 opacity-0 group-hover:opacity-100 transition-opacity absolute right-2 top-2" />
											{/if}
										</div>
									</div>
								{/each}
							</div>
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
				{#if loading}
					<div class="flex items-center justify-center py-8">
						<div class="text-slate-400">Loading amendments...</div>
					</div>
				{:else}
					<div class="space-y-4 mt-4">
						{#each amendments as amendment}
							<div class="border-l-4 {amendment.type === 'original' ? 'border-amber-400' : 'border-green-400'} pl-4">
								<div class="flex justify-between items-start">
									<h3 class="font-semibold text-white">{amendment.year}</h3>
									<div class="text-right">
										<span class="text-slate-400 text-sm">{amendment.title}</span>
										{#if amendment.voteCount}
											<div class="text-xs text-slate-500 mt-1">
												Passed: {amendment.voteCount.yes}Y/{amendment.voteCount.no}N/{amendment.voteCount.abstain}A
											</div>
										{/if}
									</div>
								</div>
								<p class="text-slate-300 mt-1">{amendment.description}</p>
								{#if amendment.type === 'amendment'}
									<div class="text-xs text-green-400 mt-2">✓ Constitutional Amendment</div>
								{/if}
							</div>
						{/each}
						
						{#if amendments.length === 0}
							<div class="text-slate-400 text-center py-4">
								No amendments found
							</div>
						{/if}
					</div>
				{/if}
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
							{constitutionSections.find(s => s.id === editingRule?.sectionId)?.content[editingRule?.ruleIndex ?? 0] ?? ''}
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
					class="px-6 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg font-semibold transition-colors flex items-center space-x-2 disabled:opacity-50"
					on:click={submitProposal}
					disabled={!proposalText.trim() || !proposalRationale.trim() || loading}
				>
					<Vote class="h-4 w-4" />
					<span>{loading ? 'Submitting...' : 'Submit Proposal'}</span>
				</button>
			</div>
		</div>
	</div>
{/if} 
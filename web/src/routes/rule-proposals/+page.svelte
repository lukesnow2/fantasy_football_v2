<script lang="ts">
	import { onMount } from 'svelte';
	import { 
		FileText, 
		Plus, 
		Vote, 
		CheckCircle, 
		XCircle, 
		Clock, 
		Archive,
		ChevronDown,
		ChevronRight,
		Calendar,
		User,
		MessageSquare,
		Trash2
	} from 'lucide-svelte';

	interface RuleProposal {
		proposalKey: number;
		proposalId: string;
		title: string;
		description: string;
		proposalType: 'add_clause' | 'edit_language' | 'tweak_rule';
		affectedSection?: string;
		currentLanguage?: string;
		proposedLanguage: string;
		rationale: string;
		effectiveSeason: number;
		submittedBy: number;
		submittedAt: string;
		votingStartDate?: string;
		votingEndDate?: string;
		status: 'draft' | 'active' | 'passed' | 'rejected' | 'archived';
		requiredVotes: number;
		yesVotes: number;
		noVotes: number;
		abstainVotes: number;
		submittedByManager?: string;
	}

	interface RuleVote {
		voteKey: number;
		proposalKey: number;
		managerKey: number;
		vote: 'yes' | 'no' | 'abstain';
		comment?: string;
		votedAt: string;
		managerName?: string;
	}

	let proposals: RuleProposal[] = [];
	let votes: RuleVote[] = [];
	let loading = true;
	let showCreateForm = false;
	let selectedProposal: RuleProposal | null = null;
	let expandedProposals = new Set<number>();

	// Form data
	let formData = {
		title: '',
		description: '',
		proposalType: 'add_clause' as const,
		affectedSection: '',
		currentLanguage: '',
		proposedLanguage: '',
		rationale: '',
		effectiveSeason: new Date().getFullYear() + 1
	};

	// Vote data
	let voteData = {
		vote: 'yes' as 'yes' | 'no' | 'abstain',
		comment: ''
	};

	onMount(async () => {
		await loadProposals();
		loading = false;
	});

	async function loadProposals() {
		try {
			const response = await fetch('/api/rule-proposals');
			const data = await response.json();
			proposals = data.proposals || [];
		} catch (error) {
			console.error('Error loading proposals:', error);
		}
	}



	async function createProposal() {
		try {
			console.log('Submitting proposal data:', formData);
			
			const response = await fetch('/api/rule-proposals', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(formData)
			});

			if (response.ok) {
				const result = await response.json();
				console.log('Proposal created successfully:', result);
				await loadProposals();
				showCreateForm = false;
				// Reset form
				formData = {
					title: '',
					description: '',
					proposalType: 'add_clause',
					affectedSection: '',
					currentLanguage: '',
					proposedLanguage: '',
					rationale: '',
					effectiveSeason: new Date().getFullYear() + 1
				};
			} else {
				const errorData = await response.json();
				console.error('Failed to create proposal:', response.status, errorData);
				alert(`Failed to create proposal: ${errorData.error || 'Unknown error'}`);
			}
		} catch (error) {
			console.error('Error creating proposal:', error);
			alert(`Error creating proposal: ${error instanceof Error ? error.message : 'Unknown error'}`);
		}
	}

	async function submitVote(proposalKey: number) {
		try {
					const response = await fetch('/api/rule-votes', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				proposalKey,
				vote: voteData.vote,
				comment: voteData.comment
			})
		});

			if (response.ok) {
				await loadProposals(); // Reload proposals to get updated vote counts
				selectedProposal = null;
				voteData = { vote: 'yes', comment: '' };
			}
		} catch (error) {
			console.error('Error submitting vote:', error);
		}
	}

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
				await loadProposals();
			}
		} catch (error) {
			console.error('Error deleting proposal:', error);
		}
	}

	function toggleProposal(proposalKey: number) {
		if (expandedProposals.has(proposalKey)) {
			expandedProposals.delete(proposalKey);
		} else {
			expandedProposals.add(proposalKey);
		}
		expandedProposals = expandedProposals; // Trigger reactivity
	}

	function getVoteCount(proposalKey: number, voteType: 'yes' | 'no' | 'abstain') {
		// Defensive check: ensure proposals array exists and is an array
		if (!proposals || !Array.isArray(proposals)) {
			console.warn('Proposals not loaded yet or not an array:', proposals);
			return 0;
		}
		
		const proposal = proposals.find(p => p && p.proposalKey === proposalKey);
		if (!proposal) {
			console.warn('Proposal not found for key:', proposalKey);
			return 0;
		}
		
		switch (voteType) {
			case 'yes': return proposal.yesVotes || 0;
			case 'no': return proposal.noVotes || 0;
			case 'abstain': return proposal.abstainVotes || 0;
			default: return 0;
		}
	}

	function getProposalVotes(proposalKey: number) {
		// Since we don't have individual vote records, return empty array for now
		// This could be enhanced later to show actual vote history
		return [];
	}

	function getStatusColor(status: string) {
		switch (status) {
			case 'draft': return 'text-slate-400';
			case 'active': return 'text-blue-400';
			case 'passed': return 'text-green-400';
			case 'rejected': return 'text-red-400';
			case 'archived': return 'text-gray-400';
			default: return 'text-slate-400';
		}
	}

	function getStatusIcon(status: string) {
		switch (status) {
			case 'draft': return Clock;
			case 'active': return Vote;
			case 'passed': return CheckCircle;
			case 'rejected': return XCircle;
			case 'archived': return Archive;
			default: return Clock;
		}
	}

	const proposalTypes = [
		{ value: 'add_clause', label: 'Add New Clause' },
		{ value: 'edit_language', label: 'Edit Current Language' },
		{ value: 'tweak_rule', label: 'Tweak Existing Rule' }
	];

	const currentYear = new Date().getFullYear();
	const seasonOptions = Array.from({ length: 5 }, (_, i) => currentYear + i + 1);
</script>

<div class="max-w-7xl mx-auto px-4 py-8 space-y-8">
	<!-- Header -->
	<div class="text-center space-y-4">
		<FileText class="h-16 w-16 text-amber-400 mx-auto" />
		<h1 class="text-4xl font-bold text-white">Annual Rules Meeting</h1>
		<p class="text-xl text-slate-300 max-w-3xl mx-auto">
			Review, propose, and vote on rule changes for the upcoming season. 
			All proposals require league member voting before implementation.
		</p>
	</div>

	<!-- Create Proposal Button -->
	<div class="flex justify-center">
		<button 
			class="bg-amber-500 hover:bg-amber-600 text-white px-6 py-3 rounded-lg font-semibold flex items-center space-x-2 transition-colors"
			on:click={() => showCreateForm = !showCreateForm}
		>
			<Plus class="h-5 w-5" />
			<span>{showCreateForm ? 'Cancel' : 'Create New Proposal'}</span>
		</button>
	</div>

	<!-- Create Proposal Form -->
	{#if showCreateForm}
		<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700/50 p-6 space-y-6">
			<h2 class="text-2xl font-bold text-white">Create Rule Proposal</h2>
			
			<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
				<div class="space-y-4">
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Proposal Title</label>
						<input 
							type="text" 
							bind:value={formData.title}
							class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
							placeholder="Brief, descriptive title"
						/>
					</div>
					
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Proposal Type</label>
						<select 
							bind:value={formData.proposalType}
							class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
						>
							{#each proposalTypes as type}
								<option value={type.value}>{type.label}</option>
							{/each}
						</select>
					</div>
					
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Affected Section (Optional)</label>
						<input 
							type="text" 
							bind:value={formData.affectedSection}
							class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
							placeholder="e.g., Article 3, Appendix 1"
						/>
					</div>
					
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Effective Season</label>
						<select 
							bind:value={formData.effectiveSeason}
							class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
						>
							{#each seasonOptions as season}
								<option value={season}>{season}</option>
							{/each}
						</select>
					</div>
				</div>
				
				<div class="space-y-4">
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Description</label>
						<textarea 
							bind:value={formData.description}
							rows="3"
							class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
							placeholder="Brief description of the proposal"
						></textarea>
					</div>
					
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Rationale</label>
						<textarea 
							bind:value={formData.rationale}
							rows="3"
							class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
							placeholder="Why is this change needed?"
						></textarea>
					</div>
				</div>
			</div>
			
			{#if formData.proposalType === 'edit_language'}
				<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Current Language</label>
						<textarea 
							bind:value={formData.currentLanguage}
							rows="4"
							class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
							placeholder="Current rule text"
						></textarea>
					</div>
					
					<div>
						<label class="block text-sm font-medium text-slate-300 mb-2">Proposed Language</label>
						<textarea 
							bind:value={formData.proposedLanguage}
							rows="4"
							class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
							placeholder="New rule text"
						></textarea>
					</div>
				</div>
			{:else}
				<div>
					<label class="block text-sm font-medium text-slate-300 mb-2">Proposed Language</label>
					<textarea 
						bind:value={formData.proposedLanguage}
						rows="4"
						class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
						placeholder="New rule text or clause"
					></textarea>
				</div>
			{/if}
			
			<div class="flex justify-end space-x-4">
				<button 
					class="px-6 py-2 text-slate-300 hover:text-white transition-colors"
					on:click={() => showCreateForm = false}
				>
					Cancel
				</button>
				<button 
					class="bg-amber-500 hover:bg-amber-600 text-white px-6 py-2 rounded-lg font-semibold transition-colors"
					on:click={createProposal}
				>
					Submit Proposal
				</button>
			</div>
		</div>
	{/if}

	<!-- Proposals List -->
	{#if loading}
		<div class="text-center py-12">
			<div class="animate-spin rounded-full h-12 w-12 border-b-2 border-amber-400 mx-auto"></div>
			<p class="text-slate-400 mt-4">Loading proposals...</p>
		</div>
	{:else}
		<div class="space-y-4">
			{#each proposals as proposal}
				<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700/50 overflow-hidden">
					<div class="p-6 flex items-center justify-between">
						<button 
							class="flex-1 flex items-center space-x-4 hover:bg-slate-700/30 transition-colors rounded-lg p-2 -m-2"
							on:click={() => toggleProposal(proposal.proposalKey)}
						>
							<svelte:component this={getStatusIcon(proposal.status)} class="h-6 w-6 text-amber-400" />
							<div class="text-left flex-1">
								<h3 class="text-lg font-semibold text-white">{proposal.title}</h3>
								<div class="flex items-center space-x-4 text-sm text-slate-400">
									<span class="flex items-center space-x-1">
										<User class="h-4 w-4" />
										<span>{proposal.submittedByManager || 'Unknown'}</span>
									</span>
									<span class="flex items-center space-x-1">
										<Calendar class="h-4 w-4" />
										<span>Effective {proposal.effectiveSeason}</span>
									</span>
									<span class="flex items-center space-x-1">
										<Vote class="h-4 w-4" />
										<span>{getVoteCount(proposal.proposalKey, 'yes')} Yes / {getVoteCount(proposal.proposalKey, 'no')} No</span>
									</span>
								</div>
							</div>
							<svelte:component 
								this={expandedProposals.has(proposal.proposalKey) ? ChevronDown : ChevronRight} 
								class="h-5 w-5 text-slate-400 ml-4" 
							/>
						</button>
						<div class="flex items-center space-x-3 ml-4">
							<button 
								class="p-2 rounded-lg text-red-300 bg-red-900/30 hover:text-red-200 hover:bg-red-900/50 transition-colors border border-red-800/50"
								on:click={() => deleteProposal(proposal.proposalKey)}
								title="Delete proposal"
							>
								<Trash2 class="h-5 w-5" />
							</button>
							<span class="px-3 py-1 rounded-full text-sm font-medium {getStatusColor(proposal.status)}">
								{proposal.status.charAt(0).toUpperCase() + proposal.status.slice(1)}
							</span>
						</div>
					</div>
					
					{#if expandedProposals.has(proposal.proposalKey)}
						<div class="px-6 pb-6 border-t border-slate-700/50">
							<div class="mt-4 space-y-6">
								<!-- Proposal Details -->
								<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
									<div class="space-y-4">
										<div>
											<h4 class="text-sm font-medium text-slate-400 uppercase tracking-wide">Description</h4>
											<p class="text-slate-300 mt-1">{proposal.description}</p>
										</div>
										
										<div>
											<h4 class="text-sm font-medium text-slate-400 uppercase tracking-wide">Rationale</h4>
											<p class="text-slate-300 mt-1">{proposal.rationale}</p>
										</div>
										
										{#if proposal.affectedSection}
											<div>
												<h4 class="text-sm font-medium text-slate-400 uppercase tracking-wide">Affected Section</h4>
												<p class="text-slate-300 mt-1">{proposal.affectedSection}</p>
											</div>
										{/if}
									</div>
									
									<div class="space-y-4">
										{#if proposal.currentLanguage}
											<div>
												<h4 class="text-sm font-medium text-slate-400 uppercase tracking-wide">Current Language</h4>
												<div class="bg-slate-900/50 border border-slate-600 rounded-lg p-3 mt-1">
													<p class="text-slate-300 text-sm">{proposal.currentLanguage}</p>
												</div>
											</div>
										{/if}
										
										<div>
											<h4 class="text-sm font-medium text-slate-400 uppercase tracking-wide">Proposed Language</h4>
											<div class="bg-amber-900/20 border border-amber-600/30 rounded-lg p-3 mt-1">
												<p class="text-amber-300 text-sm">{proposal.proposedLanguage}</p>
											</div>
										</div>
									</div>
								</div>
								
								<!-- Voting Section -->
								{#if proposal.status === 'active'}
									<div class="border-t border-slate-700/50 pt-6">
										<h4 class="text-lg font-semibold text-white mb-4">Cast Your Vote</h4>
										
										<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
											<div class="space-y-4">
												<div>
													<label class="block text-sm font-medium text-slate-300 mb-2">Your Vote</label>
													<div class="flex space-x-4">
														<label class="flex items-center space-x-2">
															<input 
																type="radio" 
																bind:group={voteData.vote} 
																value="yes"
																class="text-amber-500 focus:ring-amber-500"
															/>
															<span class="text-slate-300">Yes</span>
														</label>
														<label class="flex items-center space-x-2">
															<input 
																type="radio" 
																bind:group={voteData.vote} 
																value="no"
																class="text-amber-500 focus:ring-amber-500"
															/>
															<span class="text-slate-300">No</span>
														</label>
														<label class="flex items-center space-x-2">
															<input 
																type="radio" 
																bind:group={voteData.vote} 
																value="abstain"
																class="text-amber-500 focus:ring-amber-500"
															/>
															<span class="text-slate-300">Abstain</span>
														</label>
													</div>
												</div>
												
												<div>
													<label class="block text-sm font-medium text-slate-300 mb-2">Comment (Optional)</label>
													<textarea 
														bind:value={voteData.comment}
														rows="3"
														class="w-full bg-slate-700 border border-slate-600 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-amber-500 focus:border-transparent"
														placeholder="Share your thoughts on this proposal..."
													></textarea>
												</div>
												
												<button 
													class="bg-amber-500 hover:bg-amber-600 text-white px-6 py-2 rounded-lg font-semibold transition-colors"
													on:click={() => submitVote(proposal.proposalKey)}
												>
													Submit Vote
												</button>
											</div>
											
											<div>
												<h5 class="text-sm font-medium text-slate-400 mb-3">Current Vote Results</h5>
												<div class="space-y-3">
													<div class="flex justify-between items-center">
														<span class="text-green-400">Yes</span>
														<span class="text-white font-semibold">{getVoteCount(proposal.proposalKey, 'yes')}</span>
													</div>
													<div class="flex justify-between items-center">
														<span class="text-red-400">No</span>
														<span class="text-white font-semibold">{getVoteCount(proposal.proposalKey, 'no')}</span>
													</div>
													<div class="flex justify-between items-center">
														<span class="text-slate-400">Abstain</span>
														<span class="text-white font-semibold">{getVoteCount(proposal.proposalKey, 'abstain')}</span>
													</div>
													<div class="border-t border-slate-600 pt-3">
														<div class="flex justify-between items-center">
															<span class="text-slate-300">Required for Passage</span>
															<span class="text-white font-semibold">{proposal.requiredVotes}</span>
														</div>
													</div>
												</div>
											</div>
										</div>
									</div>
								{/if}
								
								<!-- Vote History -->
								<!-- Note: Individual vote history not implemented yet -->
								<!-- This would require a separate vote tracking system -->
							</div>
						</div>
					{/if}
				</div>
			{/each}
			
			{#if proposals.length === 0}
				<div class="text-center py-12">
					<FileText class="h-16 w-16 text-slate-600 mx-auto mb-4" />
					<h3 class="text-xl font-semibold text-slate-400 mb-2">No Proposals Yet</h3>
					<p class="text-slate-500">Be the first to propose a rule change for the upcoming season.</p>
				</div>
			{/if}
		</div>
	{/if}
</div> 
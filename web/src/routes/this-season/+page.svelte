<script lang="ts">
	import { Calendar, Trophy, TrendingUp, MessageSquare, ExternalLink, Users, Target, Send } from 'lucide-svelte';
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	import ChatMessage from '$lib/components/chat/ChatMessage.svelte';
	
	// Real data from API
	let standings: any[] = [];
	let recentTransactions: any[] = [];
	let currentWeek = 1;
	let loading = true;
	let error = '';
	let isFinalStandings = false;
	let isSeasonComplete = false;
	let lastPlaceGameLoser: any = null;
	let leagueOverview: any = {};
	
	// Get user from page data
	$: user = $page.data.user;
	$: authenticatedManager = $page.data.authenticatedManager;
	
	// Chat state
	let messages: any[] = [];
	// The logged-in manager's key (null when not authenticated).
	$: currentUserKey = authenticatedManager?.managerKey ?? null;
	let newMessage = '';
	let loadingMessages = false;
	let sendingMessage = false;
	
	// Computed playoff picture data
	$: playoffPicture = calculatePlayoffPicture(standings, currentWeek);
	
	function calculatePlayoffPicture(teams: any[], week: number) {
		if (!teams || teams.length === 0) {
			return {
				clinched: 0,
				fighting: 0,
				eliminated: 0,
				playoffTeams: [],
				eliminatedTeams: [],
				seasonPhase: 'regular'
			};
		}
		
		const totalTeams = teams.length;
		const playoffSpots = 6; // Based on your constitution
		const currentWeek = week;
		const playoffStartWeek = 14; // Based on your constitution
		
		// Determine season phase
		let seasonPhase = 'regular';
		if (currentWeek >= playoffStartWeek) {
			seasonPhase = 'playoffs';
		}
		if (currentWeek >= 17) { // Championship week
			seasonPhase = 'completed';
		}
		
		// Calculate playoff status
		const playoffTeams = teams.filter(team => team.rank <= playoffSpots);
		const eliminatedTeams = teams.filter(team => team.rank > playoffSpots);
		
		// For regular season: calculate clinched vs fighting
		let clinched = 0;
		let fighting = 0;
		
		if (seasonPhase === 'regular') {
			// Teams with high win percentage likely clinched
			clinched = playoffTeams.filter(team => {
				const winPct = parseFloat(team.winPercentage);
				return winPct >= 0.700; // 70% win rate or higher
			}).length;
			
			fighting = playoffSpots - clinched;
		} else if (seasonPhase === 'playoffs') {
			clinched = playoffSpots; // All playoff spots determined
			fighting = 0;
		} else {
			// Season completed
			clinched = totalTeams;
			fighting = 0;
		}
		
		return {
			clinched,
			fighting,
			eliminated: eliminatedTeams.length,
			playoffTeams,
			eliminatedTeams,
			seasonPhase,
			currentWeek
		};
	}

	onMount(async () => {
		try {
			console.log('This Season page loading - calling standings API...');
			
			// Fetch league overview to get current week
			const overviewResponse = await fetch('/api/leagues/overview');
			if (overviewResponse.ok) {
				const overviewData = await overviewResponse.json();
				leagueOverview = overviewData.overview;
				currentWeek = leagueOverview.currentWeek || 1;
				console.log('Current week from overview:', currentWeek);
			}
			
			// Fetch current season standings
			const standingsResponse = await fetch('/api/standings?season=2024');
			console.log('Standings API response status:', standingsResponse.status);
			if (standingsResponse.ok) {
				const standingsData = await standingsResponse.json();
				console.log('Standings data received:', standingsData);
				console.log('Debug info:', standingsData.debug);
				console.log('Playoff games:', standingsData.debug.playoffGames);
				console.log('Championship game found:', standingsData.debug.championshipGameFound);
				console.log('Playoff standings entries:', standingsData.debug.playoffStandingsEntries);
				console.log('Standings array:', standingsData.standings);
				console.log('Standings count:', standingsData.standings.length);
				console.log('First few standings:', standingsData.standings.slice(0, 3));
				console.log('All ranks in standings:', standingsData.standings.map((s: any) => s.rank).sort((a: any, b: any) => a - b));
				console.log('Teams with playoff tiers:', standingsData.standings.map((s: any) => `${s.rank}: ${s.teamName} (${s.playoffTier})`));
				standings = standingsData.standings;
				isFinalStandings = standingsData.isFinalStandings || false;
				isSeasonComplete = standingsData.isSeasonComplete || false;
				lastPlaceGameLoser = standingsData.lastPlaceGameLoser || null;
			} else {
				console.error('Standings API failed:', standingsResponse.status, standingsResponse.statusText);
			}

			// Fetch recent transactions
			const transactionsResponse = await fetch('/api/transactions?limit=10');
			if (transactionsResponse.ok) {
				const transactionsData = await transactionsResponse.json();
				recentTransactions = transactionsData.transactions;
			}

			// Load chat messages
			await loadMessages();

			loading = false;
		} catch (err) {
			console.error('Error fetching data:', err);
			error = 'Failed to load data';
			loading = false;
		}
	});

	// Load chat messages
	async function loadMessages() {
		try {
			loadingMessages = true;
			const response = await fetch('/api/chat/messages?channelId=general&limit=20');
			if (response.ok) {
				const data = await response.json();
				// Ensure each message has a reactions array
				messages = data.messages.map((msg: any) => ({
					...msg,
					reactions: msg.reactions || []
				}));
			}
		} catch (err) {
			console.error('Error loading messages:', err);
		} finally {
			loadingMessages = false;
		}
	}

	// Send message
	async function sendMessage() {
		// Check if user is authenticated
		if (!user) {
			// Redirect to login page with return URL
			goto(`/login?redirect=${encodeURIComponent('/this-season')}`);
			return;
		}
		
		if (!newMessage.trim() || sendingMessage) return;
		
		try {
			sendingMessage = true;
			const response = await fetch('/api/chat/messages', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({
					content: newMessage.trim(),
					channelId: 'general'
				})
			});
			
			if (response.ok) {
				const data = await response.json();
				// Ensure new message has reactions array
				const newMsg = {
					...data.message,
					reactions: data.message.reactions || []
				};
				messages = [...messages, newMsg];
				newMessage = '';
				
				// Scroll to bottom
				setTimeout(() => {
					const messagesContainer = document.querySelector('.messages-container');
					if (messagesContainer) {
						messagesContainer.scrollTop = messagesContainer.scrollHeight;
					}
				}, 100);
			}
		} catch (err) {
			console.error('Error sending message:', err);
		} finally {
			sendingMessage = false;
		}
	}

	// Handle message edit
	async function handleMessageEdit(event: CustomEvent) {
		// Check if user is authenticated
		if (!user) {
			// Redirect to login page with return URL
			goto(`/login?redirect=${encodeURIComponent('/this-season')}`);
			return;
		}
		
		const { messageId, content } = event.detail;
		
		try {
			const response = await fetch('/api/chat/messages', {
				method: 'PUT',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({
					messageId,
					content
				})
			});
			
			if (response.ok) {
				const data = await response.json();
				messages = messages.map(msg => 
					msg.messageId === messageId ? { ...msg, content, editedAt: new Date() } : msg
				);
			}
		} catch (err) {
			console.error('Error editing message:', err);
		}
	}

	// Handle message delete
	async function handleMessageDelete(event: CustomEvent) {
		// Check if user is authenticated
		if (!user) {
			// Redirect to login page with return URL
			goto(`/login?redirect=${encodeURIComponent('/this-season')}`);
			return;
		}
		
		const { messageId } = event.detail;
		
		try {
			const response = await fetch('/api/chat/messages', {
				method: 'DELETE',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({
					messageId
				})
			});
			
			if (response.ok) {
				messages = messages.filter(msg => msg.messageId !== messageId);
			}
		} catch (err) {
			console.error('Error deleting message:', err);
		}
	}

	// Handle message reply
	async function handleMessageReply(event: CustomEvent) {
		// Check if user is authenticated
		if (!user) {
			// Redirect to login page with return URL
			goto(`/login?redirect=${encodeURIComponent('/this-season')}`);
			return;
		}
		
		const { messageId, messageKey } = event.detail;
		
		// For now, let's just log the reply action and focus the input
		console.log('Reply to message:', messageId, messageKey);
		
		// You could implement thread functionality here
		// For now, we'll just add a mention or focus the input
		const input = document.querySelector('input[placeholder*="trash talk"]') as HTMLInputElement;
		if (input) {
			// Find the message author to create a mention
			const replyMessage = messages.find(msg => msg.messageId === messageId);
			if (replyMessage) {
				newMessage = `@${replyMessage.authorDisplayName || replyMessage.authorName} `;
			}
			input.focus();
		}
	}

	// Handle reaction
	async function handleReaction(event: CustomEvent) {
		// Check if user is authenticated
		if (!user) {
			// Redirect to login page with return URL
			goto(`/login?redirect=${encodeURIComponent('/this-season')}`);
			return;
		}
		
		const { messageId, emoji, emojiType, action } = event.detail;
		
		try {
			if (action === 'add') {
				const response = await fetch('/api/chat/reactions', {
					method: 'POST',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify({
						messageId,
						emoji,
						emojiType
					})
				});
				
				if (response.ok) {
					// Reload reactions for this message
					await loadReactions(messageId);
				}
			} else if (action === 'remove') {
				const response = await fetch('/api/chat/reactions', {
					method: 'DELETE',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify({
						messageId,
						emoji
					})
				});
				
				if (response.ok) {
					// Reload reactions for this message
					await loadReactions(messageId);
				}
			}
		} catch (err) {
			console.error('Error handling reaction:', err);
		}
	}

	// Load reactions for a message
	async function loadReactions(messageId: string) {
		try {
			const response = await fetch(`/api/chat/reactions?messageId=${messageId}`);
			if (response.ok) {
				const data = await response.json();
				// Update the message with reactions
				messages = messages.map(msg => 
					msg.messageId === messageId ? { ...msg, reactions: data.reactions } : msg
				);
			}
		} catch (err) {
			console.error('Error loading reactions:', err);
		}
	}

	// Handle keyboard shortcuts
	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			sendMessage();
		}
	}
</script>

<div class="space-y-8">
	<!-- Page Header -->
	<div class="flex items-center justify-between">
		<div>
			<h1 class="text-4xl font-bold text-white mb-2">This Season</h1>
			<p class="text-slate-400">
				Week {currentWeek} • 
				{currentWeek >= 17 ? 'Season Complete' : 
				 currentWeek >= 14 ? 'Playoffs are heating up' : 
				 currentWeek >= 10 ? 'Trade deadline passed' : 
				 'Regular season in progress'}
			</p>
		</div>
		<div class="flex items-center space-x-4">
			<a 
				href="https://yahoo.com" 
				target="_blank"
				class="bg-purple-600 hover:bg-purple-700 text-white px-4 py-2 rounded-lg font-medium flex items-center"
			>
				<ExternalLink class="w-4 h-4 mr-2" />
				Yahoo League
			</a>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
		<!-- Left Column: Standings & Matchups -->
		<div class="lg:col-span-2 space-y-8">
			<!-- Current Standings -->
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<div class="flex items-center justify-between mb-6">
					<h2 class="text-2xl font-bold text-white flex items-center">
						<Trophy class="w-6 h-6 text-amber-400 mr-3" />
						{isFinalStandings ? 'Final Standings' : 'Current Standings'}
					</h2>
					<span class="text-slate-400 text-sm">Updated 2 hours ago</span>
				</div>
				
				{#if loading}
					<div class="flex items-center justify-center h-32">
						<div class="text-slate-400">Loading standings...</div>
					</div>
				{:else if error}
					<div class="flex items-center justify-center h-32">
						<div class="text-red-400">{error}</div>
					</div>
				{:else}
					{#if isFinalStandings && isSeasonComplete}
						<!-- Final Standings with Playoff Bracket -->
						<div class="space-y-6">
							<!-- Championship Results -->
							<div class="bg-gradient-to-r from-amber-500/20 to-amber-600/10 border border-amber-500/30 rounded-lg p-4">
								<h3 class="text-lg font-bold text-amber-400 mb-3 flex items-center">
									🏆 Championship Results
								</h3>
								<div class="space-y-2">
									{#each standings.filter(team => team.playoffTier === 'Champion' || team.playoffTier === 'Runner-up') as team}
										<div class="flex items-center justify-between p-3 rounded-lg 
											{team.playoffTier === 'Champion' ? 'bg-amber-200/20 border border-amber-300/30' : 'bg-slate-600 bg-opacity-40'}">
											<div class="flex items-center space-x-4">
												<div class="text-2xl font-bold 
													{team.playoffTier === 'Champion' ? 'text-amber-300' : 'text-slate-300'}">
													{team.rank}
												</div>
												<div>
													<div class="font-semibold text-white flex items-center">
														{team.teamName}
														{#if team.playoffTier === 'Champion'}
															<span class="ml-2 text-amber-400">👑</span>
														{/if}
													</div>
													<div class="text-slate-400 text-sm">{team.managerName}</div>
													<div class="text-xs font-medium 
														{team.playoffTier === 'Champion' ? 'text-amber-300' : 'text-slate-400'}">
														{team.playoffTier}
													</div>
												</div>
											</div>
											<div class="text-right">
												<div class="font-semibold text-white">{team.wins}-{team.losses}</div>
												<div class="text-slate-400 text-sm">{parseFloat(team.pointsFor).toFixed(1)} PF</div>
											</div>
										</div>
									{/each}
								</div>
							</div>

							<!-- Semifinalists -->
							{#if standings.filter(team => team.playoffTier === 'Semifinalist').length > 0}
								<div class="bg-gradient-to-r from-blue-500/20 to-blue-600/10 border border-blue-500/30 rounded-lg p-4">
									<h3 class="text-lg font-bold text-blue-400 mb-3 flex items-center">
										🥉 Semifinalists
									</h3>
									<div class="space-y-2">
										{#each standings.filter(team => team.playoffTier === 'Semifinalist') as team}
											<div class="flex items-center justify-between p-3 rounded-lg bg-slate-600/40">
												<div class="flex items-center space-x-4">
													<div class="text-xl font-bold text-blue-400">{team.rank}</div>
													<div>
														<div class="font-semibold text-white">{team.teamName}</div>
														<div class="text-slate-400 text-sm">{team.managerName}</div>
														<div class="text-xs text-blue-300 font-medium">{team.playoffTier}</div>
													</div>
												</div>
												<div class="text-right">
													<div class="font-semibold text-white">{team.wins}-{team.losses}</div>
													<div class="text-slate-400 text-sm">{parseFloat(team.pointsFor).toFixed(1)} PF</div>
												</div>
											</div>
										{/each}
									</div>
								</div>
							{/if}

							<!-- Quarterfinalists -->
							{#if standings.filter(team => team.playoffTier === 'Quarterfinalist').length > 0}
								<div class="bg-gradient-to-r from-purple-500/20 to-purple-600/10 border border-purple-500/30 rounded-lg p-4">
									<h3 class="text-lg font-bold text-purple-400 mb-3 flex items-center">
										⚔️ Quarterfinalists
									</h3>
									<div class="space-y-2">
										{#each standings.filter(team => team.playoffTier === 'Quarterfinalist') as team}
											<div class="flex items-center justify-between p-3 rounded-lg bg-slate-600/40">
												<div class="flex items-center space-x-4">
													<div class="text-xl font-bold text-purple-400">{team.rank}</div>
													<div>
														<div class="font-semibold text-white">{team.teamName}</div>
														<div class="text-slate-400 text-sm">{team.managerName}</div>
														<div class="text-xs text-purple-300 font-medium">{team.playoffTier}</div>
													</div>
												</div>
												<div class="text-right">
													<div class="font-semibold text-white">{team.wins}-{team.losses}</div>
													<div class="text-slate-400 text-sm">{parseFloat(team.pointsFor).toFixed(1)} PF</div>
												</div>
											</div>
										{/each}
									</div>
								</div>
							{/if}

							<!-- Final Standings: Consolation Bracket (Non-Playoff Teams) -->
							<div class="bg-slate-700/30 border border-slate-600/50 rounded-lg p-4">
								<h3 class="text-lg font-bold text-slate-400 mb-3 flex items-center">
									📋 Final Standings (Consolation Bracket)
								</h3>
								<div class="space-y-2">
									{#each standings.filter(team => team.rank > 6).sort((a, b) => a.rank - b.rank) as team}
										<div class="flex items-center justify-between p-3 rounded-lg bg-slate-600/30">
											<div class="flex items-center space-x-4">
												<div class="text-xl font-bold text-slate-400">{team.rank}</div>
												<div>
													<div class="font-semibold text-white flex items-center">
														{team.teamName}
														{#if team.isLastPlaceLoser}
															<span class="ml-2 text-2xl" title="Last Place Game Loser">💩</span>
														{/if}
													</div>
													<div class="text-slate-400 text-sm">{team.managerName}</div>
													{#if team.playoffTier && team.playoffTier !== 'Regular Season'}
														<div class="text-xs text-slate-500 font-medium">{team.playoffTier}</div>
													{/if}
												</div>
											</div>
											<div class="text-right">
												<div class="font-semibold text-white">{team.wins}-{team.losses}</div>
												<div class="text-slate-400 text-sm">{parseFloat(team.pointsFor).toFixed(1)} PF</div>
											</div>
										</div>
									{/each}
								</div>
							</div>
						</div>
					{:else}
						<!-- Regular Season Standings -->
						<div class="space-y-2">
							{#each standings as team}
								<div class="flex items-center justify-between p-4 rounded-lg 
									{team.rank <= 6 ? 'bg-green-500/10 border border-green-500/20' : 'bg-slate-700/30'}">
									<div class="flex items-center space-x-4">
										<div class="text-2xl font-bold 
											{team.rank <= 6 ? 'text-green-400' : 'text-slate-400'}">
											{team.rank}
										</div>
										<div>
											<div class="font-semibold text-white">{team.teamName}</div>
											<div class="text-slate-400 text-sm">{team.managerName}</div>
											{#if team.rank <= 6}
												<div class="text-xs text-green-300 font-medium">Playoff Position</div>
											{/if}
										</div>
									</div>
									<div class="text-right">
										<div class="font-semibold text-white">{team.wins}-{team.losses}</div>
										<div class="text-slate-400 text-sm">{parseFloat(team.pointsFor).toFixed(1)} PF</div>
									</div>
									<div class="text-right">
										<div class="font-medium 
											{team.streak?.startsWith('W') ? 'text-green-400' : 'text-red-400'}">
											{team.streak || 'TBD'}
										</div>
									</div>
								</div>
							{/each}
						</div>
					{/if}
				{/if}
			</section>

			<!-- Recent Transactions -->
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
					<TrendingUp class="w-6 h-6 text-blue-400 mr-3" />
					Recent Transactions
				</h2>
				
				{#if loading}
					<div class="flex items-center justify-center h-32">
						<div class="text-slate-400">Loading transactions...</div>
					</div>
				{:else}
					<div class="space-y-4">
						{#each recentTransactions as transaction}
							<div class="flex items-center justify-between p-4 bg-slate-700/30 rounded-lg">
								<div class="flex items-center space-x-3">
									<div class="w-2 h-2 rounded-full 
										{transaction.type === 'trade' ? 'bg-amber-400' : 
										 transaction.type === 'add' || transaction.type === 'waiver' ? 'bg-green-400' : 'bg-red-400'}">
									</div>
									<div>
										<div class="text-white font-medium">
											{transaction.teamName || 'Unknown Team'}
										</div>
										<div class="text-slate-400 text-sm">
											{transaction.type} - {transaction.playerName}
											{#if transaction.faabBid}
												(${transaction.faabBid})
											{/if}
										</div>
									</div>
								</div>
								<div class="text-slate-500 text-sm">{transaction.timeAgo}</div>
							</div>
						{/each}
					</div>
				{/if}
			</section>
		</div>

		<!-- Right Column: Message Board & Quick Stats -->
		<div class="space-y-8">
			<!-- Message Board -->
			<section class="bg-gradient-to-br from-slate-800/60 to-slate-900/40 rounded-2xl p-8 border border-slate-700/30 backdrop-blur-sm shadow-2xl">
				<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
					<div class="p-2 bg-green-500 rounded-lg mr-3">
						<MessageSquare class="w-6 h-6 text-white" />
					</div>
					League Chat
				</h2>
				
				<div class="space-y-3 max-h-96 overflow-y-auto bg-slate-800 rounded-lg p-4 border border-slate-700">
					{#if loadingMessages}
						<div class="flex items-center justify-center h-32 text-slate-400">
							<div class="flex flex-col items-center gap-3">
								<div class="animate-spin rounded-full h-8 w-8 border-b-2 border-amber-400"></div>
								<span class="text-sm">Loading messages...</span>
							</div>
						</div>
					{:else}
						{#each messages as msg}
							<ChatMessage 
								message={msg}
								{currentUserKey}
								on:edit={handleMessageEdit}
								on:delete={handleMessageDelete}
								on:reaction={handleReaction}
								on:reply={handleMessageReply}
							/>
						{/each}
					{/if}
				</div>
				
				<div class="mt-6 pt-4 border-t border-slate-700">
					<div class="flex gap-3">
						<input 
							type="text" 
							placeholder="Drop some trash talk..." 
							class="flex-1 bg-slate-700 border border-slate-600 rounded-lg px-4 py-3 text-white placeholder-slate-400 focus:outline-none focus:border-amber-500"
							bind:value={newMessage}
							on:keydown={handleKeydown}
						/>
						<button 
							on:click={sendMessage}
							disabled={sendingMessage}
							class="bg-amber-500 hover:bg-amber-600 text-black px-6 py-3 rounded-lg font-medium disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
						>
							{#if sendingMessage}
								<svg class="animate-spin h-4 w-4 text-black" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
									<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
									<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
								</svg>
							{:else}
								<Send class="w-4 h-4" />
							{/if}
						</button>
					</div>
				</div>
			</section>

			<!-- Power Rankings Preview -->
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h2 class="text-xl font-bold text-white mb-6 flex items-center">
					<Target class="w-5 h-5 text-purple-400 mr-3" />
					Power Rankings
				</h2>
				
				<div class="space-y-3">
					{#each standings.slice(0, 4) as team, i}
						<div class="flex items-center justify-between">
							<div class="flex items-center space-x-3">
								<span class="text-lg font-bold text-slate-400">#{i + 1}</span>
								<span class="text-white">{team.team}</span>
							</div>
							<div class="flex items-center space-x-2">
								{#if i === 0}
									<span class="text-green-400 text-sm">↑2</span>
								{:else if i === 1}
									<span class="text-red-400 text-sm">↓1</span>
								{:else}
									<span class="text-slate-500 text-sm">-</span>
								{/if}
							</div>
						</div>
					{/each}
				</div>
				
				<a href="/power-rankings" class="mt-4 inline-flex items-center text-purple-400 hover:text-purple-300 font-medium text-sm">
					View Full Rankings →
				</a>
			</section>

			<!-- Playoff Picture -->
			<section class="bg-gradient-to-br from-green-900/20 to-amber-900/20 rounded-xl p-6 border border-green-500/20">
				<h2 class="text-xl font-bold text-white mb-4 flex items-center">
					<Trophy class="w-5 h-5 text-amber-400 mr-3" />
					{playoffPicture.seasonPhase === 'completed' ? 'Final Standings' : 
					 playoffPicture.seasonPhase === 'playoffs' ? 'Playoff Bracket' : 'Playoff Picture'}
				</h2>
				
				{#if playoffPicture.seasonPhase === 'regular'}
					<div class="space-y-2 text-sm">
						<div class="flex justify-between">
							<span class="text-slate-300">Clinched:</span>
							<span class="text-green-400 font-medium">{playoffPicture.clinched} teams</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-300">Fighting for spots:</span>
							<span class="text-amber-400 font-medium">{playoffPicture.fighting} teams</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-300">Eliminated:</span>
							<span class="text-red-400 font-medium">{playoffPicture.eliminated} teams</span>
						</div>
					</div>
					
					{#if playoffPicture.playoffTeams.length > 0}
						<div class="mt-4 pt-3 border-t border-green-500/20">
							<div class="text-xs text-slate-400 mb-2">Playoff Teams:</div>
							<div class="space-y-1">
								{#each playoffPicture.playoffTeams.slice(0, 3) as team}
									<div class="text-xs text-white">#{team.rank} {team.teamName}</div>
								{/each}
								{#if playoffPicture.playoffTeams.length > 3}
									<div class="text-xs text-slate-400">+{playoffPicture.playoffTeams.length - 3} more</div>
								{/if}
							</div>
						</div>
					{/if}
				{:else if playoffPicture.seasonPhase === 'playoffs'}
					<div class="space-y-2 text-sm">
						<div class="flex justify-between">
							<span class="text-slate-300">Playoff Teams:</span>
							<span class="text-green-400 font-medium">{playoffPicture.playoffTeams.length} teams</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-300">Eliminated:</span>
							<span class="text-red-400 font-medium">{playoffPicture.eliminatedTeams.length} teams</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-300">Current Week:</span>
							<span class="text-amber-400 font-medium">Week {playoffPicture.currentWeek}</span>
						</div>
					</div>
					
					<div class="mt-4 pt-3 border-t border-green-500/20">
						<div class="text-xs text-slate-400 mb-2">Playoff Bracket:</div>
						<div class="space-y-1">
							{#each playoffPicture.playoffTeams.slice(0, 4) as team}
								<div class="text-xs text-white">#{team.rank} {team.teamName}</div>
							{/each}
						</div>
					</div>
				{:else}
					<!-- Season Completed - Show Final Standings -->
					<div class="space-y-2 text-sm">
						<div class="flex justify-between">
							<span class="text-slate-300">Champion:</span>
							<span class="text-amber-400 font-medium">{standings[0]?.teamName || 'TBD'}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-300">Runner-up:</span>
							<span class="text-slate-400 font-medium">{standings[1]?.teamName || 'TBD'}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-300">3rd Place:</span>
							<span class="text-orange-400 font-medium">{standings[2]?.teamName || 'TBD'}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-300">Consolation Winner:</span>
							<span class="text-blue-400 font-medium">{standings[6]?.teamName || 'TBD'}</span>
						</div>
					</div>
					
					<div class="mt-4 pt-3 border-t border-green-500/20">
						<div class="text-xs text-slate-400 mb-2">Playoff Rankings:</div>
						<div class="space-y-1">
							{#each standings.slice(0, 6) as team, i}
								<div class="text-xs text-white">#{i + 1} {team.teamName}</div>
							{/each}
						</div>
					</div>
					
					<div class="mt-3 pt-3 border-t border-slate-500/20">
						<div class="text-xs text-slate-400 mb-2">Consolation Rankings:</div>
						<div class="space-y-1">
							{#each standings.filter(team => team.rank > 6).slice(0, 4) as team, i}
								<div class="text-xs text-white">#{team.rank} {team.teamName}</div>
							{/each}
						</div>
					</div>
				{/if}
			</section>
		</div>
	</div>
</div>

<style>
	.custom-scrollbar::-webkit-scrollbar {
		width: 8px;
	}

	.custom-scrollbar::-webkit-scrollbar-track {
		background: rgba(51, 65, 85, 0.1);
		border-radius: 4px;
	}

	.custom-scrollbar::-webkit-scrollbar-thumb {
		background: linear-gradient(135deg, rgba(245, 158, 11, 0.6), rgba(245, 158, 11, 0.8));
		border-radius: 4px;
		border: 1px solid rgba(71, 85, 105, 0.3);
	}

	.custom-scrollbar::-webkit-scrollbar-thumb:hover {
		background: linear-gradient(135deg, rgba(245, 158, 11, 0.8), rgba(245, 158, 11, 1));
		box-shadow: 0 2px 4px rgba(245, 158, 11, 0.3);
	}

	/* Firefox scrollbar */
	.custom-scrollbar {
		scrollbar-width: thin;
		scrollbar-color: rgba(245, 158, 11, 0.6) rgba(51, 65, 85, 0.1);
	}
</style> 
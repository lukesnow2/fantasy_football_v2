<script lang="ts">
	import { Calendar, Trophy, TrendingUp, MessageSquare, ExternalLink, Users, Target } from 'lucide-svelte';
	import { onMount } from 'svelte';
	
	// Real data from API
	let standings: any[] = [];
	let recentTransactions: any[] = [];
	let currentWeek = 1;
	let loading = true;
	let error = '';
	
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
			// Fetch current season standings
			const standingsResponse = await fetch('/api/standings?season=2024');
			if (standingsResponse.ok) {
				const standingsData = await standingsResponse.json();
				standings = standingsData.standings;
				currentWeek = standingsData.currentWeek || 1;
			}

			// Fetch recent transactions
			const transactionsResponse = await fetch('/api/transactions?limit=10');
			if (transactionsResponse.ok) {
				const transactionsData = await transactionsResponse.json();
				recentTransactions = transactionsData.transactions;
			}

			loading = false;
		} catch (err) {
			console.error('Error fetching data:', err);
			error = 'Failed to load data';
			loading = false;
		}
	});

	const messages = [
		{ user: "Mike", message: "CMC trade was highway robbery 😤", time: "3 hours ago", replies: 7 },
		{ user: "Sarah", message: "Anyone else think the playoffs are wide open this year?", time: "6 hours ago", replies: 12 },
		{ user: "Jake", message: "Kelce for a 1st? Bold move Alex...", time: "1 day ago", replies: 5 },
		{ user: "Emma", message: "Tyler Boyd szn 🚀", time: "1 day ago", replies: 3 },
	];
</script>

<div class="space-y-8">
	<!-- Page Header -->
	<div class="flex items-center justify-between">
		<div>
			<h1 class="text-4xl font-bold text-white mb-2">This Season</h1>
			<p class="text-slate-400">Week 15 • Playoffs are heating up</p>
		</div>
		<div class="flex items-center space-x-4">
			<div class="bg-green-500/20 text-green-400 px-4 py-2 rounded-lg font-medium">
				Live
			</div>
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
						Current Standings
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
					<div class="space-y-2">
						{#each standings as team}
							<div class="flex items-center justify-between p-4 rounded-lg 
								{team.rank <= 4 ? 'bg-green-500/10 border border-green-500/20' : 'bg-slate-700/30'}">
								<div class="flex items-center space-x-4">
									<div class="text-2xl font-bold 
										{team.rank <= 4 ? 'text-green-400' : 'text-slate-400'}">
										{team.rank}
									</div>
									<div>
										<div class="font-semibold text-white">{team.teamName}</div>
										<div class="text-slate-400 text-sm">{team.managerName}</div>
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
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h2 class="text-xl font-bold text-white mb-6 flex items-center">
					<MessageSquare class="w-5 h-5 text-green-400 mr-3" />
					League Chat
				</h2>
				
				<div class="space-y-4 max-h-96 overflow-y-auto">
					{#each messages as msg}
						<div class="p-3 bg-slate-700/40 rounded-lg">
							<div class="flex items-center justify-between mb-2">
								<span class="font-medium text-amber-400">{msg.user}</span>
								<span class="text-slate-500 text-xs">{msg.time}</span>
							</div>
							<p class="text-slate-300 text-sm mb-2">{msg.message}</p>
							<div class="flex items-center text-slate-500 text-xs">
								<MessageSquare class="w-3 h-3 mr-1" />
								{msg.replies} replies
							</div>
						</div>
					{/each}
				</div>
				
				<div class="mt-4 pt-4 border-t border-slate-700/50">
					<div class="flex space-x-2">
						<input 
							type="text" 
							placeholder="Drop some trash talk..." 
							class="flex-1 bg-slate-700/50 border border-slate-600 rounded-lg px-3 py-2 text-white placeholder-slate-400 focus:outline-none focus:border-amber-400"
						/>
						<button class="bg-amber-500 hover:bg-amber-600 text-black px-4 py-2 rounded-lg font-medium">
							Send
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
					</div>
					
					<div class="mt-4 pt-3 border-t border-green-500/20">
						<div class="text-xs text-slate-400 mb-2">Final Rankings:</div>
						<div class="space-y-1">
							{#each standings.slice(0, 5) as team, i}
								<div class="text-xs text-white">#{i + 1} {team.teamName}</div>
							{/each}
						</div>
					</div>
				{/if}
			</section>
		</div>
	</div>
</div> 
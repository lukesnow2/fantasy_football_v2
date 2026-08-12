<script lang="ts">
	import { Calendar, Trophy, TrendingUp, ExternalLink, Users, Target } from 'lucide-svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	import ChatPreview from '$lib/components/chat/ChatPreview.svelte';
	import { YAHOO_LEAGUE_URL } from '$lib/config/league';
	
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
	// Power rankings are their own mart, not a reordering of the standings — the
	// preview used to render `standings.slice(0, 4)` under a "Power Rankings" heading.
	let powerRankings: any[] = [];
	let powerRankingsWeek: number | null = null;
	
	// Get user from page data
	$: user = $page.data.user;
	$: authenticatedManager = $page.data.authenticatedManager;
	
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
			
			// Fetch standings for the current season (API defaults to the latest season).
			const standingsResponse = await fetch('/api/standings');
			if (standingsResponse.ok) {
				const standingsData = await standingsResponse.json();
				standings = standingsData.standings;
				isFinalStandings = standingsData.isFinalStandings || false;
				isSeasonComplete = standingsData.isSeasonComplete || false;
				lastPlaceGameLoser = standingsData.lastPlaceGameLoser || null;
			} else {
				console.error('Standings API failed:', standingsResponse.status, standingsResponse.statusText);
			}

			// Fetch the latest ranked week's power rankings for the preview panel.
			const powerResponse = await fetch('/api/power-rankings');
			if (powerResponse.ok) {
				const powerData = await powerResponse.json();
				powerRankings = powerData.rankings ?? [];
				powerRankingsWeek = powerData.week ?? null;
			} else {
				console.error('Power rankings API failed:', powerResponse.status);
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

</script>

<div class="space-y-8">
	<PageHeader icon={Calendar} title="This Season">
		Live standings, matchups and the playoff picture as the season plays out.
	</PageHeader>

	<!-- Status bar: the Trade Center's filter-bar treatment, which is where
	     page-level context and controls live. -->
	<div class="flex flex-wrap items-center justify-between gap-3 bg-slate-800/30 rounded-lg p-4">
		<div class="text-slate-300 font-medium">
			Week {currentWeek} <span class="text-slate-500">•</span>
			<span class="text-slate-400">
				{currentWeek >= 17
					? 'Season Complete'
					: currentWeek >= 14
						? 'Playoffs are heating up'
						: currentWeek >= 10
							? 'Trade deadline passed'
							: 'Regular season in progress'}
			</span>
		</div>
		<a
			href={YAHOO_LEAGUE_URL}
			target="_blank"
			rel="noopener noreferrer"
			class="bg-purple-600 hover:bg-purple-700 text-white px-4 py-2 rounded-lg font-medium flex items-center"
		>
			<ExternalLink class="w-4 h-4 mr-2" />
			Yahoo League
		</a>
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
			<ChatPreview />

			<!-- Power Rankings Preview -->
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h2 class="text-xl font-bold text-white mb-6 flex items-center">
					<Target class="w-5 h-5 text-purple-400 mr-3" />
					Power Rankings
				</h2>
				
				{#if powerRankings.length > 0}
					<div class="space-y-3">
						{#each powerRankings.slice(0, 5) as team (team.teamKey)}
							<div class="flex items-center justify-between">
								<div class="flex items-center space-x-3 min-w-0">
									<span class="text-lg font-bold text-slate-400 tabular-nums">#{team.powerRank}</span>
									<span class="min-w-0">
										<span class="block truncate text-white">{team.teamName}</span>
										{#if team.managerName}
											<span class="block text-xs text-slate-400">{team.managerName}</span>
										{/if}
									</span>
								</div>
								<!-- Real week-over-week movement. This used to be a hardcoded
								     ↑2 on row one and ↓1 on row two, regardless of the data. -->
								<div class="flex items-center space-x-2 shrink-0">
									{#if team.rankChange == null}
										<span class="text-slate-500 text-sm">–</span>
									{:else if team.rankChange > 0}
										<span class="text-green-400 text-sm tabular-nums">↑{team.rankChange}</span>
									{:else if team.rankChange < 0}
										<span class="text-red-400 text-sm tabular-nums">↓{Math.abs(team.rankChange)}</span>
									{:else}
										<span class="text-slate-500 text-sm">–</span>
									{/if}
								</div>
							</div>
						{/each}
					</div>
					{#if powerRankingsWeek}
						<p class="mt-3 text-xs text-slate-500">Through week {powerRankingsWeek}</p>
					{/if}
				{:else}
					<p class="text-sm text-slate-500">Power rankings aren't available for this season yet.</p>
				{/if}

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


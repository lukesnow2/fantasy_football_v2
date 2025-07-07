<script lang="ts">
	import { onMount } from 'svelte';
	import { Crown, Trophy, Target, TrendingUp, Award, BarChart3, Users, Medal } from 'lucide-svelte';
	import ManagerProfilePicture from '$lib/components/ManagerProfilePicture.svelte';
	
	let hallOfFameData: any = null;
	let loading = true;
	let error = '';

	// Load Hall of Fame data
	async function loadHallOfFameData() {
		loading = true;
		try {
			const response = await fetch('/api/hall-of-fame');
			if (response.ok) {
				hallOfFameData = await response.json();
				console.log('Hall of Fame data loaded:', hallOfFameData);
			} else {
				error = 'Failed to load Hall of Fame data';
				console.error('Failed to fetch Hall of Fame data:', response.status, response.statusText);
			}
		} catch (err) {
			error = 'Error loading Hall of Fame data';
			console.error('Error fetching Hall of Fame data:', err);
		} finally {
			loading = false;
		}
	}

	onMount(loadHallOfFameData);

	// Helper functions
	function getTierColor(rank: number): string {
		if (rank <= 3) return 'from-amber-500 to-yellow-400'; // Hall of Fame Elite
		if (rank <= 6) return 'from-purple-500 to-indigo-400'; // Hall of Fame
		if (rank <= 10) return 'from-blue-500 to-cyan-400'; // Hall of Very Good
		if (rank <= 15) return 'from-green-500 to-emerald-400'; // Solid Contributor
		return 'from-slate-500 to-gray-400'; // Developing Legacy
	}

	function getTierBadge(rank: number): string {
		if (rank <= 3) return '👑'; // Hall of Fame Elite
		if (rank <= 6) return '🏆'; // Hall of Fame
		if (rank <= 10) return '🥇'; // Hall of Very Good
		if (rank <= 15) return '🥈'; // Solid Contributor
		return '🥉'; // Developing Legacy
	}

	function getTierName(rank: number): string {
		if (rank <= 3) return 'Hall of Fame Elite';
		if (rank <= 6) return 'Hall of Fame';
		if (rank <= 10) return 'Hall of Very Good';
		if (rank <= 15) return 'Solid Contributor';
		return 'Developing Legacy';
	}

	function getWinPercentageColor(winPct: number): string {
		if (winPct >= 0.7) return 'text-green-400';
		if (winPct >= 0.6) return 'text-blue-400';
		if (winPct >= 0.5) return 'text-yellow-400';
		if (winPct >= 0.4) return 'text-orange-400';
		return 'text-red-400';
	}

	function getIndexColor(rank: number, totalManagers: number): string {
		const percentage = rank / totalManagers;
		if (percentage <= 0.2) return 'text-amber-400';
		if (percentage <= 0.4) return 'text-purple-400';
		if (percentage <= 0.6) return 'text-blue-400';
		if (percentage <= 0.8) return 'text-green-400';
		return 'text-slate-400';
	}

	$: managers = hallOfFameData?.hall_of_fame || [];
	$: analytics = hallOfFameData?.analytics || {};
	$: tiers = hallOfFameData?.tiers || [];
</script>

<svelte:head>
	<title>Hall of Fame - The League</title>
</svelte:head>

<div class="space-y-8">
	<!-- Hero Section -->
	<section class="text-center py-8">
		<div class="max-w-4xl mx-auto">
			<Crown class="h-16 w-16 text-amber-400 mx-auto mb-4" />
			<h1 class="text-4xl font-bold text-white mb-4">
				The League <span class="text-amber-400">Hall of Fame</span>
			</h1>
			<p class="text-xl text-slate-300 leading-relaxed">
				Celebrating the greatest managers in league history. Based on championships, consistency, and overall excellence.
			</p>
		</div>
	</section>

	{#if loading}
		<div class="flex items-center justify-center h-64">
			<div class="text-slate-400">Loading Hall of Fame...</div>
		</div>
	{:else if error}
		<div class="flex items-center justify-center h-64">
			<div class="text-red-400">{error}</div>
		</div>
	{:else if hallOfFameData}
		<!-- Overall Statistics -->
		<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
			<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
				<BarChart3 class="w-6 h-6 text-blue-400 mr-3" />
				Hall of Fame Overview
			</h2>
			
			<div class="grid grid-cols-2 md:grid-cols-5 gap-6">
				<div class="text-center">
					<div class="text-3xl font-bold text-blue-400">{analytics.total_managers || 0}</div>
					<div class="text-slate-400">Total Managers</div>
				</div>
				<div class="text-center">
					<div class="text-3xl font-bold text-amber-400">{parseFloat(analytics.avg_championships || 0).toFixed(1)}</div>
					<div class="text-slate-400">Avg Championships</div>
				</div>
				<div class="text-center">
					<div class="text-3xl font-bold text-green-400">{(parseFloat(analytics.avg_win_percentage || 0) * 100).toFixed(1)}%</div>
					<div class="text-slate-400">Avg Win Rate</div>
				</div>
				<div class="text-center">
					<div class="text-3xl font-bold text-purple-400">{parseFloat(analytics.avg_seasons_played || 0).toFixed(1)}</div>
					<div class="text-slate-400">Avg Seasons</div>
				</div>
				<div class="text-center">
					<div class="text-3xl font-bold text-cyan-400">{parseFloat(analytics.avg_hall_of_fame_index || 0).toFixed(3)}</div>
					<div class="text-slate-400">Avg HoF Index</div>
				</div>
			</div>
			
			<!-- Additional Analytics Row -->
			<div class="grid grid-cols-2 md:grid-cols-4 gap-6 mt-6 pt-6 border-t border-slate-700/50">
				<div class="text-center">
					<div class="text-2xl font-bold text-orange-400">{parseFloat(analytics.avg_points_per_game || 0).toFixed(1)}</div>
					<div class="text-slate-400">Avg Pts/Game</div>
				</div>
				<div class="text-center">
					<div class="text-2xl font-bold text-emerald-400">{parseInt(analytics.total_career_wins || 0).toLocaleString()}</div>
					<div class="text-slate-400">Total Wins</div>
				</div>
				<div class="text-center">
					<div class="text-2xl font-bold text-red-400">{parseInt(analytics.total_career_losses || 0).toLocaleString()}</div>
					<div class="text-slate-400">Total Losses</div>
				</div>
				<div class="text-center">
					<div class="text-2xl font-bold text-yellow-400">{parseInt(analytics.total_career_ties || 0)}</div>
					<div class="text-slate-400">Total Ties</div>
				</div>
			</div>
		</section>

		<!-- Tier Breakdown -->
		{#if tiers.length > 0}
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
					<Medal class="w-6 h-6 text-yellow-400 mr-3" />
					Hall of Fame Tiers
				</h2>
				
				<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
					{#each tiers as tier}
						<div class="bg-slate-700/30 rounded-lg p-4 border border-slate-600/50">
							<h3 class="font-bold text-white mb-2">{tier.tier}</h3>
							<div class="space-y-2 text-sm">
								<div class="flex justify-between">
									<span class="text-slate-400">Managers:</span>
									<span class="text-white font-medium">{tier.manager_count}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Avg Seasons:</span>
									<span class="text-blue-400 font-medium">{parseFloat(tier.avg_seasons || 0).toFixed(1)}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Avg Championships:</span>
									<span class="text-amber-400 font-medium">{parseFloat(tier.avg_championships || 0).toFixed(1)}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Avg Win %:</span>
									<span class="text-green-400 font-medium">{(parseFloat(tier.avg_win_pct || 0) * 100).toFixed(1)}%</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Avg HoF Index:</span>
									<span class="text-cyan-400 font-medium">{parseFloat(tier.avg_hall_of_fame_index || 0).toFixed(3)}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Career Record:</span>
									<span class="text-white font-medium">{parseInt(tier.total_wins || 0)}-{parseInt(tier.total_losses || 0)}</span>
								</div>
							</div>
						</div>
					{/each}
				</div>
			</section>
		{/if}

		<!-- Hall of Fame Rankings -->
		<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
			<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
				<Trophy class="w-6 h-6 text-amber-400 mr-3" />
				Official Rankings
			</h2>
			
			<div class="space-y-3">
				{#each managers as manager, index}
					<div class="bg-gradient-to-r {getTierColor(manager.hall_of_fame_rank)} p-[1px] rounded-xl">
						<div class="bg-slate-800 rounded-xl p-6">
							<div class="flex items-center justify-between">
								<!-- Left Side: Rank and Manager Info -->
								<div class="flex items-center space-x-4">
									<div class="flex-shrink-0 text-center">
										<div class="text-3xl font-bold text-white">#{manager.hall_of_fame_rank}</div>
										<div class="text-lg">{getTierBadge(manager.hall_of_fame_rank)}</div>
									</div>
									
									<div class="flex items-center space-x-4">
										<ManagerProfilePicture 
											managerName={manager.manager_name} 
											className="w-16 h-16" 
										/>
										<div>
											<h3 class="text-xl font-bold text-white">{manager.manager_name}</h3>
											<div class="text-sm text-slate-400">{getTierName(manager.hall_of_fame_rank)}</div>
											<div class="flex items-center gap-2 mt-1">
												{#if manager.championships_won > 0}
													<span class="flex items-center text-amber-400 text-sm">
														<Trophy class="w-4 h-4 mr-1" />
														{manager.championships_won} Championship{manager.championships_won > 1 ? 's' : ''}
													</span>
												{/if}
											</div>
										</div>
									</div>
								</div>

								<!-- Right Side: Stats Grid -->
								<div class="grid grid-cols-2 md:grid-cols-5 gap-4 text-center">
									<div>
										<div class="text-lg font-bold {getIndexColor(manager.hall_of_fame_rank, analytics.total_managers)}">
											#{manager.hall_of_fame_rank}
										</div>
										<div class="text-xs text-slate-400">HoF Rank</div>
									</div>
									<div>
										<div class="text-lg font-bold text-cyan-400">
											{parseFloat(manager.hall_of_fame_index || 0).toFixed(3)}
										</div>
										<div class="text-xs text-slate-400">HoF Index</div>
									</div>
									<div>
										<div class="text-lg font-bold {getWinPercentageColor(manager.career_win_percentage)}">
											{(manager.career_win_percentage * 100).toFixed(1)}%
										</div>
										<div class="text-xs text-slate-400">Win Rate</div>
									</div>
									<div>
										<div class="text-lg font-bold text-emerald-400">
											{manager.career_wins}-{manager.career_losses}
											{#if manager.career_ties > 0}-{manager.career_ties}{/if}
										</div>
										<div class="text-xs text-slate-400">Career Record</div>
									</div>
									<div>
										<div class="text-lg font-bold text-blue-400">
											{parseFloat(manager.total_points_scored).toLocaleString()}
										</div>
										<div class="text-xs text-slate-400">Career Points</div>
									</div>
								</div>
							</div>

							<!-- Additional Stats Row -->
							<div class="mt-4 pt-4 border-t border-slate-700/50">
								<div class="grid grid-cols-2 md:grid-cols-4 gap-6 text-sm">
									<div class="flex justify-between">
										<span class="text-slate-400">Avg Points/Game:</span>
										<span class="font-medium text-green-400">{parseFloat(manager.avg_points_per_game).toFixed(1)}</span>
									</div>
									<div class="flex justify-between">
										<span class="text-slate-400">Playoff Apps:</span>
										<span class="font-medium text-cyan-400">{manager.playoff_appearances}</span>
									</div>
									<div class="flex justify-between">
										<span class="text-slate-400">Seasons Played:</span>
										<span class="font-medium text-orange-400">{manager.total_seasons}</span>
									</div>
									<div class="flex justify-between">
										<span class="text-slate-400">Consistency:</span>
										<span class="font-medium text-rose-400">{parseFloat(manager.season_consistency_score || 0).toFixed(3)}</span>
									</div>
								</div>
							</div>
						</div>
					</div>
				{/each}
			</div>
		</section>

		<!-- Hall of Fame Criteria -->
		<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
			<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
				<Award class="w-6 h-6 text-purple-400 mr-3" />
				Hall of Fame Criteria
			</h2>
			
			<div class="grid grid-cols-1 md:grid-cols-2 gap-8">
				<div>
					<h3 class="text-lg font-bold text-white mb-4">How the Index is Calculated</h3>
					<div class="space-y-3 text-sm text-slate-300">
						<div class="bg-slate-700/30 rounded-lg p-3 border border-slate-600/50 mb-4">
							<div class="font-bold text-cyan-400 mb-2">Hall of Fame Index Formula:</div>
							<div class="text-xs text-slate-300">
								HoF Index = (60% × Championship Score) + (40% × Win Percentage)
							</div>
						</div>
						<div class="flex items-start">
							<Trophy class="w-4 h-4 text-amber-400 mr-2 mt-0.5 flex-shrink-0" />
							<span><strong>Championships (60%):</strong> Multiple titles significantly boost ranking with diminishing returns</span>
						</div>
						<div class="flex items-start">
							<Target class="w-4 h-4 text-blue-400 mr-2 mt-0.5 flex-shrink-0" />
							<span><strong>Win Percentage (40%):</strong> Consistent success over entire career</span>
						</div>
						<div class="flex items-start">
							<TrendingUp class="w-4 h-4 text-green-400 mr-2 mt-0.5 flex-shrink-0" />
							<span><strong>Playoff Success:</strong> Making and succeeding in playoffs</span>
						</div>
						<div class="flex items-start">
							<BarChart3 class="w-4 h-4 text-purple-400 mr-2 mt-0.5 flex-shrink-0" />
							<span><strong>Consistency:</strong> Avoiding bad seasons, maintaining high performance</span>
						</div>
						<div class="flex items-start">
							<Users class="w-4 h-4 text-cyan-400 mr-2 mt-0.5 flex-shrink-0" />
							<span><strong>Longevity:</strong> Sustained excellence over multiple seasons</span>
						</div>
					</div>
				</div>
				
				<div>
					<h3 class="text-lg font-bold text-white mb-4">Tier Breakdown</h3>
					<div class="space-y-3 text-sm">
						<div class="flex items-center justify-between p-3 bg-gradient-to-r from-amber-500/20 to-yellow-400/20 rounded-lg border border-amber-500/30">
							<span class="flex items-center">
								<span class="text-lg mr-2">👑</span>
								<span class="font-medium text-white">Hall of Fame Elite</span>
							</span>
							<span class="text-amber-400 font-bold">Top 3</span>
						</div>
						<div class="flex items-center justify-between p-3 bg-gradient-to-r from-purple-500/20 to-indigo-400/20 rounded-lg border border-purple-500/30">
							<span class="flex items-center">
								<span class="text-lg mr-2">🏆</span>
								<span class="font-medium text-white">Hall of Fame</span>
							</span>
							<span class="text-purple-400 font-bold">Ranks 4-6</span>
						</div>
						<div class="flex items-center justify-between p-3 bg-gradient-to-r from-blue-500/20 to-cyan-400/20 rounded-lg border border-blue-500/30">
							<span class="flex items-center">
								<span class="text-lg mr-2">🥇</span>
								<span class="font-medium text-white">Hall of Very Good</span>
							</span>
							<span class="text-blue-400 font-bold">Ranks 7-10</span>
						</div>
						<div class="flex items-center justify-between p-3 bg-gradient-to-r from-green-500/20 to-emerald-400/20 rounded-lg border border-green-500/30">
							<span class="flex items-center">
								<span class="text-lg mr-2">🥈</span>
								<span class="font-medium text-white">Solid Contributor</span>
							</span>
							<span class="text-green-400 font-bold">Ranks 11-15</span>
						</div>
						<div class="flex items-center justify-between p-3 bg-gradient-to-r from-slate-500/20 to-gray-400/20 rounded-lg border border-slate-500/30">
							<span class="flex items-center">
								<span class="text-lg mr-2">🥉</span>
								<span class="font-medium text-white">Developing Legacy</span>
							</span>
							<span class="text-slate-400 font-bold">Ranks 16+</span>
						</div>
					</div>
				</div>
			</div>
		</section>
	{/if}
</div> 
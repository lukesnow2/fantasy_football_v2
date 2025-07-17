<script lang="ts">
	import { onMount } from 'svelte';
	import { Trophy, Target, TrendingUp, Award, User, Crown, BarChart3, Calendar, Users } from 'lucide-svelte';
	import ManagerProfilePicture from '$lib/components/ManagerProfilePicture.svelte';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	import { getTierColor, getWinPercentageColor, getChampionshipBadge, getInitials } from '$lib/utils/managerUtils';

	export let data;

	let managerData: any = null;
	let loading = true;
	let selectedManagerName = '';

	// Load manager performance data
	async function loadManagerData() {
		loading = true;
		try {
			const response = await fetch('/api/managers/performance?analysis=all');
			if (response.ok) {
				managerData = await response.json();
				console.log('Manager data loaded:', managerData);
				// Set authenticated user's manager as default, or first manager if not authenticated
				if (managerData?.data?.rankings?.length > 0) {
					const userManager = data.userManagerName;
					if (userManager && managerData.data.rankings.find((m: any) => m.managerName === userManager)) {
						selectedManagerName = userManager;
					} else {
						selectedManagerName = managerData.data.rankings[0].managerName;
					}
				} else {
					selectedManagerName = 'All Managers';
				}
			} else {
				console.error('Failed to fetch manager data:', response.status, response.statusText);
			}
		} catch (err) {
			console.error('Error fetching manager data:', err);
		} finally {
			loading = false;
		}
	}

	onMount(loadManagerData);

	$: managers = (managerData?.data?.rankings || []).filter((m: any) => m.managerName != null);
	$: currentManager = managers.find((m: any) => m.managerName === selectedManagerName) || managers[0];
	
	// Debug: Log what we're getting
	$: if (managerData?.data?.rankings) {
		console.log('Raw rankings data:', managerData.data.rankings);
		console.log('Filtered managers:', managers);
		console.log('Current manager:', currentManager);
	}

	function selectManager(managerName: string) {
		selectedManagerName = managerName;
	}

	function getManagerAchievements(managerName: string): string[] {
		const achievements = managerData?.data?.achievements?.find(
			(a: any) => a.managerName === managerName
		);
		return achievements?.allAchievements || [];
	}
</script>

<div class="min-h-screen bg-slate-900 flex">
	{#if loading}
		<div class="flex items-center justify-center w-full h-screen">
			<div class="text-2xl text-slate-400">Loading manager profiles...</div>
		</div>
	{:else if currentManager}
		<!-- Left Sidebar - Manager Selector -->
		<div class="w-20 bg-slate-800/50 border-r border-slate-700/50 flex flex-col items-center py-6 space-y-3 overflow-y-auto">
			{#each managers as manager}
				<button
					on:click={() => selectManager(manager.managerName)}
					class="relative group"
					title={manager.managerName}
				>
					<!-- Profile Picture -->
					{#if manager.managerName}
						<ManagerProfilePicture 
							managerName={manager.managerName}
							size="small"
							className="transition-all duration-200 
								{manager.managerName === selectedManagerName 
									? 'ring-2 ring-blue-400 scale-110' 
									: 'ring-1 ring-slate-600 hover:ring-slate-500 hover:scale-105'}"
						/>
					{/if}
					

				</button>
			{/each}
		</div>

		<!-- Main Content Area -->
		<div class="flex-1 flex flex-col">
			<!-- Header -->
			<div class="bg-slate-800/30 border-b border-slate-700/50 px-8 py-6">
				<div class="flex items-center justify-between">
					<div class="flex items-center gap-6">
						<!-- Large Profile Picture -->
						{#if currentManager.managerName}
							<ManagerProfilePicture 
								managerName={currentManager.managerName}
								size="medium"
								className="ring-2 ring-slate-600"
							/>
						{/if}
						
						<!-- Manager Info -->
						<div>
							<h1 class="text-4xl font-bold text-white mb-2">
								{currentManager.managerName}
								{#if currentManager.totalChampionships > 0}
									<span class="ml-3">{getChampionshipBadge(currentManager.totalChampionships)}</span>
								{/if}
							</h1>
																			<div class="flex items-center gap-4">
								<span class="px-3 py-1 rounded-full text-sm font-semibold border {getTierColor(currentManager.tierClassification)}">
									{currentManager.tierClassification}
								</span>
								<span class="text-slate-400">
									{currentManager.firstSeason} - {currentManager.lastSeason}
								</span>
							</div>
						</div>
					</div>
					
					<!-- Quick Stats -->
					<div class="text-right">
						<div class="flex items-baseline justify-end gap-6 mb-1">
							<div class="text-3xl font-bold text-slate-300">
								<span class="text-green-400">{currentManager.totalWins || 0}</span>-<span class="text-red-400">{currentManager.totalLosses || 0}</span>-<span class="text-amber-400">{currentManager.totalTies || 0}</span>
							</div>
							<div class="text-3xl font-bold {getWinPercentageColor(parseFloat(currentManager.winPercentage || 0))}">
								{parseFloat(currentManager.winPercentage || 0).toFixed(3)}
							</div>
						</div>
						<div class="text-slate-400 text-sm">
							<MetricHelp metricId="career_win_percentage" position="bottom" theme="dark" className="text-slate-400" showIcon={false}>
								Win Rate
							</MetricHelp>
						</div>
					</div>
				</div>
			</div>

			<!-- Main Stats Grid -->
			<div class="flex-1 p-8 overflow-y-auto">
				<div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
					
					<!-- Left Column: Core Stats -->
					<div class="lg:col-span-2 space-y-6">
						
						<!-- League Tenure & Impact -->
						<div class="bg-slate-800/40 rounded-xl p-6 border border-slate-700/50">
							<h3 class="text-xl font-bold text-white mb-4 flex items-center">
								<BarChart3 class="w-5 h-5 text-blue-400 mr-2" />
								League Tenure & Impact
							</h3>
							<div class="grid grid-cols-4 gap-6">
								<div class="text-center">
									<div class="text-3xl font-bold text-purple-400">{currentManager.totalSeasons}</div>
									<div class="text-slate-400 font-medium">Seasons</div>
								</div>
								<div class="text-center">
									<div class="text-3xl font-bold text-blue-400">{currentManager.playoffAppearances || 0}</div>
									<div class="text-slate-400 font-medium">Playoffs</div>
								</div>
								<div class="text-center">
									<div class="text-3xl font-bold text-amber-400">{currentManager.totalChampionships || 0}</div>
									<div class="text-slate-400 font-medium">Championships</div>
								</div>
								<div class="text-center">
									<div class="text-3xl font-bold text-green-400">{currentManager.totalTransactions || 0}</div>
									<div class="text-slate-400 font-medium">Transactions</div>
								</div>
							</div>
						</div>

						<!-- Performance Metrics -->
						<div class="bg-slate-800/40 rounded-xl p-6 border border-slate-700/50">
							<h3 class="text-xl font-bold text-white mb-4 flex items-center">
								<Target class="w-5 h-5 text-green-400 mr-2" />
								Scoring Performance
							</h3>
							<div class="grid grid-cols-3 gap-6">
								<div class="text-center">
									<div class="text-2xl font-bold text-blue-400">{parseFloat(currentManager.avgPointsFor || 0).toFixed(1)}</div>
									<div class="text-slate-400 text-sm">Avg Points/Season</div>
								</div>
								<div class="text-center">
									<div class="text-2xl font-bold text-green-400">{parseFloat(currentManager.avgPointsPerGame || 0).toFixed(1)}</div>
									<div class="text-slate-400 text-sm">
										<MetricHelp metricId="avg_points_per_game" position="top" theme="dark" className="text-slate-400" showIcon={false}>
											Avg Points/Game
										</MetricHelp>
									</div>
								</div>
								<div class="text-center">
									<div class="text-2xl font-bold text-purple-400">{parseFloat(currentManager.totalPointsScored || 0).toLocaleString()}</div>
									<div class="text-slate-400 text-sm">Career Points</div>
								</div>
							</div>
						</div>

						<!-- League Rankings -->
						<div class="bg-slate-800/40 rounded-xl p-6 border border-slate-700/50">
							<h3 class="text-xl font-bold text-white mb-4 flex items-center">
								<Crown class="w-5 h-5 text-amber-400 mr-2" />
								League Rankings
							</h3>
							<div class="grid grid-cols-4 gap-4">
								<div class="text-center p-4 bg-slate-900/30 rounded-lg">
									<div class="text-xl font-bold text-amber-400">#{currentManager.championshipRank}</div>
									<div class="text-slate-400 text-sm">Championships</div>
								</div>
								<div class="text-center p-4 bg-slate-900/30 rounded-lg">
									<div class="text-xl font-bold text-blue-400">#{currentManager.winPctRank}</div>
									<div class="text-slate-400 text-sm">Win Rate</div>
								</div>
								<div class="text-center p-4 bg-slate-900/30 rounded-lg">
									<div class="text-xl font-bold text-green-400">#{currentManager.scoringRank}</div>
									<div class="text-slate-400 text-sm">Scoring</div>
								</div>
								<div class="text-center p-4 bg-slate-900/30 rounded-lg">
									<div class="text-xl font-bold text-purple-400">#{currentManager.playoffRank}</div>
									<div class="text-slate-400 text-sm">Playoffs</div>
								</div>
							</div>
						</div>
					</div>

					<!-- Right Column: Additional Stats -->
					<div class="space-y-6">
						
						<!-- Season Performance Range -->
						<div class="bg-slate-800/40 rounded-xl p-6 border border-slate-700/50">
							<h3 class="text-lg font-bold text-white mb-4 flex items-center">
								<TrendingUp class="w-5 h-5 text-green-400 mr-2" />
								Career Range
							</h3>
							<div class="space-y-4">
								<div class="flex justify-between items-center">
									<span class="text-slate-400">Best Season</span>
									<span class="text-lg font-bold text-green-400">{currentManager.bestSeasonRecord || 'N/A'}</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="text-slate-400">Worst Season</span>
									<span class="text-lg font-bold text-red-400">{currentManager.worstSeasonRecord || 'N/A'}</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="text-slate-400">Best Draft Year</span>
									<span class="text-lg font-bold text-blue-400">{currentManager.bestDraftYear || 'N/A'}</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="text-slate-400">Worst Draft Year</span>
									<span class="text-lg font-bold text-amber-400">{currentManager.worstDraftYear || 'N/A'}</span>
								</div>
							</div>
						</div>

						<!-- Advanced Metrics -->
						<div class="bg-slate-800/40 rounded-xl p-6 border border-slate-700/50">
							<h3 class="text-lg font-bold text-white mb-4 flex items-center">
								<TrendingUp class="w-5 h-5 text-purple-400 mr-2" />
								Advanced Metrics
							</h3>
							<div class="space-y-4">
								<div class="flex justify-between items-center">
									<span class="text-slate-400">
										<MetricHelp metricId="draft_value_index" position="left" theme="dark" className="text-slate-400" showIcon={false}>
											Draft Grade
										</MetricHelp>
									</span>
									<span class="text-lg font-bold text-green-400">{parseFloat(currentManager.avgDraftGrade || 0).toFixed(1)}</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="text-slate-400">
										<MetricHelp metricId="faab_efficiency" position="left" theme="dark" className="text-slate-400" showIcon={false}>
											FAAB Efficiency
										</MetricHelp>
									</span>
									<span class="text-lg font-bold text-blue-400">{parseFloat(currentManager.faabEfficiencyRating || 0).toFixed(1)}</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="text-slate-400">
										<MetricHelp metricId="season_consistency_score" position="left" theme="dark" className="text-slate-400" showIcon={false}>
											Consistency Score
										</MetricHelp>
									</span>
									<span class="text-lg font-bold text-purple-400">{parseFloat(currentManager.seasonConsistencyScore || 0).toFixed(1)}</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="text-slate-400">Avg Transactions</span>
									<span class="text-lg font-bold text-amber-400">{parseFloat(currentManager.avgTransactionsPerSeason || 0).toFixed(1)}</span>
								</div>
							</div>
						</div>

						<!-- Career Highlights -->
						<div class="bg-slate-800/40 rounded-xl p-6 border border-slate-700/50">
							<h3 class="text-lg font-bold text-white mb-4 flex items-center">
								<Award class="w-5 h-5 text-yellow-400 mr-2" />
								Career Highlights
							</h3>
							<div class="space-y-3">
								<div class="flex justify-between items-center">
									<span class="text-slate-400">Best Season</span>
									<span class="text-sm font-bold text-green-400">{currentManager.bestSeasonRecord || 'N/A'}</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="text-slate-400">Best Draft Year</span>
									<span class="text-sm font-bold text-blue-400">{currentManager.bestDraftYear || 'N/A'}</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="text-slate-400">Total Transactions</span>
									<span class="text-sm font-bold text-purple-400">{currentManager.totalTransactions || 0}</span>
								</div>
							</div>
						</div>

						<!-- Achievements -->
						{#if currentManager}
							{@const achievements = getManagerAchievements(currentManager.managerName)}
							{#if achievements.length > 0}
								<div class="bg-slate-800/40 rounded-xl p-6 border border-slate-700/50">
									<h3 class="text-lg font-bold text-white mb-4">Achievements</h3>
									<div class="flex flex-wrap gap-2">
										{#each achievements as achievement}
											<span class="px-2 py-1 bg-amber-400/10 text-amber-400 rounded-md border border-amber-400/20 text-xs font-medium">
												{achievement}
											</span>
										{/each}
									</div>
								</div>
							{/if}
						{/if}
					</div>
				</div>

				<!-- View Full Profile Button -->
				<div class="text-center mt-8">
					<button 
						class="bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white font-bold py-3 px-6 rounded-lg transition-all transform hover:scale-105 shadow-lg"
						on:click={() => window.location.href = `/managers/${encodeURIComponent(currentManager.managerName)}`}
					>
						View Detailed Profile & History
					</button>
				</div>
			</div>
		</div>
	{:else}
		<div class="flex items-center justify-center w-full h-screen">
			<div class="text-2xl text-slate-400">No manager data available</div>
		</div>
	{/if}
</div>

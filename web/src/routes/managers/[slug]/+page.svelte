<script lang="ts">
	import { page } from '$app/stores';
	import { onMount } from 'svelte';
	import { Crown, Trophy, TrendingUp, Users, Target, Calendar, BarChart3, Award, ArrowLeft } from 'lucide-svelte';
	import MetricHelp from '$lib/components/MetricHelp.svelte';

	let managerData: any = null;
	let loading = true;
	let selectedTab = 'overview';

	// Get manager name from URL
	$: managerName = decodeURIComponent($page.params.slug);

	// Load manager specific data
	async function loadManagerData() {
		loading = true;
		try {
			const response = await fetch(`/api/managers/performance?manager=${encodeURIComponent(managerName)}&analysis=all`);
			if (response.ok) {
				managerData = await response.json();
				console.log('Manager profile loaded:', managerData);
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

	// Helper functions
	function getTierColor(tier: string): string {
		switch (tier) {
			case 'League Legend': return 'text-amber-400 bg-amber-400/10 border-amber-400/20';
			case 'Championship Contender': return 'text-purple-400 bg-purple-400/10 border-purple-400/20';
			case 'Playoff Regular': return 'text-blue-400 bg-blue-400/10 border-blue-400/20';
			case 'Mid-Tier Manager': return 'text-green-400 bg-green-400/10 border-green-400/20';
			case 'Rebuilding Mode': return 'text-red-400 bg-red-400/10 border-red-400/20';
			default: return 'text-gray-400 bg-gray-400/10 border-gray-400/20';
		}
	}

	function getWinPercentageColor(winPct: number): string {
		if (winPct >= 0.70) return 'text-green-400';
		if (winPct >= 0.60) return 'text-blue-400';
		if (winPct >= 0.50) return 'text-amber-400';
		return 'text-red-400';
	}

	function getChampionshipBadge(championships: number): string {
		if (championships >= 3) return '👑';
		if (championships >= 2) return '🏆🏆';
		if (championships >= 1) return '🏆';
		return '';
	}

	function getSeasonOutcome(season: any): string {
		if (season.championship_winner) return '🏆 Champion';
		if (season.made_playoffs) return '🔥 Playoffs';
		return '📉 Missed Playoffs';
	}

	function getSeasonOutcomeColor(season: any): string {
		if (season.championship_winner) return 'text-amber-400';
		if (season.made_playoffs) return 'text-blue-400';
		return 'text-red-400';
	}

	// Get manager's main data
	$: manager = managerData?.data?.performance?.[0];
	$: ranking = managerData?.data?.rankings?.[0];
	$: achievements = managerData?.data?.achievements?.[0]?.all_achievements || [];

	const tabs = [
		{ id: 'overview', name: 'Overview', icon: BarChart3 },
		{ id: 'seasons', name: 'Season History', icon: Calendar },
		{ id: 'head-to-head', name: 'Head to Head', icon: Users },
		{ id: 'achievements', name: 'Achievements', icon: Award }
	];
</script>

<div class="space-y-8">
	<!-- Back Button -->
	<div>
		<button 
			on:click={() => window.history.back()}
			class="flex items-center text-slate-300 hover:text-white transition-colors"
		>
			<ArrowLeft class="w-4 h-4 mr-2" />
			Back to Manager Profiles
		</button>
	</div>

	{#if loading}
		<div class="flex items-center justify-center h-64">
			<div class="text-slate-400">Loading {managerName}'s profile...</div>
		</div>
	{:else if manager}
		<!-- Profile Header -->
		<div class="bg-slate-800/50 rounded-xl p-8 border border-slate-700/50">
			<div class="flex flex-col md:flex-row items-start md:items-center gap-6">
				<div class="w-24 h-24 bg-gradient-to-br from-blue-500 to-purple-600 rounded-full flex items-center justify-center font-bold text-white text-3xl">
					{manager.manager_name.charAt(0)}
				</div>
				
				<div class="flex-1">
					<div class="flex items-center gap-4 mb-2">
						<h1 class="text-4xl font-bold text-white">{manager.manager_name}</h1>
						{#if manager.total_championships > 0}
							<span class="text-3xl">
								{getChampionshipBadge(manager.total_championships)}
							</span>
						{/if}
					</div>
					
					{#if ranking}
						<div class="flex items-center gap-4 mb-4">
							<span class="text-lg px-3 py-1 rounded-full border {getTierColor(ranking.tier_classification)}">
								{ranking.tier_classification}
							</span>
							<span class="text-slate-300">
								Ranked #{ranking.overall_rank} Overall
							</span>
						</div>
					{/if}

					<div class="grid grid-cols-2 md:grid-cols-4 gap-6">
						<div class="text-center">
							<div class="text-2xl font-bold text-blue-400">{manager.total_championships}</div>
							<div class="text-sm text-slate-400">Championships</div>
						</div>
						<div class="text-center">
							<div class="text-2xl font-bold {getWinPercentageColor(manager.win_percentage)}">
								{(manager.win_percentage * 100).toFixed(1)}%
							</div>
							<div class="text-sm text-slate-400">
								<MetricHelp metricId="career_win_percentage" position="bottom" theme="dark" className="text-slate-400" showIcon={false}>
									Win Rate
								</MetricHelp>
							</div>
						</div>
						<div class="text-center">
							<div class="text-2xl font-bold text-green-400">{manager.avg_points_for?.toFixed(1)}</div>
							<div class="text-sm text-slate-400">
								<MetricHelp metricId="avg_points_per_game" position="bottom" theme="dark" className="text-slate-400" showIcon={false}>
									Avg Points
								</MetricHelp>
							</div>
						</div>
						<div class="text-center">
							<div class="text-2xl font-bold text-purple-400">{manager.total_seasons}</div>
							<div class="text-sm text-slate-400">Seasons</div>
						</div>
					</div>
				</div>
			</div>
		</div>

		<!-- Achievements Banner -->
		{#if achievements.length > 0}
			<div class="bg-gradient-to-r from-amber-400/10 to-amber-600/5 border border-amber-400/20 rounded-xl p-6">
				<h3 class="text-xl font-bold text-amber-400 mb-4">🏅 Achievements</h3>
				<div class="flex flex-wrap gap-2">
					{#each achievements as achievement}
						<span class="px-3 py-2 bg-amber-400/10 text-amber-400 rounded-lg border border-amber-400/20 font-medium">
							{achievement}
						</span>
					{/each}
				</div>
			</div>
		{/if}

		<!-- Tab Navigation -->
		<div class="flex flex-wrap justify-center gap-2 bg-slate-800/30 p-2 rounded-xl">
			{#each tabs as tab}
				<button
					class="flex items-center px-6 py-3 rounded-lg font-medium transition-all
						{selectedTab === tab.id 
							? 'bg-amber-500 text-black' 
							: 'text-slate-300 hover:text-white hover:bg-slate-700/50'}"
					on:click={() => selectedTab = tab.id}
				>
					<svelte:component this={tab.icon} class="w-4 h-4 mr-2" />
					{tab.name}
				</button>
			{/each}
		</div>

		<!-- Tab Content -->
		{#if selectedTab === 'overview'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
				<!-- Career Stats -->
				<div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
					<h3 class="text-xl font-bold text-white mb-4">Career Statistics</h3>
					<div class="space-y-4">
						<div class="flex justify-between">
							<span class="text-slate-400">Total Record</span>
							<span class="font-bold text-white">{manager.total_wins}-{manager.total_losses}-{manager.total_ties}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Playoff Appearances</span>
							<span class="font-bold text-blue-400">{manager.total_playoff_appearances}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Playoff Win %</span>
							<span class="font-bold {getWinPercentageColor(manager.playoff_win_percentage)}">
								{(manager.playoff_win_percentage * 100).toFixed(1)}%
							</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Career Points For</span>
							<span class="font-bold text-green-400">{manager.career_points_for?.toLocaleString()}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Career Points Against</span>
							<span class="font-bold text-red-400">{manager.career_points_against?.toLocaleString()}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Point Differential</span>
							<span class="font-bold {manager.point_differential >= 0 ? 'text-green-400' : 'text-red-400'}">
								{manager.point_differential >= 0 ? '+' : ''}{manager.point_differential?.toFixed(1)}
							</span>
						</div>
					</div>
				</div>

				<!-- Streaks & Records -->
				<div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
					<h3 class="text-xl font-bold text-white mb-4">Records & Streaks</h3>
					<div class="space-y-4">
						<div class="flex justify-between">
							<span class="text-slate-400">Longest Win Streak</span>
							<span class="font-bold text-green-400">{manager.longest_win_streak} games</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Longest Loss Streak</span>
							<span class="font-bold text-red-400">{manager.longest_loss_streak} games</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Best Season Record</span>
							<span class="font-bold text-white">{manager.best_season_record}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Worst Season Record</span>
							<span class="font-bold text-slate-400">{manager.worst_season_record}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Best Season Points</span>
							<span class="font-bold text-green-400">{manager.best_season_points?.toFixed(1)}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Worst Season Points</span>
							<span class="font-bold text-red-400">{manager.worst_season_points?.toFixed(1)}</span>
						</div>
					</div>
				</div>
			</div>
		{:else if selectedTab === 'seasons'}
			<div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h3 class="text-xl font-bold text-white mb-6">Season-by-Season History</h3>
				{#if managerData?.data?.seasons}
					<div class="overflow-x-auto">
						<table class="w-full text-sm">
							<thead>
								<tr class="border-b border-slate-700">
									<th class="text-left py-3 px-4 text-slate-300">Season</th>
									<th class="text-center py-3 px-4 text-slate-300">Record</th>
									<th class="text-center py-3 px-4 text-slate-300">Points</th>
									<th class="text-center py-3 px-4 text-slate-300">Rank</th>
									<th class="text-center py-3 px-4 text-slate-300">Outcome</th>
								</tr>
							</thead>
							<tbody>
								{#each managerData.data.seasons as season}
									<tr class="border-b border-slate-700/50">
										<td class="py-3 px-4 font-bold text-white">{season.season_year}</td>
										<td class="text-center py-3 px-4">
											<span class="font-mono {getWinPercentageColor(season.win_percentage)}">
												{season.wins}-{season.losses}-{season.ties}
											</span>
										</td>
										<td class="text-center py-3 px-4 text-green-400">{season.points_for?.toFixed(1)}</td>
										<td class="text-center py-3 px-4 text-slate-300">#{season.final_rank}</td>
										<td class="text-center py-3 px-4 {getSeasonOutcomeColor(season)}">
											{getSeasonOutcome(season)}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>
		{:else if selectedTab === 'head-to-head'}
			<div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h3 class="text-xl font-bold text-white mb-6">Head-to-Head Records</h3>
				{#if managerData?.data?.head_to_head}
					<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
						{#each managerData.data.head_to_head as h2h}
							<div class="bg-slate-700/30 rounded-lg p-4">
								<div class="flex justify-between items-center mb-2">
									<span class="font-bold text-white">vs {h2h.opponent}</span>
									<span class="text-sm text-slate-400">{h2h.total_matchups} games</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="font-mono {getWinPercentageColor(h2h.win_percentage)}">
										{h2h.wins}-{h2h.losses}
									</span>
									<span class="text-sm {getWinPercentageColor(h2h.win_percentage)}">
										{(h2h.win_percentage * 100).toFixed(1)}%
									</span>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		{:else if selectedTab === 'achievements'}
			<div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h3 class="text-xl font-bold text-white mb-6">All Achievements & Recognition</h3>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
					<div>
						<h4 class="text-lg font-semibold text-amber-400 mb-3">🏆 Championship History</h4>
						{#if manager.championship_years}
							<div class="space-y-2">
								{#each manager.championship_years.split(',') as year}
									<div class="flex items-center gap-2">
										<Trophy class="w-4 h-4 text-amber-400" />
										<span class="text-white">{year.trim()} Champion</span>
									</div>
								{/each}
							</div>
						{:else}
							<div class="text-slate-400">No championships yet</div>
						{/if}
					</div>

					<div>
						<h4 class="text-lg font-semibold text-blue-400 mb-3">📊 Category Rankings</h4>
						{#if ranking}
							<div class="space-y-2">
								<div class="flex justify-between">
									<span class="text-slate-400">Overall Rank</span>
									<span class="font-bold text-white">#{ranking.overall_rank}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Championships</span>
									<span class="font-bold text-amber-400">#{ranking.championship_rank}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Win Percentage</span>
									<span class="font-bold text-blue-400">#{ranking.win_pct_rank}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Scoring</span>
									<span class="font-bold text-green-400">#{ranking.scoring_rank}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Playoffs</span>
									<span class="font-bold text-purple-400">#{ranking.playoff_rank}</span>
								</div>
							</div>
						{/if}
					</div>
				</div>
			</div>
		{/if}
	{:else}
		<div class="text-center py-8">
			<div class="text-slate-400">Manager profile not found</div>
		</div>
	{/if}
</div>

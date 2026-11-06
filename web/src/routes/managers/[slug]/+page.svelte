<script lang="ts">
	import { page } from '$app/stores';
	import { onMount } from 'svelte';
	import { Crown, Trophy, TrendingUp, Users, Target, Calendar, BarChart3, Award, ArrowLeft } from 'lucide-svelte';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	import ManagerLuckCard from '$lib/components/ManagerLuckCard.svelte';
	import { formatLuck, getLuckColor } from '$lib/utils/managerUtils';

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

	// A season still being played has no outcome yet: "missed the playoffs"
	// was only ever the absence of a playoff flag, which no team has before
	// the bracket is set.
	// isChampion is per team: in a year a manager ran two teams, only the team
	// that won the title is the champion. champYears is the fallback.
	function wonTitle(season: any, champYears: number[]): boolean {
		return season.isChampion ?? champYears.includes(Number(season.seasonYear));
	}

	function getSeasonOutcome(season: any, champYears: number[]): string {
		if (wonTitle(season, champYears)) return '🏆 Champion';
		if (season.inProgress) {
			// Playoffs under way: the bracket is set, so say where the team stands.
			if (season.bracketSet) {
				return season.madePlayoffs ? '🔥 Playoffs · in progress' : '📉 Missed Playoffs';
			}
			return season.playoffOdds != null
				? `⏳ In progress · ${Math.round(season.playoffOdds * 100)}% playoff odds`
				: '⏳ In progress';
		}
		if (season.madePlayoffs) return '🔥 Playoffs';
		return '📉 Missed Playoffs';
	}

	function getSeasonOutcomeColor(season: any, champYears: number[]): string {
		if (wonTitle(season, champYears)) return 'text-amber-400';
		if (season.inProgress && !season.bracketSet) return 'text-slate-300';
		if (season.madePlayoffs) return 'text-blue-400';
		return 'text-red-400';
	}


	// Get manager's main data
	$: manager = managerData?.data?.performance?.[0];
	$: ranking = managerData?.data?.rankings?.[0];
	$: achievements = managerData?.data?.achievements?.[0]?.allAchievements || [];
	$: seasons = managerData?.data?.seasons ?? [];
	$: championshipSeasons = managerData?.data?.championshipSeasons ?? [];

	// Fields the mart doesn't carry, derived from the per-season breakdown.
	$: careerPointsFor = manager?.totalPointsScored
		? parseFloat(manager.totalPointsScored)
		: seasons.reduce((s: number, x: any) => s + (parseFloat(x.pointsFor) || 0), 0);
	$: careerPointsAgainst = seasons.reduce((s: number, x: any) => s + (parseFloat(x.pointsAgainst) || 0), 0);
	$: pointDifferential = careerPointsFor - careerPointsAgainst;
	// Finished seasons only: a season three weeks old is not anyone's worst.
	$: finishedSeasons = seasons.filter((x: any) => !x.inProgress);
	$: bestSeasonPoints = finishedSeasons.length
		? Math.max(...finishedSeasons.map((x: any) => parseFloat(x.pointsFor) || 0))
		: null;
	$: worstSeasonPoints = finishedSeasons.length
		? Math.min(...finishedSeasons.map((x: any) => parseFloat(x.pointsFor) || 0))
		: null;
	$: streaks = managerData?.data?.streaks ?? null;

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
		<!-- A real link, not history.back(): this page is deep-linkable from the nav
		     and from a shared URL, and going "back" from those left the app. -->
		<a
			href="/managers"
			class="flex items-center text-slate-300 hover:text-white transition-colors"
		>
			<ArrowLeft class="w-4 h-4 mr-2" />
			Back to Manager Profiles
		</a>
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
					<span class="text-4xl font-bold text-white">
						{manager.managerName.charAt(0)}
					</span>
				</div>
				
				<div class="flex-1">
					<div class="flex items-center gap-4 mb-2">
						<h1 class="text-4xl font-bold text-white">{manager.managerName}</h1>
						{#if manager.totalChampionships > 0}
							<span class="text-3xl">
								{getChampionshipBadge(manager.totalChampionships)}
							</span>
						{/if}
					</div>
					
					{#if ranking}
						<div class="flex items-center gap-4 mb-4">
							<span class="text-lg px-3 py-1 rounded-full border {getTierColor(ranking.tierClassification)}">
								{ranking.tierClassification}
							</span>
							<span class="text-slate-300">
								Ranked #{ranking.overallRank} Overall
							</span>
						</div>
					{/if}

					<div class="grid grid-cols-2 md:grid-cols-4 gap-6">
						<div class="text-center">
							<div class="text-2xl font-bold text-blue-400">{manager.totalChampionships}</div>
							<div class="text-sm text-slate-400">Championships</div>
						</div>
						<div class="text-center">
							<div class="text-2xl font-bold {getWinPercentageColor(manager.winPercentage)}">
								{(manager.winPercentage * 100).toFixed(1)}%
							</div>
							<div class="text-sm text-slate-400">
								<MetricHelp metricId="career_win_percentage" position="bottom" theme="dark" className="text-slate-400" showIcon={false}>
									Win Rate
								</MetricHelp>
							</div>
						</div>
						<div class="text-center">
							<!-- avgPointsPerGame, not avgPointsFor: the latter is points per
							     season (~1800), which does not belong under a per-game label. -->
							<div class="text-2xl font-bold text-green-400">
								{manager.avgPointsPerGame != null ? manager.avgPointsPerGame.toFixed(1) : '—'}
							</div>
							<div class="text-sm text-slate-400">
								<MetricHelp metricId="points_per_week" position="bottom" theme="dark" className="text-slate-400" showIcon={false}>
									Avg Points
								</MetricHelp>
							</div>
						</div>
						<div class="text-center">
							<div class="text-2xl font-bold text-purple-400">{manager.totalSeasons}</div>
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
							<span class="font-bold text-white">{manager.totalWins}-{manager.totalLosses}-{manager.totalTies}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Playoff Appearances</span>
							<span class="font-bold text-blue-400">{manager.playoffAppearances}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Playoff Win %</span>
							<span class="font-bold {getWinPercentageColor(manager.playoffWinPercentage)}">
								{(manager.playoffWinPercentage * 100).toFixed(1)}%
							</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Career Points For</span>
							<span class="font-bold text-green-400">{Math.round(careerPointsFor).toLocaleString()}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Career Points Against</span>
							<span class="font-bold text-red-400">{Math.round(careerPointsAgainst).toLocaleString()}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Point Differential</span>
							<span class="font-bold {pointDifferential >= 0 ? 'text-green-400' : 'text-red-400'}">
								{pointDifferential >= 0 ? '+' : ''}{pointDifferential.toFixed(1)}
							</span>
						</div>
					</div>
				</div>

				<!-- Streaks & Records -->
				<div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
					<h3 class="text-xl font-bold text-white mb-4">Season Records</h3>
					<div class="space-y-4">
						<div class="flex justify-between">
							<span class="text-slate-400">Best Season Record</span>
							<span class="font-bold text-white">{manager.bestSeasonRecord}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Worst Season Record</span>
							<span class="font-bold text-slate-400">{manager.worstSeasonRecord}</span>
						</div>
						{#if streaks}
							<div class="flex justify-between">
								<span class="text-slate-400">Longest Win Streak</span>
								<span class="font-bold text-green-400">{streaks.longestWinStreak} games</span>
							</div>
							<div class="flex justify-between">
								<span class="text-slate-400">Longest Losing Streak</span>
								<span class="font-bold text-red-400">{streaks.longestLossStreak} games</span>
							</div>
						{/if}
						<div class="flex justify-between">
							<span class="text-slate-400">Best Season Points</span>
							<span class="font-bold text-green-400">{bestSeasonPoints != null ? bestSeasonPoints.toFixed(1) : '—'}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Worst Season Points</span>
							<span class="font-bold text-red-400">{worstSeasonPoints != null ? worstSeasonPoints.toFixed(1) : '—'}</span>
						</div>
						<div class="flex justify-between">
							<span class="text-slate-400">Total Transactions</span>
							<span class="font-bold text-amber-400">{manager.totalTransactions ?? '—'}</span>
						</div>
					</div>
				</div>
			</div>
			<ManagerLuckCard {manager} />
		{:else if selectedTab === 'seasons'}
			<div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h3 class="text-xl font-bold text-white mb-6">Season-by-Season History</h3>
				{#if seasons.length > 0}
					<div class="overflow-x-auto">
						<table class="w-full text-sm">
							<thead>
								<tr class="border-b border-slate-700">
									<th class="text-left py-3 px-4 text-slate-300">Season</th>
									<th class="text-center py-3 px-4 text-slate-300">Record</th>
									<th class="text-center py-3 px-4 text-slate-300">Points For</th>
									<th class="text-center py-3 px-4 text-slate-300">Points Against</th>
									<th class="text-center py-3 px-4 text-slate-300">
										<MetricHelp metricId="pythagorean_wins" position="bottom" theme="dark" className="text-slate-300" showIcon={false}>
											Reg. W / Expected
										</MetricHelp>
									</th>
									<th class="text-center py-3 px-4 text-slate-300">
										<MetricHelp metricId="luck_factor" position="bottom" theme="dark" className="text-slate-300" showIcon={false}>
											Luck
										</MetricHelp>
									</th>
									<th class="text-center py-3 px-4 text-slate-300">Outcome</th>
								</tr>
							</thead>
							<tbody>
								{#each seasons as season}
									<tr class="border-b border-slate-700/50">
										<td class="py-3 px-4">
											<div class="font-bold text-white">{season.seasonYear}</div>
											{#if season.teamName}<div class="text-xs text-slate-400">{season.teamName}</div>{/if}
										</td>
										<td class="text-center py-3 px-4">
											<span class="font-mono {getWinPercentageColor(season.winPercentage)}">
												{season.wins}-{season.losses}-{season.ties}
											</span>
										</td>
										<td class="text-center py-3 px-4 text-green-400">{parseFloat(season.pointsFor).toFixed(1)}</td>
										<td class="text-center py-3 px-4 text-red-400">{parseFloat(season.pointsAgainst).toFixed(1)}</td>
										<td class="text-center py-3 px-4 text-slate-200 tabular-nums">
											{season.pythagoreanWins != null
												? `${season.regularSeasonWins} / ${season.pythagoreanWins.toFixed(1)}`
												: '—'}
										</td>
										<td class="text-center py-3 px-4 tabular-nums {getLuckColor(season.luckFactor)}">
											{formatLuck(season.luckFactor)}
										</td>
										<td class="text-center py-3 px-4 {getSeasonOutcomeColor(season, championshipSeasons)}">
											{getSeasonOutcome(season, championshipSeasons)}
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
				{#if managerData?.data?.head_to_head?.length}
					<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
						{#each managerData.data.head_to_head as h2h}
							<div class="bg-slate-700/30 rounded-lg p-4">
								<div class="flex justify-between items-center mb-2">
									<span class="font-bold text-white">vs {h2h.opponent}</span>
									<span class="text-sm text-slate-400">{h2h.totalMatchups} games</span>
								</div>
								<div class="flex justify-between items-center">
									<span class="font-mono {getWinPercentageColor(h2h.winPercentage)}">
										{h2h.wins}-{h2h.losses}
									</span>
									<span class="text-sm {getWinPercentageColor(h2h.winPercentage)}">
										{(h2h.winPercentage * 100).toFixed(1)}%
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
						{#if championshipSeasons.length > 0}
							<div class="space-y-2">
								{#each championshipSeasons as year}
									<div class="flex items-center gap-2">
										<Trophy class="w-4 h-4 text-amber-400" />
										<span class="text-white">{year} Champion</span>
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
									<span class="font-bold text-white">#{ranking.overallRank}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Championships</span>
									<span class="font-bold text-amber-400">#{ranking.championshipRank}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Win Percentage</span>
									<span class="font-bold text-blue-400">#{ranking.winPctRank}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Scoring</span>
									<span class="font-bold text-green-400">#{ranking.scoringRank}</span>
								</div>
								<div class="flex justify-between">
									<span class="text-slate-400">Playoffs</span>
									<span class="font-bold text-purple-400">#{ranking.playoffRank}</span>
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

<script lang="ts">
	import { onMount } from 'svelte';
	import { Target, TrendingUp, Award, Crown, Users } from 'lucide-svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import ManagerProfilePicture from '$lib/components/ManagerProfilePicture.svelte';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	import ManagerLuckCard from '$lib/components/ManagerLuckCard.svelte';
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

<svelte:head>
	<title>Managers - The League</title>
</svelte:head>

<div class="space-y-8">
	<PageHeader icon={Users} title="Managers">
		Career records, championships and season-by-season history for every manager in the league.
	</PageHeader>

	<!-- The selector/profile split is this page's own layout, but it now sits in
	     the same panel treatment every other page uses instead of bleeding to the
	     window edges. -->
	<div
		class="flex bg-slate-800/50 rounded-xl border border-slate-700/50 overflow-hidden min-h-[32rem]"
	>
	{#if loading}
		<div class="flex items-center justify-center w-full h-64">
			<div class="text-slate-400">Loading manager profiles...</div>
		</div>
	{:else if currentManager}
		<!-- Left Sidebar - Manager Selector (desktop) -->
		<div class="hidden sm:flex w-20 bg-slate-800/50 border-r border-slate-700/50 flex-col items-center py-6 space-y-3 overflow-y-auto">
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
		<div class="flex-1 flex flex-col min-w-0">
			<!-- Mobile Manager Selector -->
			<div class="sm:hidden p-4 border-b border-slate-700/50">
				<label for="mobile-manager-select" class="sr-only">Select manager</label>
				<select
					id="mobile-manager-select"
					bind:value={selectedManagerName}
					class="w-full bg-slate-700 text-white px-4 py-3 rounded-lg border border-slate-600 focus:border-blue-400 focus:outline-none text-base font-medium"
				>
					{#each managers as manager}
						<option value={manager.managerName}>{manager.managerName}</option>
					{/each}
				</select>
			</div>

			<!-- Header -->
			<div class="bg-slate-800/30 border-b border-slate-700/50 px-4 sm:px-8 py-6">
				<div class="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
					<div class="flex items-center gap-4 sm:gap-6 min-w-0">
						<!-- Large Profile Picture -->
						{#if currentManager.managerName}
						<ManagerProfilePicture 
								managerName={currentManager.managerName}
							size="medium"
							className="ring-2 ring-slate-600 shrink-0"
						/>
						{/if}
						
						<!-- Manager Info -->
						<div class="min-w-0">
							<h1 class="text-3xl sm:text-4xl font-bold text-white mb-2 break-words">
								{currentManager.managerName}
								{#if currentManager.totalChampionships > 0}
									<span class="ml-3">{getChampionshipBadge(currentManager.totalChampionships)}</span>
								{/if}
							</h1>
							<div class="flex flex-wrap items-center gap-x-4 gap-y-2">
								<span class="px-3 py-1 rounded-full text-sm font-semibold border {getTierColor(currentManager.tierClassification)}">
									{currentManager.tierClassification}
								</span>
								<span class="text-slate-400">{currentManager.firstSeason} – {currentManager.lastSeason}</span>
								{#each getManagerAchievements(currentManager.managerName) as achievement}
									<span class="px-2 py-0.5 bg-amber-400/10 text-amber-300 rounded-md border border-amber-400/20 text-xs font-medium">{achievement}</span>
								{/each}
							</div>
						</div>
					</div>
					
					<!-- Quick Stats -->
					<div class="sm:text-right shrink-0">
						<div class="flex items-baseline gap-6 mb-1 sm:justify-end">
							<div class="text-2xl sm:text-3xl font-bold text-slate-300">
								<span class="text-green-400">{currentManager.totalWins || 0}</span>-<span class="text-red-400">{currentManager.totalLosses || 0}</span>-<span class="text-amber-400">{currentManager.totalTies || 0}</span>
							</div>
							<div class="text-2xl sm:text-3xl font-bold {getWinPercentageColor(parseFloat(currentManager.winPercentage || 0))}">
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

			<!-- Body. Ordered by what a visitor asks first: the headline numbers,
			     then how this manager's record compares with their scoring (career
			     and this season), then four equal cards. It used to be a 2:1 split
			     whose narrow right column ran ~900px past the left one, leaving a
			     block of empty space beside it, and repeated three of its own stats
			     in a "Career Highlights" card. -->
			<div class="flex-1 space-y-6 overflow-y-auto p-4 sm:p-8">
				<!-- Headline numbers -->
				<div class="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
					{#each [
						{ label: 'Seasons', value: currentManager.totalSeasons ?? '—' },
						{ label: 'Playoffs', value: currentManager.playoffAppearances ?? 0 },
						{ label: 'Championships', value: currentManager.totalChampionships ?? 0, accent: 'text-amber-400' },
						{ label: 'Points / game', value: currentManager.avgPointsPerGame != null ? currentManager.avgPointsPerGame.toFixed(1) : '—', help: 'points_per_week' },
						{ label: 'Career points', value: currentManager.totalPointsScored != null ? Math.round(currentManager.totalPointsScored).toLocaleString() : '—' },
						{ label: 'Playoff win %', value: currentManager.playoffWinPercentage != null ? `${(currentManager.playoffWinPercentage * 100).toFixed(1)}%` : '—' }
					] as stat}
						<div class="rounded-xl border border-slate-700/50 bg-slate-800/40 px-4 py-3">
							<div class="text-2xl font-bold tabular-nums {stat.accent ?? 'text-white'}">{stat.value}</div>
							<div class="text-xs text-slate-400">
								{#if stat.help}
									<MetricHelp metricId={stat.help} position="bottom" theme="dark" className="text-slate-400" showIcon={false}>{stat.label}</MetricHelp>
								{:else}
									{stat.label}
								{/if}
							</div>
						</div>
					{/each}
				</div>

				<ManagerLuckCard manager={currentManager} />

				<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
					<!-- League Rankings -->
					<section class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-6">
						<h3 class="mb-4 flex items-center text-lg font-bold text-white">
							<Crown class="mr-2 h-5 w-5 text-amber-400" />
							League Rankings
						</h3>
						<div class="grid grid-cols-2 gap-3">
							{#each [
								{ label: 'Championships', rank: currentManager.championshipRank },
								{ label: 'Win rate', rank: currentManager.winPctRank },
								{ label: 'Scoring', rank: currentManager.scoringRank },
								{ label: 'Playoff win rate', rank: currentManager.playoffRank }
							] as r}
								<div class="rounded-lg bg-slate-900/30 px-4 py-3">
									<div class="text-xl font-bold text-white tabular-nums">#{r.rank}</div>
									<div class="text-sm text-slate-400">{r.label}</div>
								</div>
							{/each}
						</div>
					</section>

					<!-- Seasons -->
					<section class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-6">
						<h3 class="mb-4 flex items-center text-lg font-bold text-white">
							<TrendingUp class="mr-2 h-5 w-5 text-green-400" />
							Seasons
						</h3>
						<dl class="space-y-3">
							<div class="flex items-center justify-between gap-4">
								<dt class="text-slate-400">Best season</dt>
								<dd class="font-bold text-green-400 tabular-nums">{currentManager.bestSeasonRecord || '—'}</dd>
							</div>
							<div class="flex items-center justify-between gap-4">
								<dt class="text-slate-400">Worst season</dt>
								<dd class="font-bold text-red-400 tabular-nums">{currentManager.worstSeasonRecord || '—'}</dd>
							</div>
							<div class="flex items-center justify-between gap-4">
								<dt class="text-slate-400">Avg points / season</dt>
								<dd class="font-bold text-white tabular-nums">{currentManager.avgPointsFor != null ? currentManager.avgPointsFor.toFixed(1) : '—'}</dd>
							</div>
							<div class="flex items-center justify-between gap-4">
								<!-- A standard deviation of season win %, so lower is steadier;
								     three places, since every manager sits between .11 and .19
								     (it showed "0.2" at one place). -->
								<dt class="text-slate-400">
									<MetricHelp metricId="season_consistency_score" position="left" theme="dark" className="text-slate-400" showIcon={false}>
										Year-to-year swing
									</MetricHelp>
								</dt>
								<dd class="font-bold text-white tabular-nums">
									{currentManager.seasonConsistencyScore != null ? `±${currentManager.seasonConsistencyScore.toFixed(3).replace(/^0/, '')}` : '—'}
								</dd>
							</div>
						</dl>
					</section>

					<!-- Waiver Wire -->
					<section class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-6">
						<h3 class="mb-4 flex items-center text-lg font-bold text-white">
							<Target class="mr-2 h-5 w-5 text-blue-400" />
							Waiver Wire
						</h3>
						<dl class="space-y-3">
							<div class="flex items-center justify-between gap-4">
								<dt class="text-slate-400">
									<MetricHelp metricId="faab_efficiency_rating" position="left" theme="dark" className="text-slate-400" showIcon={false}>
										FAAB efficiency
									</MetricHelp>
								</dt>
								<!-- Starter points per FAAB dollar. A dash, not 0.0, for a manager
								     who never paid for a claim (the league has used FAAB since 2022). -->
								<dd class="font-bold text-white tabular-nums">
									{#if currentManager.faabEfficiencyRating != null}
										{currentManager.faabEfficiencyRating.toFixed(2)}<span class="ml-1 text-xs font-medium text-slate-400">pts/$</span>
									{:else}—{/if}
								</dd>
							</div>
							<div class="flex items-center justify-between gap-4">
								<dt class="text-slate-400">Transactions / season</dt>
								<dd class="font-bold text-white tabular-nums">{currentManager.avgTransactionsPerSeason != null ? currentManager.avgTransactionsPerSeason.toFixed(1) : '—'}</dd>
							</div>
							<div class="flex items-center justify-between gap-4">
								<dt class="text-slate-400">Total transactions</dt>
								<dd class="font-bold text-white tabular-nums">{currentManager.totalTransactions ?? '—'}</dd>
							</div>
						</dl>
					</section>

					<!-- Draft. "Draft Grade" used to sit here: it was the average overall
					     pick number (75-83 for everyone), which says nothing about drafting. -->
					<section class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-6">
						<h3 class="mb-4 flex items-center text-lg font-bold text-white">
							<Award class="mr-2 h-5 w-5 text-purple-400" />
							Draft
						</h3>
						<dl class="space-y-3">
							<div class="flex items-center justify-between gap-4">
								<dt class="text-slate-400">Best pick's season</dt>
								<dd class="font-bold text-green-400 tabular-nums">{currentManager.bestDraftYear || '—'}</dd>
							</div>
							<div class="flex items-center justify-between gap-4">
								<dt class="text-slate-400">Worst pick's season</dt>
								<dd class="font-bold text-red-400 tabular-nums">{currentManager.worstDraftYear || '—'}</dd>
							</div>
						</dl>
						<p class="mt-4 text-xs text-slate-500">The seasons of this manager's single highest- and lowest-scoring draft picks.</p>
					</section>
				</div>

				<div class="text-center">
					<!-- An anchor rather than window.location.href: the latter forced a
					     full page reload in the middle of an SPA, which reads as a stall. -->
					<a
						href="/managers/{encodeURIComponent(currentManager.managerName)}"
						class="inline-block bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white font-bold py-3 px-6 rounded-lg transition-all transform hover:scale-105 shadow-lg"
					>
						View Detailed Profile & History
					</a>
				</div>
			</div>
		</div>
	{:else}
		<div class="flex items-center justify-center w-full h-64">
			<div class="text-slate-400">No manager data available</div>
		</div>
	{/if}
	</div>
</div>

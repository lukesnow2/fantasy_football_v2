<script lang="ts">
	import { BarChart3, TrendingUp, Trophy, Users, Target, Calendar, Zap } from 'lucide-svelte';
	import { onMount } from 'svelte';
	import ClientOnlyD3Overview from '$lib/components/ClientOnlyD3Overview.svelte';
	
	let h2hData: any = null;
	let overviewData: any = null;
	let loading = true;
	let loadingH2H = false;
	let loadingOverview = false;
	let selectedSeason = 'all';

	const headToHeadMatrix = [
		{ manager: "Mike", vs: { Sarah: "12-3", Jake: "8-6", Emma: "9-4", Chris: "11-2", Alex: "10-3" } },
		{ manager: "Sarah", vs: { Mike: "3-12", Jake: "7-7", Emma: "8-5", Chris: "9-4", Alex: "9-4" } },
		{ manager: "Jake", vs: { Mike: "6-8", Sarah: "7-7", Emma: "6-8", Chris: "8-5", Alex: "8-5" } },
		{ manager: "Emma", vs: { Mike: "4-9", Sarah: "5-8", Jake: "8-6", Chris: "7-6", Alex: "7-6" } },
		{ manager: "Chris", vs: { Mike: "2-11", Sarah: "4-9", Jake: "5-8", Emma: "6-7", Alex: "6-7" } },
		{ manager: "Alex", vs: { Mike: "3-10", Sarah: "4-9", Jake: "5-8", Emma: "6-7", Chris: "7-6" } },
	];

	const bestTrades = [
		{ 
			season: "2023", 
			trader: "Mike", 
			gave: "Dalvin Cook, 2024 2nd", 
			got: "Jonathan Taylor", 
			outcome: "+156 pts",
			grade: "A+"
		},
		{ 
			season: "2022", 
			trader: "Sarah", 
			gave: "Brandin Cooks", 
			got: "Rhamondre Stevenson", 
			outcome: "+89 pts",
			grade: "A"
		},
		{ 
			season: "2021", 
			trader: "Jake", 
			gave: "2022 1st, Courtland Sutton", 
			got: "Davante Adams", 
			outcome: "+134 pts",
			grade: "A+"
		},
	];

	const worstTrades = [
		{ 
			season: "2023", 
			trader: "Chris", 
			gave: "Josh Jacobs", 
			got: "Tony Pollard, 2024 3rd", 
			outcome: "-87 pts",
			grade: "D-"
		},
		{ 
			season: "2022", 
			trader: "Alex", 
			gave: "Cooper Kupp", 
			got: "DJ Moore, 2023 2nd", 
			outcome: "-156 pts",
			grade: "F"
		},
	];

	const draftGrades = [
		{ year: 2024, manager: "Sarah", grade: "A-", hits: 4, misses: 1, notes: "Nailed RB depth" },
		{ year: 2024, manager: "Mike", grade: "B+", hits: 3, misses: 2, notes: "QB reach hurt" },
		{ year: 2024, manager: "Jake", grade: "B", hits: 2, misses: 2, notes: "Safe picks" },
		{ year: 2023, manager: "Mike", grade: "A+", hits: 5, misses: 0, notes: "Perfect draft" },
		{ year: 2023, manager: "Emma", grade: "A", hits: 4, misses: 1, notes: "RB1 jackpot" },
	];

	const leagueRecords = [
		{ record: "Highest Single Season Points", holder: "Mike (2023)", value: "1,847.3", year: "2023" },
		{ record: "Most Championships", holder: "Mike", value: "3", year: "2018-2023" },
		{ record: "Longest Win Streak", holder: "Sarah", value: "11 games", year: "2022-2023" },
		{ record: "Most Trades in Season", holder: "Chris (2021)", value: "17", year: "2021" },
		{ record: "Highest Playoff Score", holder: "Jake", value: "178.9", year: "2022" },
		{ record: "Most Waiver Pickups", holder: "Alex (2020)", value: "47", year: "2020" },
	];

	let selectedTab = 'overview';
	const tabs = [
		{ id: 'overview', name: 'Overview', icon: BarChart3 },
		{ id: 'head-to-head', name: 'Head to Head', icon: Users },
		{ id: 'records', name: 'Record Book', icon: Trophy },
	];

	// D3 Chart placeholder - will implement actual charts
	let chartContainer: HTMLDivElement;
	
	onMount(async () => {
		// Skip the failing managers/historical endpoint for now
		loading = false;
		
		// Load overview data since it's the default tab - use setTimeout to ensure it runs after mount
		setTimeout(() => {
			loadOverviewData();
		}, 100);
		
		console.log('Historical page mounted, ready for D3 charts');
	});



	// Load head-to-head data
	async function loadH2HData() {
		if (h2hData) return; // Already loaded
		
		loadingH2H = true;
		try {
			const response = await fetch('/api/head-to-head?analysis=all');
			if (response.ok) {
				h2hData = await response.json();
				console.log('H2H data loaded:', h2hData);
			} else {
				console.error('Failed to fetch H2H data:', response.status, response.statusText);
			}
		} catch (err) {
			console.error('Error fetching H2H data:', err);
		} finally {
			loadingH2H = false;
		}
	}

	// Load overview data for D3 visualizations
	async function loadOverviewData() {
		console.log('loadOverviewData called, current state:', { overviewData: !!overviewData, loadingOverview });
		if (overviewData) return; // Already loaded
		
		loadingOverview = true;
		console.log('Setting loadingOverview to true');
		try {
			const response = await fetch('/api/overview?metric=all');
			console.log('API response status:', response.status);
			if (response.ok) {
				overviewData = await response.json();
				console.log('Overview data loaded:', overviewData);
				console.log('Data keys:', Object.keys(overviewData));
			} else {
				console.error('Failed to fetch overview data:', response.status, response.statusText);
			}
		} catch (err) {
			console.error('Error fetching overview data:', err);
		} finally {
			loadingOverview = false;
			console.log('Setting loadingOverview to false, overviewData exists:', !!overviewData);
		}
	}



	// Helper function to get rivalry tier color
	function getRivalryTierColor(tier: string): string {
		switch (tier) {
			case 'Epic Rivalry': return 'text-red-400';
			case 'Major Rivalry': return 'text-orange-400';
			case 'Strong Rivalry': return 'text-amber-400';
			case 'Developing Rivalry': return 'text-blue-400';
			default: return 'text-slate-400';
		}
	}

	// Helper function to get win percentage color
	function getWinPercentageColor(winPct: number): string {
		if (winPct >= 70) return 'text-green-400';
		if (winPct >= 55) return 'text-blue-400';
		if (winPct >= 45) return 'text-amber-400';
		return 'text-red-400';
	}



	// Handle tab selection
	function handleTabSelect(tabId: string) {
		selectedTab = tabId;
		if (tabId === 'head-to-head') {
			loadH2HData();
		} else if (tabId === 'overview') {
			loadOverviewData();
		}
	}
</script>

<div class="space-y-8">
	<!-- Page Header -->
	<div class="text-center py-8">
		<h1 class="text-5xl font-bold text-white mb-4">Historical Deep Dive</h1>
		<p class="text-xl text-slate-300 max-w-3xl mx-auto">
			Explore 14+ years of fantasy football history. Trades, head-to-head matchups, 
			league evolution, and championship moments analyzed and visualized.
		</p>
	</div>

	<!-- Tab Navigation -->
	<div class="flex flex-wrap justify-center gap-2 bg-slate-800/30 p-2 rounded-xl">
		{#each tabs as tab}
			<button
				class="flex items-center px-6 py-3 rounded-lg font-medium transition-all
					{selectedTab === tab.id 
						? 'bg-amber-500 text-black' 
						: 'text-slate-300 hover:text-white hover:bg-slate-700/50'}"
				on:click={() => handleTabSelect(tab.id)}
			>
				<svelte:component this={tab.icon} class="w-4 h-4 mr-2" />
				{tab.name}
			</button>
		{/each}
	</div>

	<!-- Tab Content -->
	{#if selectedTab === 'overview'}
		{#if loadingOverview}
			<div class="flex items-center justify-center h-64">
				<div class="text-slate-400">Loading comprehensive league analytics...</div>
			</div>
		{:else if overviewData}
			<!-- Debug: showing data keys -->
			<div class="text-xs text-slate-500 mb-2">Debug: Data loaded with keys: {Object.keys(overviewData.data || overviewData).join(', ')}</div>
			<div class="space-y-8">
				<!-- Key Statistics -->
				<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
					<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
						<Trophy class="w-6 h-6 text-amber-400 mr-3" />
						20-Year League Summary
					</h2>
					
					<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
						<div class="bg-gradient-to-r from-blue-500/10 to-blue-600/5 border border-blue-500/20 rounded-lg p-4">
							<h3 class="font-bold text-white mb-2">Total Seasons</h3>
							<div class="text-3xl font-bold text-blue-400">20</div>
							<div class="text-slate-300 text-sm">2005 - 2024</div>
						</div>
						
						<div class="bg-gradient-to-r from-green-500/10 to-green-600/5 border border-green-500/20 rounded-lg p-4">
							<h3 class="font-bold text-white mb-2">						Average Score</h3>
						<div class="text-3xl font-bold text-green-400">
							{#if overviewData.scoring_patterns}
								{(overviewData.scoring_patterns.reduce((sum: number, s: any) => sum + parseFloat(s.avg_weekly_score), 0) / overviewData.scoring_patterns.length).toFixed(1)}
							{:else}
								125.2
							{/if}
							</div>
							<div class="text-slate-300 text-sm">Points per week</div>
						</div>
						
						<div class="bg-gradient-to-r from-amber-500/10 to-amber-600/5 border border-amber-500/20 rounded-lg p-4">
							<h3 class="font-bold text-white mb-2">Total Trades</h3>
							<div class="text-3xl font-bold text-amber-400">
								{#if overviewData.trade_activity}
									{overviewData.trade_activity.reduce((sum: number, t: any) => sum + (t.total_trades || 0), 0)}
								{:else}
									99
								{/if}
							</div>
							<div class="text-slate-300 text-sm">All-time</div>
						</div>
						
						<div class="bg-gradient-to-r from-purple-500/10 to-purple-600/5 border border-purple-500/20 rounded-lg p-4">
							<h3 class="font-bold text-white mb-2">Peak Activity</h3>
							<div class="text-3xl font-bold text-purple-400">
								{#if overviewData.league_evolution}
									{Math.max(...overviewData.league_evolution.map((l: any) => l.total_transactions))}
								{:else}
									641
								{/if}
							</div>
							<div class="text-slate-300 text-sm">Transactions (2020)</div>
						</div>
					</div>
				</section>

				<!-- D3 Visualization Section -->
				<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
					<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
						<BarChart3 class="w-6 h-6 text-blue-400 mr-3" />
						League Evolution (2005-2024)
					</h2>
					
					<ClientOnlyD3Overview data={overviewData} />
				</section>

				<!-- Era Analysis -->
				<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
					<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
						<Calendar class="w-6 h-6 text-purple-400 mr-3" />
						League Eras Analysis
					</h2>
					
					<div class="mb-4 text-sm text-slate-400 italic">
						Analysis generated from scoring patterns, transaction volumes, league structure, and competitiveness metrics (2005-2024)
					</div>
					
					<div class="grid grid-cols-1 md:grid-cols-3 gap-6">
						<div class="bg-slate-700/30 rounded-lg p-4 border-l-4 border-blue-400">
							<h3 class="font-bold text-white mb-2">Formation Era (2005-2010)</h3>
							<div class="text-sm text-slate-300 space-y-1">
								<div>• Structural instability: 8-10 teams fluctuating</div>
								<div>• Wild scoring swings: 98.7-133.8 pts</div>
								<div>• High volatility: 12-16 score variance</div>
								<div>• Erratic activity: 202-424 transactions</div>
								<div>• Chaotic competition: Win parity 30-59</div>
								<div>• Inconsistent close games: 21-40 range</div>
								<div class="text-xs text-slate-400 mt-2">2007: Lowest close games (21.7), 2010: Peak parity (59.0)</div>
							</div>
						</div>
						
						<div class="bg-slate-700/30 rounded-lg p-4 border-l-4 border-green-400">
							<h3 class="font-bold text-white mb-2">Maturation Era (2011-2018)</h3>
							<div class="text-sm text-slate-300 space-y-1">
								<div>• League stabilizes: Consistent 10 teams</div>
								<div>• Scoring convergence: 119-133 range</div>
								<div>• Volatility decline: 12→9 (peak stability 2016-17)</div>
								<div>• Transaction growth: 450-600 range</div>
								<div>• Competition improves: Win parity trending up</div>
								<div>• Peak close games: 2016-17 (42-43)</div>
								<div class="text-xs text-slate-400 mt-2">2018: Peak parity (63.6), 2017: Most close games (43.4)</div>
							</div>
						</div>
						
						<div class="bg-slate-700/30 rounded-lg p-4 border-l-4 border-amber-400">
							<h3 class="font-bold text-white mb-2">Modern Era (2019-2024)</h3>
							<div class="text-sm text-slate-300 space-y-1">
								<div>• High activity plateau: 575+ transactions</div>
								<div>• Scoring stabilization: 122-132 narrow band</div>
								<div>• Controlled volatility: 10-12 range</div>
								<div>• Peak engagement: 2020-21 (641-634 transactions)</div>
								<div>• Sustained high competition: 55+ avg parity</div>
								<div>• Consistent close games: 31-37 stable range</div>
								<div class="text-xs text-slate-400 mt-2">2021: Peak point spread (83.2), most balanced era overall</div>
							</div>
						</div>
					</div>
				</section>

			</div>
		{:else}
			<div class="text-center py-8">
				<div class="text-slate-400">Click Overview to load comprehensive league analytics</div>
				<div class="text-xs text-slate-500 mt-2">Debug: loadingOverview={loadingOverview}, overviewData={!!overviewData}</div>
				<button 
					class="mt-4 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
					on:click={loadOverviewData}
				>
					Load Overview Data Manually
				</button>
			</div>
		{/if}

	{:else if selectedTab === 'head-to-head'}
		{#if loadingH2H}
			<div class="flex items-center justify-center h-64">
				<div class="text-slate-400">Loading comprehensive head-to-head analysis...</div>
			</div>
		{:else if h2hData}
			<div class="space-y-8">
				<!-- H2H Overview Analytics -->
				{#if h2hData.analytics?.overview}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<BarChart3 class="w-6 h-6 text-blue-400 mr-3" />
							Head-to-Head Overview
						</h2>
						
						<div class="grid grid-cols-2 md:grid-cols-4 gap-6">
							<div class="text-center">
								<div class="text-3xl font-bold text-blue-400">{h2hData.analytics.overview.total_rivalries || 0}</div>
								<div class="text-slate-400">Total Rivalries</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-green-400">{parseFloat(h2hData.analytics.overview.avg_matchups_per_rivalry || 0).toFixed(1)}</div>
								<div class="text-slate-400">Avg Games</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-amber-400">{h2hData.analytics.overview.longest_rivalry_seasons || 0}</div>
								<div class="text-slate-400">Longest Rivalry</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-purple-400">{h2hData.analytics.overview.championship_rivalries || 0}</div>
								<div class="text-slate-400">Championship Battles</div>
							</div>
						</div>
					</section>
				{/if}

				<!-- Top Rivalries -->
				{#if h2hData.analytics?.top_rivalries}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<Trophy class="w-6 h-6 text-amber-400 mr-3" />
							Epic Rivalries
						</h2>
						
						<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
							{#each h2hData.analytics.top_rivalries.slice(0, 6) as rivalry}
								<div class="bg-slate-700/30 rounded-lg p-4 border-l-4 border-amber-400">
									<div class="flex items-center justify-between mb-3">
										<div class="font-bold text-white text-lg">
											{rivalry.manager_a_name} vs {rivalry.manager_b_name}
										</div>
										<span class="text-xs px-2 py-1 rounded {getRivalryTierColor(rivalry.rivalry_tier)} bg-slate-600/50">
											{rivalry.rivalry_tier}
										</span>
									</div>
									
									<div class="grid grid-cols-2 gap-4 text-sm">
										<div>
											<div class="text-slate-400">Total Games</div>
											<div class="font-bold text-white">{rivalry.total_matchups}</div>
										</div>
										<div>
											<div class="text-slate-400">Seasons Together</div>
											<div class="font-bold text-white">{rivalry.seasons_played_together}</div>
										</div>
										<div>
											<div class="text-slate-400">Series Leader</div>
											<div class="font-bold text-green-400">{rivalry.series_leader}</div>
										</div>
										<div>
											<div class="text-slate-400">Record</div>
											<div class="font-mono font-bold text-white">{rivalry.series_record}</div>
										</div>
									</div>
									
									{#if rivalry.playoff_matchups > 0 || rivalry.championship_matchups > 0}
										<div class="mt-3 pt-3 border-t border-slate-600">
											<div class="flex items-center space-x-4 text-xs">
												{#if rivalry.championship_matchups > 0}
													<span class="text-amber-400">🏆 {rivalry.championship_matchups} Title Games</span>
												{/if}
												{#if rivalry.playoff_matchups > 0}
													<span class="text-blue-400">🏆 {rivalry.playoff_matchups} Playoff Games</span>
												{/if}
											</div>
										</div>
									{/if}
								</div>
							{/each}
						</div>
					</section>
				{/if}

				<!-- Complete Head-to-Head Matrix -->
				<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
					<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
						<Users class="w-6 h-6 text-green-400 mr-3" />
						Complete Head-to-Head Records
					</h2>
					
					<div class="overflow-x-auto">
						<table class="w-full text-sm">
							<thead>
								<tr class="border-b border-slate-700">
									<th class="text-left py-3 px-4 text-slate-300">Matchup</th>
									<th class="text-center py-3 px-4 text-slate-300">Games</th>
									<th class="text-center py-3 px-4 text-slate-300">Record</th>
									<th class="text-center py-3 px-4 text-slate-300">Avg Score</th>
									<th class="text-center py-3 px-4 text-slate-300">Biggest Win</th>
									<th class="text-center py-3 px-4 text-slate-300">Playoffs</th>
								</tr>
							</thead>
							<tbody>
								{#each h2hData.head_to_head.slice(0, 20) as matchup}
									<tr class="border-b border-slate-700/50">
										<td class="py-3 px-4">
											<div class="font-bold text-white">{matchup.manager_a_name} vs {matchup.manager_b_name}</div>
											<div class="text-xs text-slate-400">{matchup.seasons_played_together} seasons • {matchup.first_matchup_date} - {matchup.last_matchup_date}</div>
										</td>
										<td class="text-center py-3 px-4 font-bold text-blue-400">{matchup.total_matchups}</td>
										<td class="text-center py-3 px-4">
											<div class="font-mono font-bold text-white">{matchup.series_record}</div>
											<div class="text-xs text-slate-400">{matchup.series_leader}</div>
										</td>
										<td class="text-center py-3 px-4">
											<div class="font-bold text-white">{parseFloat(matchup.avg_total_points_per_game || 0).toFixed(1)}</div>
											<div class="text-xs text-slate-400">pts/game</div>
										</td>
										<td class="text-center py-3 px-4">
											<div class="font-bold text-amber-400">{parseFloat(matchup.most_lopsided_game || 0).toFixed(1)}</div>
											<div class="text-xs text-slate-400">margin</div>
										</td>
										<td class="text-center py-3 px-4">
											{#if matchup.championship_matchups > 0}
												<span class="text-amber-400 font-bold">🏆 {matchup.championship_matchups}</span>
											{:else if matchup.playoff_matchups > 0}
												<span class="text-blue-400 font-bold">⚔️ {matchup.playoff_matchups}</span>
											{:else}
												<span class="text-slate-500">-</span>
											{/if}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
					
					{#if h2hData.head_to_head.length > 20}
						<div class="mt-4 text-center text-slate-400 text-sm">
							Showing top 20 of {h2hData.head_to_head.length} rivalries
						</div>
					{/if}
				</section>

				<!-- League Records -->
				{#if h2hData.analytics?.records}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<Trophy class="w-6 h-6 text-amber-400 mr-3" />
							Head-to-Head Records
						</h2>
						
						<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
							<div class="bg-gradient-to-r from-green-500/10 to-green-600/5 border border-green-500/20 rounded-lg p-4">
								<h3 class="font-bold text-white mb-2">Highest Score</h3>
								<div class="text-2xl font-bold text-green-400">{parseFloat(h2hData.analytics.records.highest_score_value || 0).toFixed(1)}</div>
								<div class="text-slate-300 font-medium">{h2hData.analytics.records.highest_score_manager}</div>
							</div>
							
							<div class="bg-gradient-to-r from-red-500/10 to-red-600/5 border border-red-500/20 rounded-lg p-4">
								<h3 class="font-bold text-white mb-2">Biggest Blowout</h3>
								<div class="text-2xl font-bold text-red-400">{parseFloat(h2hData.analytics.records.biggest_win_value || 0).toFixed(1)}</div>
								<div class="text-slate-300 font-medium">{h2hData.analytics.records.biggest_win_manager}</div>
							</div>
							
							<div class="bg-gradient-to-r from-blue-500/10 to-blue-600/5 border border-blue-500/20 rounded-lg p-4">
								<h3 class="font-bold text-white mb-2">Hot Streak</h3>
								<div class="text-2xl font-bold text-blue-400">{h2hData.analytics.records.longest_streak_value || 0}</div>
								<div class="text-slate-300 font-medium">{h2hData.analytics.records.longest_streak_manager}</div>
							</div>
							
							<div class="bg-gradient-to-r from-purple-500/10 to-purple-600/5 border border-purple-500/20 rounded-lg p-4">
								<h3 class="font-bold text-white mb-2">Cold Streak</h3>
								<div class="text-2xl font-bold text-purple-400">{h2hData.analytics.records.worst_streak_value || 0}</div>
								<div class="text-slate-300 font-medium">{h2hData.analytics.records.worst_streak_manager}</div>
							</div>
						</div>
					</section>
				{/if}

				<!-- Playoff Impact -->
				{#if h2hData.analytics?.playoff_impact && h2hData.analytics.playoff_impact.length > 0}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<Target class="w-6 h-6 text-red-400 mr-3" />
							Championship & Playoff Battles
						</h2>
						
						<div class="space-y-4">
							{#each h2hData.analytics.playoff_impact.slice(0, 8) as battle}
								<div class="bg-slate-700/30 rounded-lg p-4">
									<div class="flex items-center justify-between mb-2">
										<div class="font-bold text-white">{battle.manager_a_name} vs {battle.manager_b_name}</div>
										<div class="flex items-center space-x-2">
											{#if battle.championship_matchups > 0}
												<span class="bg-amber-500 text-black px-2 py-1 rounded text-xs font-bold">
													{battle.championship_matchups} Title Games
												</span>
											{/if}
											{#if battle.playoff_matchups > 0}
												<span class="bg-blue-500 text-white px-2 py-1 rounded text-xs font-bold">
													{battle.playoff_matchups} Playoff Games
												</span>
											{/if}
										</div>
									</div>
									
									<div class="grid grid-cols-2 gap-4 text-sm">
										<div>
											<span class="text-blue-400 font-medium">{battle.manager_a_name}:</span>
											<span class="text-slate-300">
												{battle.manager_a_championship_wins || 0} titles, {battle.manager_a_playoff_wins || 0} playoff wins
											</span>
										</div>
										<div>
											<span class="text-purple-400 font-medium">{battle.manager_b_name}:</span>
											<span class="text-slate-300">
												{battle.manager_b_championship_wins || 0} titles, {battle.manager_b_playoff_wins || 0} playoff wins
											</span>
										</div>
									</div>
									
									{#if battle.most_important_game_winner}
										<div class="mt-2 pt-2 border-t border-slate-600 text-xs text-slate-400">
											Most Important: {battle.most_important_game_winner} ({battle.most_important_game_type}, {battle.most_important_game_season})
										</div>
									{/if}
								</div>
							{/each}
						</div>
					</section>
				{/if}
			</div>
		{:else}
			<div class="text-center py-8">
				<div class="text-slate-400">No head-to-head data available</div>
			</div>
		{/if}


	{:else if selectedTab === 'records'}
		<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
			<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
				<Trophy class="w-6 h-6 text-amber-400 mr-3" />
				League Record Book
			</h2>
			
			<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
				{#each leagueRecords as record}
					<div class="bg-gradient-to-r from-amber-500/10 to-amber-600/5 border border-amber-500/20 rounded-lg p-6">
						<h3 class="font-bold text-white mb-2">{record.record}</h3>
						<div class="flex items-center justify-between">
							<div>
								<div class="text-2xl font-bold text-amber-400">{record.value}</div>
								<div class="text-slate-300 font-medium">{record.holder}</div>
							</div>
							<div class="text-right">
								<div class="text-slate-400 text-sm">{record.year}</div>
								<Trophy class="w-6 h-6 text-amber-400 ml-auto mt-1" />
							</div>
						</div>
					</div>
				{/each}
			</div>
		</section>
	{/if}
</div> 
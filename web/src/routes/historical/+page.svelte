<script lang="ts">
	import { onMount } from 'svelte';
	import { Trophy, TrendingUp, Users, Target, Calendar, BarChart3, MessageSquare, ExternalLink } from 'lucide-svelte';
	import MetricTooltip from '$lib/components/MetricTooltip.svelte';
	import ClientOnlyD3Overview from '$lib/components/ClientOnlyD3Overview.svelte';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	
	// Real data from API
	let h2hData: any = {};
	let overviewData: any = null;
	let recordBookData: any = null;
	let loading = true;
	let loadingH2H = false;
	let loadingOverview = false;
	let loadingRecords = false;
	let error = '';
	let selectedTab = 'head-to-head';
	let selectedSeason = 'all';
	
	// Filter state
	let h2hManagerA = '';
	let h2hManagerB = '';
	let showAllH2H = false;
	
	// Available managers for filtering
	let availableManagers: string[] = [];
	
	// Computed filtered data
	$: filteredH2HData = h2hData.headToHead ? h2hData.headToHead.filter((matchup: any) => {
		if (h2hManagerA && !matchup.managerAName?.includes(h2hManagerA) && !matchup.managerBName?.includes(h2hManagerA)) return false;
		if (h2hManagerB && !matchup.managerAName?.includes(h2hManagerB) && !matchup.managerBName?.includes(h2hManagerB)) return false;
		return true;
	}) : [];
	
	$: displayH2HData = showAllH2H ? filteredH2HData : filteredH2HData.slice(0, 20);

	// Function to get display data
	function getDisplayH2HData(data: any[], managerA: string, managerB: string, showAll: boolean) {
		if (!data || data.length === 0) return [];
		
		// If filters are applied, show all filtered results
		if (managerA || managerB) {
			return data;
		}
		
		// If no filters and showAll is false, limit to 20
		if (!showAll) {
			return data.slice(0, 20);
		}
		
		// Otherwise show all
		return data;
	}

	// Function to filter head-to-head data
	function filterH2HData(data: any[], managerA: string, managerB: string) {
		if (!data || data.length === 0) return [];
		
		let filtered = data;
		
		// Apply specific manager filters
		if (managerA || managerB) {
			filtered = filtered.filter(matchup => {
				const managerAName = matchup.managerAName || matchup.manager_a_name;
				const managerBName = matchup.managerBName || matchup.manager_b_name;
				const matchupManagers = [managerAName, managerBName];
				
				let matchesA = !managerA || matchupManagers.includes(managerA);
				let matchesB = !managerB || matchupManagers.includes(managerB);
				
				// If both managers are selected, ensure both are in the matchup
				if (managerA && managerB) {
					return matchupManagers.includes(managerA) && matchupManagers.includes(managerB);
				}
				
				return matchesA && matchesB;
			});
		}
		
		return filtered;
	}

	// Function to extract unique managers from h2h data
	function extractUniqueManagers(h2hData: any[]): string[] {
		if (!h2hData || h2hData.length === 0) return [];
		
		const managers = new Set<string>();
		h2hData.forEach(matchup => {
			if (matchup.managerAName || matchup.manager_a_name) managers.add(matchup.managerAName || matchup.manager_a_name);
			if (matchup.managerBName || matchup.manager_b_name) managers.add(matchup.managerBName || matchup.manager_b_name);
		});
		
		return Array.from(managers).sort();
	}

	// Clear all filters
	function clearH2HFilters() {
		h2hManagerA = '';
		h2hManagerB = '';
		showAllH2H = false; // Reset show all when clearing filters
	}

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



	// Tab configuration
	const tabs = [
		{ id: 'head-to-head', label: 'Head-to-Head', icon: Users },
		{ id: 'overview', label: 'League Overview', icon: BarChart3 },
		{ id: 'records', label: 'Record Book', icon: Trophy }
	];
	
	// D3 Chart placeholder - will implement actual charts
	let chartContainer: HTMLDivElement;
	
	onMount(async () => {
		// Skip the failing managers/historical endpoint for now
		loading = false;
		
		// Test the meta-data API endpoint
		console.log('=== TESTING META-DATA API ===');
		try {
			const testResponse = await fetch('/api/meta-data?metric_id=pythagorean_wins');
			console.log('Meta-data API response status:', testResponse.status);
			if (testResponse.ok) {
				const testData = await testResponse.json();
				console.log('Meta-data API response data:', testData);
			} else {
				console.error('Meta-data API failed:', testResponse.status, testResponse.statusText);
			}
		} catch (err) {
			console.error('Meta-data API error:', err);
		}
		console.log('=== END META-DATA API TEST ===');
		
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
				// Extract available managers for filtering
				availableManagers = extractUniqueManagers(h2hData.headToHead || []);
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

	// Load record book data
	async function loadRecordBookData() {
		if (recordBookData) return; // Already loaded
		
		loadingRecords = true;
		try {
			const response = await fetch('/api/record-book');
			if (response.ok) {
				recordBookData = await response.json();
				console.log('Record book data loaded:', recordBookData);
			} else {
				console.error('Failed to fetch record book data:', response.status, response.statusText);
			}
		} catch (err) {
			console.error('Error fetching record book data:', err);
		} finally {
			loadingRecords = false;
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
		} else if (tabId === 'records') {
			loadRecordBookData();
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
				{tab.label}
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
								<div class="text-3xl font-bold text-blue-400">{h2hData.analytics.overview.totalRivalries || h2hData.analytics.overview.total_rivalries || 0}</div>
								<div class="text-slate-400">Total Rivalries</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-green-400">{parseFloat(h2hData.analytics.overview.avgMatchupsPerRivalry || h2hData.analytics.overview.avg_matchups_per_rivalry || 0).toFixed(1)}</div>
								<div class="text-slate-400">Avg Games</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-amber-400">{h2hData.analytics.overview.longestRivalrySeasons || h2hData.analytics.overview.longest_rivalry_seasons || 0}</div>
								<div class="text-slate-400">Longest Rivalry</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-purple-400">{h2hData.analytics.overview.championshipRivalries || h2hData.analytics.overview.championship_rivalries || 0}</div>
								<div class="text-slate-400">Championship Battles</div>
							</div>
						</div>
					</section>
				{/if}

				<!-- Top Rivalries -->
				{#if h2hData.analytics?.topRivalries}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<Trophy class="w-6 h-6 text-amber-400 mr-3" />
							Epic Rivalries
						</h2>
						
						<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
							{#each h2hData.analytics.topRivalries.slice(0, 6) as rivalry}
								<div class="bg-slate-700/30 rounded-lg p-4 border-l-4 border-amber-400">
									<div class="flex items-center justify-between mb-3">
										<div class="font-bold text-white text-lg">
											{rivalry.managerAName || rivalry.manager_a_name} vs {rivalry.managerBName || rivalry.manager_b_name}
										</div>
										<span class="text-xs px-2 py-1 rounded {getRivalryTierColor(rivalry.rivalryTier || rivalry.rivalry_tier)} bg-slate-600/50">
											{rivalry.rivalryTier || rivalry.rivalry_tier}
										</span>
									</div>
									
									<div class="grid grid-cols-2 gap-4 text-sm">
										<div>
											<div class="text-slate-400">Total Games</div>
											<div class="font-bold text-white">{rivalry.totalMatchups || rivalry.total_matchups}</div>
										</div>
										<div>
											<div class="text-slate-400">Seasons Together</div>
											<div class="font-bold text-white">{rivalry.seasonsPlayedTogether || rivalry.seasons_played_together}</div>
										</div>
										<div>
											<div class="text-slate-400">Series Leader</div>
											<div class="font-bold text-green-400">{rivalry.seriesLeader || rivalry.series_leader}</div>
										</div>
										<div>
											<div class="text-slate-400">Record</div>
											<div class="font-mono font-bold text-white">{rivalry.seriesRecord || rivalry.series_record}</div>
										</div>
									</div>
									
									{#if (rivalry.playoffMatchups || rivalry.playoff_matchups) > 0 || (rivalry.championshipMatchups || rivalry.championship_matchups) > 0}
										<div class="mt-3 pt-3 border-t border-slate-600">
											<div class="flex items-center space-x-4 text-xs">
												{#if (rivalry.championshipMatchups || rivalry.championship_matchups) > 0}
													<span class="text-amber-400">🏆 {rivalry.championshipMatchups || rivalry.championship_matchups} Title Games</span>
												{/if}
												{#if (rivalry.playoffMatchups || rivalry.playoff_matchups) > 0}
													<span class="text-blue-400">🏆 {rivalry.playoffMatchups || rivalry.playoff_matchups} Playoff Games</span>
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
					
					<!-- Filter Controls -->
					<div class="mb-6 space-y-4">
						<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
							<!-- Manager A Selector -->
							<div>
								<label for="h2h-manager-a" class="block text-sm font-medium text-slate-300 mb-2">
									Manager A
								</label>
								<select
									id="h2h-manager-a"
									bind:value={h2hManagerA}
									class="w-full bg-slate-700/50 border border-slate-600 rounded-lg px-3 py-2 text-white focus:outline-none focus:border-green-400"
								>
									<option value="">Any Manager</option>
									{#each availableManagers as manager}
										<option value={manager}>{manager}</option>
									{/each}
								</select>
							</div>
							
							<!-- Manager B Selector -->
							<div>
								<label for="h2h-manager-b" class="block text-sm font-medium text-slate-300 mb-2">
									Manager B
								</label>
								<select
									id="h2h-manager-b"
									bind:value={h2hManagerB}
									class="w-full bg-slate-700/50 border border-slate-600 rounded-lg px-3 py-2 text-white focus:outline-none focus:border-green-400"
								>
									<option value="">Any Manager</option>
									{#each availableManagers as manager}
										<option value={manager}>{manager}</option>
									{/each}
								</select>
							</div>
						</div>
						
						<!-- Filter Actions -->
						<div class="flex items-center justify-between">
							<div class="text-sm text-slate-400">
								{#if h2hManagerA || h2hManagerB}
									Showing {displayH2HData.length} of {h2hData?.headToHead?.length || 0} matchups
								{:else}
									Showing {displayH2HData.length} of {filteredH2HData.length} rivalries
								{/if}
							</div>
							{#if h2hManagerA || h2hManagerB}
								<button
									on:click={clearH2HFilters}
									class="text-sm text-amber-400 hover:text-amber-300 font-medium"
								>
									Clear Filters
								</button>
							{/if}
						</div>
					</div>
					
					<div class="relative">
						<!-- TEMPORARY: Removed overflow-x-auto to test tooltip positioning -->
						<table class="w-full text-sm">
							<thead>
								<tr class="border-b border-slate-700">
									<th class="text-left py-3 px-4 text-slate-300">Matchup</th>
									<th class="text-center py-3 px-4 text-slate-300">Games</th>
									<th class="text-center py-3 px-4 text-slate-300">Record</th>
									<th class="text-center py-3 px-4 text-slate-300">Avg Score</th>
									<th class="text-center py-3 px-4 text-slate-300">Biggest Win</th>
									<th class="text-center py-3 px-4 text-slate-300">
										<span class="cursor-help" title="Expected wins based on points scored vs points against. Higher = better performance relative to scoring.">
											Pythagorean Wins
										</span>
									</th>
									<th class="text-center py-3 px-4 text-slate-300">
										<span class="cursor-help" title="Difference between actual wins and expected wins. Positive = lucky, negative = unlucky.">
											Luck Factor
										</span>
									</th>
									<th class="text-center py-3 px-4 text-slate-300">
										<!-- Test tooltip to see if component works -->
										<MetricTooltip metricId="total_matchups" position="top" maxWidth="400px">
											<span class="cursor-help text-red-400" on:mouseenter={() => console.log('Test tooltip mouseenter')} on:mouseleave={() => console.log('Test tooltip mouseleave')}>Test Tooltip</span>
										</MetricTooltip>
									</th>
									<th class="text-center py-3 px-4 text-slate-300">Playoffs</th>
								</tr>
							</thead>
							<tbody>
								{#each displayH2HData as matchup}
									<tr class="border-b border-slate-700/50">
										<td class="py-3 px-4">
											<div class="font-bold text-white">{matchup.managerAName || matchup.manager_a_name} vs {matchup.managerBName || matchup.manager_b_name}</div>
											<div class="text-xs text-slate-400">{matchup.seasonsPlayedTogether || matchup.seasons_played_together} seasons • {matchup.firstMatchupDate || matchup.first_matchup_date} - {matchup.lastMatchupDate || matchup.last_matchup_date}</div>
										</td>
										<td class="text-center py-3 px-4 font-bold text-blue-400">{matchup.totalMatchups || matchup.total_matchups}</td>
										<td class="text-center py-3 px-4">
											<div class="font-mono font-bold text-white">{matchup.seriesRecord || matchup.series_record}</div>
											<div class="text-xs text-slate-400">{matchup.seriesLeader || matchup.series_leader}</div>
										</td>
										<td class="text-center py-3 px-4">
											<div class="font-bold text-white">{parseFloat(matchup.avgTotalPointsPerGame || matchup.avg_total_points_per_game || 0).toFixed(1)}</div>
											<div class="text-xs text-slate-400">pts/game</div>
										</td>
										<td class="text-center py-3 px-4">
											<div class="font-bold text-amber-400">{parseFloat(matchup.mostLopsidedGame || matchup.most_lopsided_game || 0).toFixed(1)}</div>
											<div class="text-xs text-slate-400">margin</div>
										</td>
										<td class="text-center py-3 px-4">
											<div class="font-bold text-purple-400">
												{parseFloat(matchup.managerAPythagoreanWins || matchup.manager_a_pythagorean_wins || 0).toFixed(1)} - {parseFloat(matchup.managerBPythagoreanWins || matchup.manager_b_pythagorean_wins || 0).toFixed(1)}
											</div>
											<div class="text-xs text-slate-400">expected wins</div>
										</td>
										<td class="text-center py-3 px-4">
											<div class="font-bold {parseFloat(matchup.managerALuckFactor || matchup.manager_a_luck_factor || 0) > 0 ? 'text-green-400' : 'text-red-400'}">
												{parseFloat(matchup.managerALuckFactor || matchup.manager_a_luck_factor || 0).toFixed(1)} / {parseFloat(matchup.managerBLuckFactor || matchup.manager_b_luck_factor || 0).toFixed(1)}
											</div>
											<div class="text-xs text-slate-400">luck factor</div>
										</td>
										<td class="text-center py-3 px-4">
											{#if (matchup.championshipMatchups || matchup.championship_matchups) > 0}
												<span class="text-amber-400 font-bold">🏆 {matchup.championshipMatchups || matchup.championship_matchups}</span>
											{:else if (matchup.playoffMatchups || matchup.playoff_matchups) > 0}
												<span class="text-blue-400 font-bold">⚔️ {matchup.playoffMatchups || matchup.playoff_matchups}</span>
											{:else}
												<span class="text-slate-500">-</span>
											{/if}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
					
					<!-- Show More/Less Controls -->
					{#if !h2hManagerA && !h2hManagerB}
						<div class="mt-6 text-center">
							{#if !showAllH2H && filteredH2HData.length > 20}
								<button
									on:click={() => showAllH2H = true}
									class="inline-flex items-center px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors"
								>
									Show all {filteredH2HData.length} rivalries
								</button>
							{:else if showAllH2H}
								<button
									on:click={() => showAllH2H = false}
									class="inline-flex items-center px-4 py-2 bg-slate-600 hover:bg-slate-700 text-white font-medium rounded-lg transition-colors"
								>
									Show top 20
								</button>
							{/if}
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
								<div class="text-2xl font-bold text-green-400">{parseFloat(h2hData.analytics.records.highestScoreValue || h2hData.analytics.records.highest_score_value || 0).toFixed(1)}</div>
								<div class="text-slate-300 font-medium">{h2hData.analytics.records.highestScoreManager || h2hData.analytics.records.highest_score_manager}</div>
							</div>
							
							<div class="bg-gradient-to-r from-red-500/10 to-red-600/5 border border-red-500/20 rounded-lg p-4">
								<h3 class="font-bold text-white mb-2">Biggest Blowout</h3>
								<div class="text-2xl font-bold text-red-400">{parseFloat(h2hData.analytics.records.biggestWinValue || h2hData.analytics.records.biggest_win_value || 0).toFixed(1)}</div>
								<div class="text-slate-300 font-medium">{h2hData.analytics.records.biggestWinManager || h2hData.analytics.records.biggest_win_manager}</div>
							</div>
							
							<div class="bg-gradient-to-r from-blue-500/10 to-blue-600/5 border border-blue-500/20 rounded-lg p-4">
								<h3 class="font-bold text-white mb-2">Hot Streak</h3>
								<div class="text-2xl font-bold text-blue-400">{h2hData.analytics.records.longestStreakValue || h2hData.analytics.records.longest_streak_value || 0}</div>
								<div class="text-slate-300 font-medium">{h2hData.analytics.records.longestStreakManager || h2hData.analytics.records.longest_streak_manager}</div>
							</div>
							
							<div class="bg-gradient-to-r from-purple-500/10 to-purple-600/5 border border-purple-500/20 rounded-lg p-4">
								<h3 class="font-bold text-white mb-2">Cold Streak</h3>
								<div class="text-2xl font-bold text-purple-400">{h2hData.analytics.records.worstStreakValue || h2hData.analytics.records.worst_streak_value || 0}</div>
								<div class="text-slate-300 font-medium">{h2hData.analytics.records.worstStreakManager || h2hData.analytics.records.worst_streak_manager}</div>
							</div>
						</div>
					</section>
				{/if}

				<!-- Playoff Impact -->
				{#if h2hData.analytics?.playoffImpact && h2hData.analytics.playoffImpact.length > 0}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<Target class="w-6 h-6 text-red-400 mr-3" />
							Championship & Playoff Battles
						</h2>
						
						<div class="space-y-4">
							{#each h2hData.analytics.playoffImpact.slice(0, 8) as battle}
								<div class="bg-slate-700/30 rounded-lg p-4">
									<div class="flex items-center justify-between mb-2">
										<div class="font-bold text-white">{battle.managerAName || battle.manager_a_name} vs {battle.managerBName || battle.manager_b_name}</div>
										<div class="flex items-center space-x-2">
											{#if (battle.championshipMatchups || battle.championship_matchups) > 0}
												<span class="bg-amber-500 text-black px-2 py-1 rounded text-xs font-bold">
													{battle.championshipMatchups || battle.championship_matchups} Title Games
												</span>
											{/if}
											{#if (battle.playoffMatchups || battle.playoff_matchups) > 0}
												<span class="bg-blue-500 text-white px-2 py-1 rounded text-xs font-bold">
													{battle.playoffMatchups || battle.playoff_matchups} Playoff Games
												</span>
											{/if}
										</div>
									</div>
									
									<div class="grid grid-cols-2 gap-4 text-sm">
										<div>
											<span class="text-blue-400 font-medium">{battle.managerAName || battle.manager_a_name}:</span>
											<span class="text-slate-300">
												{battle.managerAChampionshipWins || battle.manager_a_championship_wins || 0} titles, {battle.managerAPlayoffWins || battle.manager_a_playoff_wins || 0} playoff wins
											</span>
										</div>
										<div>
											<span class="text-purple-400 font-medium">{battle.managerBName || battle.manager_b_name}:</span>
											<span class="text-slate-300">
												{battle.managerBChampionshipWins || battle.manager_b_championship_wins || 0} titles, {battle.managerBPlayoffWins || battle.manager_b_playoff_wins || 0} playoff wins
											</span>
										</div>
									</div>
									
									{#if battle.mostImportantGameWinner || battle.most_important_game_winner}
										<div class="mt-2 pt-2 border-t border-slate-600 text-xs text-slate-400">
											Most Important: {battle.mostImportantGameWinner || battle.most_important_game_winner} ({battle.mostImportantGameType || battle.most_important_game_type}, {battle.mostImportantGameSeason || battle.most_important_game_season})
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
		{#if loadingRecords}
			<div class="flex items-center justify-center h-64">
				<div class="text-slate-400">Loading league record book...</div>
			</div>
		{:else if recordBookData?.recordsByCategory}
			<div class="space-y-8">
				<!-- Header -->
				<section class="text-center">
					<h2 class="text-3xl font-bold text-white mb-4 flex items-center justify-center">
						<Trophy class="w-8 h-8 text-amber-400 mr-3" />
						League Record Book
					</h2>
					<p class="text-slate-300 max-w-2xl mx-auto">
						The greatest achievements in league history, organized by category for easy browsing.
					</p>
					<div class="mt-4 text-sm text-slate-400">
						{recordBookData.totalRecords} total records across {recordBookData.categories?.length || 0} categories
					</div>
				</section>

				<!-- Categories -->
				{#each recordBookData.categories || [] as category}
					{@const categoryRecords = recordBookData.recordsByCategory[category] || []}
					{@const categoryIcons: Record<string, string> = {
						'Scoring Records': '🎯',
						'Championship Records': '🏆', 
						'Win/Loss Records': '⚔️',
						'Transaction Records': '💼',
						'Draft Records': '📋',
						'Season Records': '📅',
						'Other Records': '📊'
					}}
					
					<section class="bg-slate-800/30 rounded-xl border border-slate-700/30 overflow-hidden">
						<div class="bg-slate-800/50 px-6 py-4 border-b border-slate-700/50">
							<h3 class="text-xl font-bold text-white flex items-center">
								<span class="text-2xl mr-3">{categoryIcons[category] || '📊'}</span>
								{category}
								<span class="ml-auto text-sm font-normal text-slate-400">
									{categoryRecords.length} records
								</span>
							</h3>
						</div>
						
						<div class="p-6">
							<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
								{#each categoryRecords as record}
									<div class="bg-slate-700/20 hover:bg-slate-700/30 rounded-lg p-4 border border-slate-600/30 transition-colors">
										<div class="flex items-start justify-between">
											<div class="flex-1 min-w-0">
												<h4 class="font-semibold text-white mb-1 text-sm leading-tight">
													{record.record}
												</h4>
												<div class="flex items-baseline space-x-2">
													<span class="text-xl font-bold text-amber-400">
														{record.value}
													</span>
													{#if record.year}
														<span class="text-xs text-slate-400">
															({record.year})
														</span>
													{/if}
												</div>
												<div class="text-sm text-slate-300 font-medium mt-1 truncate">
													{record.holder}
												</div>
											</div>
											<div class="flex-shrink-0 ml-3">
												<Trophy class="w-5 h-5 text-amber-400/60" />
											</div>
										</div>
									</div>
								{/each}
							</div>
						</div>
					</section>
				{/each}

				<!-- Footer -->
				{#if recordBookData.lastUpdated}
					<div class="text-center text-slate-500 text-sm bg-slate-800/20 rounded-lg p-4">
						<div class="flex items-center justify-center space-x-2">
							<span>📊</span>
							<span>Last updated: {new Date(recordBookData.lastUpdated).toLocaleString()}</span>
						</div>
					</div>
				{/if}
			</div>
		{:else}
			<div class="text-center py-8">
				<div class="text-slate-400">No record book data available</div>
				<button 
					class="mt-4 px-4 py-2 bg-amber-500 text-black rounded hover:bg-amber-600"
					on:click={loadRecordBookData}
				>
					Load Record Book
				</button>
			</div>
		{/if}
	{/if}
</div> 
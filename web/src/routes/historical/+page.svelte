<script lang="ts">
	import { BarChart3, TrendingUp, Trophy, Users, Target, Calendar, Zap } from 'lucide-svelte';
	import { onMount } from 'svelte';
	import ClientOnlyD3Overview from '$lib/components/ClientOnlyD3Overview.svelte';
	import DraftBoard from '$lib/components/DraftBoard.svelte';
	import DraftAnalysis from '$lib/components/DraftAnalysis.svelte';
	
	let tradeData: any = null;
	let h2hData: any = null;
	let overviewData: any = null;
	let draftData: any = null;
	let specificSeasonDraft: any = null;
	let loading = true;
	let loadingTrades = false;
	let loadingH2H = false;
	let loadingOverview = false;
	let loadingDrafts = false;
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
		{ id: 'trades', name: 'Trade Analysis', icon: TrendingUp },
		{ id: 'drafts', name: 'Draft History', icon: Target },
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

	// Load trade data when trades tab is selected
	async function loadTradeData(season = 'all') {
		loadingTrades = true;
		try {
			const response = await fetch(`/api/trades?analysis=all&season=${season}`);
			if (response.ok) {
				tradeData = await response.json();
				console.log('Trade data loaded:', tradeData);
			} else {
				console.error('Failed to fetch trade data:', response.status, response.statusText);
			}
		} catch (err) {
			console.error('Error fetching trade data:', err);
		} finally {
			loadingTrades = false;
		}
	}

	// Handle season filter change
	function handleSeasonChange() {
		loadTradeData(selectedSeason);
	}

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

	// Helper function to get trade score color
	function getScoreColor(score: number): string {
		if (score >= 70) return 'text-green-400';
		if (score >= 55) return 'text-blue-400';
		if (score >= 45) return 'text-amber-400';
		return 'text-red-400';
	}

	// Helper function to get trade grade
	function getTradeGrade(score: number): string {
		if (score >= 85) return 'A+';
		if (score >= 80) return 'A';
		if (score >= 75) return 'A-';
		if (score >= 70) return 'B+';
		if (score >= 65) return 'B';
		if (score >= 60) return 'B-';
		if (score >= 55) return 'C+';
		if (score >= 50) return 'C';
		if (score >= 45) return 'C-';
		if (score >= 40) return 'D+';
		if (score >= 35) return 'D';
		return 'F';
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

	function getGradeColor(grade: string): string {
		if (grade.startsWith('A')) return 'text-green-400';
		if (grade.startsWith('B')) return 'text-blue-400';
		if (grade.startsWith('C')) return 'text-yellow-400';
		if (grade.startsWith('D')) return 'text-orange-400';
		return 'text-red-400';
	}

	// Calculate comprehensive manager analytics
	function getManagerAnalytics(drafts: any[]) {
		if (!drafts?.length) return [];
		
		const managerStats: any = {};
		
		drafts.forEach(pick => {
			const manager = pick.manager_name;
			const points = parseFloat(pick.season_points || 0);
			const pickNum = pick.overall_pick;
			
			if (!managerStats[manager]) {
				managerStats[manager] = {
					name: manager,
					picks: [],
					totalPoints: 0,
					totalPicks: 0
				};
			}
			
			managerStats[manager].picks.push({ points, pickNum, pick });
			managerStats[manager].totalPoints += points;
			managerStats[manager].totalPicks++;
		});
		
		return Object.values(managerStats).map((manager: any) => {
			const avgPoints = manager.totalPoints / manager.totalPicks;
			const avgPick = manager.picks.reduce((sum: number, p: any) => sum + p.pickNum, 0) / manager.picks.length;
			
			// Calculate steals (picks that performed way above expected value)
			const steals = manager.picks.filter((p: any) => {
				const expected = Math.max(180 - (p.pickNum * 2), 40);
				return p.points > expected + 50;
			}).length;
			
			// Calculate busts (picks that performed way below expected value)
			const busts = manager.picks.filter((p: any) => {
				const expected = Math.max(180 - (p.pickNum * 2), 40);
				return p.points < expected - 50;
			}).length;
			
			// Calculate hit rate (picks that exceeded or met expectations)
			const hits = manager.picks.filter((p: any) => {
				const expected = Math.max(180 - (p.pickNum * 2), 40);
				return p.points >= expected - 20;
			}).length;
			
			const hitRate = Math.round((hits / manager.totalPicks) * 100);
			
			// Best pick
			const bestPick = Math.min(...manager.picks.map((p: any) => p.pickNum));
			
			// Grade calculation
			let grade = 'F';
			if (avgPoints >= 120) grade = 'A';
			else if (avgPoints >= 100) grade = 'B';
			else if (avgPoints >= 80) grade = 'C';
			else if (avgPoints >= 60) grade = 'D';
			
			return {
				...manager,
				avgPick: Math.round(avgPick),
				avgPointsPerPick: avgPoints.toFixed(1),
				steals,
				busts,
				hitRate,
				bestPick,
				grade
			};
		}).sort((a, b) => parseFloat(b.avgPointsPerPick) - parseFloat(a.avgPointsPerPick));
	}

	// Find best early round drafter (rounds 1-3)
	function getBestEarlyRoundDrafter(drafts: any[]) {
		if (!drafts?.length) return null;
		
		const earlyPicks = drafts.filter(pick => pick.round_number <= 3);
		const managerStats: any = {};
		
		earlyPicks.forEach(pick => {
			const manager = pick.manager_name;
			const points = parseFloat(pick.season_points || 0);
			
			if (!managerStats[manager]) {
				managerStats[manager] = { name: manager, totalPoints: 0, picks: 0 };
			}
			
			managerStats[manager].totalPoints += points;
			managerStats[manager].picks++;
		});
		
		const results = Object.values(managerStats)
			.filter((m: any) => m.picks >= 5) // Need at least 5 early picks
			.map((m: any) => ({
				...m,
				avgPoints: (m.totalPoints / m.picks).toFixed(1)
			}))
			.sort((a: any, b: any) => parseFloat(b.avgPoints) - parseFloat(a.avgPoints));
		
		return results[0] || null;
	}

	// Find best value hunter
	function getBestValueHunter(drafts: any[]) {
		if (!drafts?.length) return null;
		
		const managerStats: any = {};
		
		drafts.forEach(pick => {
			const manager = pick.manager_name;
			const points = parseFloat(pick.season_points || 0);
			const pickNum = pick.overall_pick;
			const expected = Math.max(180 - (pickNum * 2), 40);
			const value = points - expected;
			
			if (!managerStats[manager]) {
				managerStats[manager] = { name: manager, steals: 0, totalValue: 0 };
			}
			
			if (value > 50) managerStats[manager].steals++;
			managerStats[manager].totalValue += value;
		});
		
		const results = Object.values(managerStats)
			.map((m: any) => ({
				...m,
				valueScore: Math.round(m.totalValue)
			}))
			.sort((a: any, b: any) => b.steals - a.steals);
		
		return results[0] || null;
	}

	// Find most consistent drafter
	function getMostConsistent(drafts: any[]) {
		if (!drafts?.length) return null;
		
		const managerStats: any = {};
		
		drafts.forEach(pick => {
			const manager = pick.manager_name;
			const points = parseFloat(pick.season_points || 0);
			const pickNum = pick.overall_pick;
			const expected = Math.max(180 - (pickNum * 2), 40);
			
			if (!managerStats[manager]) {
				managerStats[manager] = { name: manager, hits: 0, total: 0 };
			}
			
			if (points >= expected - 20) managerStats[manager].hits++;
			managerStats[manager].total++;
		});
		
		const results = Object.values(managerStats)
			.filter((m: any) => m.total >= 10) // Need at least 10 picks
			.map((m: any) => ({
				...m,
				consistency: Math.round((m.hits / m.total) * 100)
			}))
			.sort((a: any, b: any) => b.consistency - a.consistency);
		
		return results[0] || null;
	}

	// Load draft data
	async function loadDraftData() {
		if (draftData) return; // Already loaded
		
		loadingDrafts = true;
		try {
			const response = await fetch('/api/draft?analysis=all');
			if (response.ok) {
				draftData = await response.json();
				console.log('Draft data loaded:', draftData);
			} else {
				console.error('Failed to fetch draft data:', response.status, response.statusText);
			}
		} catch (err) {
			console.error('Error fetching draft data:', err);
		} finally {
			loadingDrafts = false;
		}
	}

	// Load specific season draft data
	async function loadSpecificSeasonDraft(season: string) {
		if (season === 'all') {
			specificSeasonDraft = null;
			return;
		}
		
		loadingDrafts = true;
		try {
			const response = await fetch(`/api/draft?season=${season}&analysis=all`);
			if (response.ok) {
				specificSeasonDraft = await response.json();
				console.log(`Draft data loaded for ${season}:`, specificSeasonDraft);
			} else {
				console.error('Failed to fetch season draft data:', response.status, response.statusText);
			}
		} catch (err) {
			console.error('Error fetching season draft data:', err);
		} finally {
			loadingDrafts = false;
		}
	}

	// Handle tab selection
	function handleTabSelect(tabId: string) {
		selectedTab = tabId;
		if (tabId === 'trades') {
			loadTradeData();
		} else if (tabId === 'head-to-head') {
			loadH2HData();
		} else if (tabId === 'overview') {
			loadOverviewData();
		} else if (tabId === 'drafts') {
			loadDraftData();
		}
	}

	// Reactive analytics calculations
	$: managerAnalytics = draftData?.data?.drafts ? getManagerAnalytics(draftData.data.drafts) : [];
	$: bestEarlyRoundDrafter = draftData?.data?.drafts ? getBestEarlyRoundDrafter(draftData.data.drafts) : null;
	$: bestValueHunter = draftData?.data?.drafts ? getBestValueHunter(draftData.data.drafts) : null;
	$: mostConsistent = draftData?.data?.drafts ? getMostConsistent(draftData.data.drafts) : null;
</script>

<div class="space-y-8">
	<!-- Page Header -->
	<div class="text-center py-8">
		<h1 class="text-5xl font-bold text-white mb-4">Historical Deep Dive</h1>
		<p class="text-xl text-slate-300 max-w-3xl mx-auto">
			Explore 14+ years of fantasy football history. Every trade, every draft pick, 
			every championship moment analyzed and visualized.
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

	{:else if selectedTab === 'trades'}
		{#if loadingTrades}
			<div class="flex items-center justify-center h-64">
				<div class="text-slate-400">Loading comprehensive trade analysis...</div>
			</div>
		{:else if tradeData}
			<div class="space-y-8">
				<!-- Season Filter -->
				<div class="flex items-center justify-between bg-slate-800/30 rounded-lg p-4">
					<div class="flex items-center space-x-4">
						<label class="text-slate-300 font-medium">Filter by Season:</label>
						<select 
							bind:value={selectedSeason} 
							on:change={handleSeasonChange}
							class="bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:border-amber-400 focus:outline-none"
						>
							<option value="all">All Seasons</option>
							<option value="2024">2024</option>
							<option value="2023">2023</option>
							<option value="2022">2022</option>
							<option value="2021">2021</option>
							<option value="2020">2020</option>
							<option value="2019">2019</option>
							<option value="2018">2018</option>
							<option value="2017">2017</option>
							<option value="2016">2016</option>
							<option value="2015">2015</option>
							<option value="2014">2014</option>
							<option value="2013">2013</option>
							<option value="2012">2012</option>
							<option value="2011">2011</option>
							<option value="2010">2010</option>
							<option value="2009">2009</option>
							<option value="2008">2008</option>
							<option value="2007">2007</option>
							<option value="2006">2006</option>
							<option value="2005">2005</option>
						</select>
					</div>
					<div class="text-slate-400 text-sm">
						{tradeData.meta?.total_returned || 0} trades • {selectedSeason === 'all' ? 'All seasons' : selectedSeason}
					</div>
				</div>

				<!-- Trade Analytics Overview -->
				{#if tradeData.analytics?.overview}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<BarChart3 class="w-6 h-6 text-blue-400 mr-3" />
							Trade Analytics Overview
						</h2>
						
						<div class="grid grid-cols-2 md:grid-cols-4 gap-6">
							<div class="text-center">
								<div class="text-3xl font-bold text-blue-400">{tradeData.analytics.overview.total_trades || 0}</div>
								<div class="text-slate-400">Total Trades</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-green-400">{tradeData.analytics.overview.decisive_trades || 0}</div>
								<div class="text-slate-400">Decisive Trades</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-amber-400">{tradeData.analytics.overview.even_trades || 0}</div>
								<div class="text-slate-400">Even Trades</div>
							</div>
							<div class="text-center">
								<div class="text-3xl font-bold text-purple-400">{tradeData.analytics.overview.championship_impact_trades || 0}</div>
								<div class="text-slate-400">Championship Impact</div>
							</div>
						</div>
					</section>
				{/if}

				<!-- Manager Trade Performance -->
				{#if tradeData.analytics?.managers}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<Users class="w-6 h-6 text-green-400 mr-3" />
							Manager Trade Performance
						</h2>
						
						<div class="overflow-x-auto">
							<table class="w-full">
								<thead>
									<tr class="border-b border-slate-700">
										<th class="text-left py-3 px-4 text-slate-300">Manager</th>
										<th class="text-center py-3 px-4 text-slate-300">Trades</th>
										<th class="text-center py-3 px-4 text-slate-300">Win %</th>
										<th class="text-center py-3 px-4 text-slate-300">Avg Score</th>
										<th class="text-center py-3 px-4 text-slate-300">Championships</th>
									</tr>
								</thead>
								<tbody>
									{#each tradeData.analytics.managers as manager}
										<tr class="border-b border-slate-700/50">
											<td class="py-3 px-4 font-bold text-white">{manager.manager_name}</td>
											<td class="text-center py-3 px-4 text-slate-300">{manager.total_trades}</td>
											<td class="text-center py-3 px-4">
												<span class="font-bold {manager.win_percentage >= 60 ? 'text-green-400' : 
													manager.win_percentage >= 40 ? 'text-amber-400' : 'text-red-400'}">
													{manager.win_percentage || 0}%
												</span>
											</td>
											<td class="text-center py-3 px-4">
												<span class="font-mono {getScoreColor(manager.avg_trade_score || 0)}">
													{manager.avg_trade_score || 0}
												</span>
											</td>
											<td class="text-center py-3 px-4">
												{#if manager.championship_trades > 0}
													<span class="text-amber-400 font-bold">🏆 {manager.championship_trades}</span>
												{:else}
													<span class="text-slate-500">-</span>
												{/if}
											</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</section>
				{/if}

				<div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
					<!-- Best Trades -->
					{#if tradeData.analytics?.best_trades}
						<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
							<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
								<TrendingUp class="w-6 h-6 text-green-400 mr-3" />
								Most Decisive Trades
							</h2>
							
							<div class="space-y-4">
								{#each tradeData.analytics.best_trades as trade}
									{@const winnerScore = parseFloat(trade.team_a_final_score || 0) > parseFloat(trade.team_b_final_score || 0) ? parseFloat(trade.team_a_final_score || 0) : parseFloat(trade.team_b_final_score || 0)}
									{@const loserScore = parseFloat(trade.team_a_final_score || 0) < parseFloat(trade.team_b_final_score || 0) ? parseFloat(trade.team_a_final_score || 0) : parseFloat(trade.team_b_final_score || 0)}
									<div class="bg-green-500/10 border border-green-500/20 rounded-lg p-4">
										<div class="flex items-center justify-between mb-2">
											<span class="font-bold text-white">{trade.trade_winner}</span>
											<div class="flex items-center space-x-2">
												<span class="bg-green-500 text-black px-2 py-1 rounded text-sm font-bold">
													{getTradeGrade(winnerScore)}
												</span>
												<span class="text-xs text-slate-400">{trade.season_year}</span>
											</div>
										</div>
										<div class="text-sm text-slate-300 mb-2">
											<span class="text-blue-400">{trade.team_a_manager}:</span> {trade.team_a_gives}
										</div>
										<div class="text-sm text-slate-300 mb-2">
											<span class="text-purple-400">{trade.team_b_manager}:</span> {trade.team_b_gives}
										</div>
										<div class="flex justify-between text-xs">
											<span class="text-slate-500">Week {trade.transaction_week}</span>
											<span class="text-green-400 font-bold">
												{winnerScore.toFixed(1)} vs {loserScore.toFixed(1)}
											</span>
										</div>
										<div class="text-xs text-slate-400 mt-2">{trade.trade_analysis}</div>
									</div>
								{/each}
							</div>
						</section>
					{/if}

					<!-- Championship Impact Trades -->
					{#if tradeData.analytics?.championship_trades}
						<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
							<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
								<Trophy class="w-6 h-6 text-amber-400 mr-3" />
								Championship Impact Trades
							</h2>
							
							<div class="space-y-4">
								{#each tradeData.analytics.championship_trades as trade}
									{@const champion = trade.team_a_champion ? trade.team_a_manager : trade.team_b_manager}
									<div class="bg-amber-500/10 border border-amber-500/20 rounded-lg p-4">
										<div class="flex items-center justify-between mb-2">
											<span class="font-bold text-white">{champion} 🏆</span>
											<div class="flex items-center space-x-2">
												<span class="bg-amber-500 text-black px-2 py-1 rounded text-sm font-bold">
													CHAMP
												</span>
												<span class="text-xs text-slate-400">{trade.season_year}</span>
											</div>
										</div>
										<div class="text-sm text-slate-300 mb-2">
											<span class="text-blue-400">{trade.team_a_manager}:</span> {trade.team_a_gives}
										</div>
										<div class="text-sm text-slate-300 mb-2">
											<span class="text-purple-400">{trade.team_b_manager}:</span> {trade.team_b_gives}
										</div>
										<div class="flex justify-between text-xs">
											<span class="text-slate-500">Week {trade.transaction_week}</span>
											<span class="text-amber-400 font-bold">
												Production: {parseFloat(trade.production_differential || 0) > 0 ? '+' : ''}{parseFloat(trade.production_differential || 0).toFixed(1)}
											</span>
										</div>
										<div class="text-xs text-slate-400 mt-2">{trade.trade_analysis}</div>
									</div>
								{/each}
							</div>
						</section>
					{/if}
				</div>

				<!-- Recent Trades -->
				{#if tradeData.trades}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<Calendar class="w-6 h-6 text-blue-400 mr-3" />
							Recent Trade History
						</h2>
						
						<div class="space-y-3">
							{#each tradeData.trades.slice(0, 10) as trade}
								<div class="bg-slate-700/30 rounded-lg p-4 border-l-4 
									{trade.trade_winner === 'Even Trade' ? 'border-amber-400' :
									 trade.team_a_final_score > trade.team_b_final_score ? 'border-green-400' : 'border-blue-400'}">
									<div class="flex items-center justify-between mb-2">
										<div class="flex items-center space-x-3">
											<span class="text-white font-bold">{trade.trade_type}</span>
											<span class="text-slate-400 text-sm">Week {trade.transaction_week}, {trade.season_year}</span>
										</div>
										<span class="text-xs px-2 py-1 rounded
											{trade.trade_winner === 'Even Trade' ? 'bg-amber-500/20 text-amber-300' :
											 'bg-green-500/20 text-green-300'}">
											{trade.trade_winner}
										</span>
									</div>
									
									<div class="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
										<div>
											<span class="text-blue-400 font-medium">{trade.team_a_manager}:</span>
											<span class="text-slate-300">{trade.team_a_gives}</span>
											<div class="text-xs text-slate-500 mt-1">
												Score: {parseFloat(trade.team_a_final_score || 0).toFixed(1)} 
												{#if trade.team_a_made_playoffs} | Made Playoffs{/if}
												{#if trade.team_a_champion} | 🏆 Champion{/if}
											</div>
										</div>
										<div>
											<span class="text-purple-400 font-medium">{trade.team_b_manager}:</span>
											<span class="text-slate-300">{trade.team_b_gives}</span>
											<div class="text-xs text-slate-500 mt-1">
												Score: {parseFloat(trade.team_b_final_score || 0).toFixed(1)}
												{#if trade.team_b_made_playoffs} | Made Playoffs{/if}
												{#if trade.team_b_champion} | 🏆 Champion{/if}
											</div>
										</div>
									</div>
									
									{#if parseFloat(trade.production_differential || 0) !== 0}
										<div class="mt-2 text-xs text-slate-400">
											Production Impact: {parseFloat(trade.production_differential || 0) > 0 ? '+' : ''}{parseFloat(trade.production_differential || 0).toFixed(1)} points
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
				<div class="text-slate-400">No trade data available</div>
			</div>
		{/if}

	{:else if selectedTab === 'drafts'}
		<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
			<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
				<Target class="w-6 h-6 text-purple-400 mr-3" />
				Draft History & Analysis
			</h2>
			
			{#if loadingDrafts}
				<div class="flex items-center justify-center py-8">
					<div class="text-blue-400">Loading draft data...</div>
				</div>
			{:else if draftData?.data?.available_seasons}
				<div class="space-y-6">
					<div class="text-sm text-gray-400 mb-4">
						{draftData.data.available_seasons.length} seasons available ({draftData.data.available_seasons[draftData.data.available_seasons.length - 1]} - {draftData.data.available_seasons[0]})
					</div>
					
					<!-- Season Selector -->
					<div class="mb-6">
						<label class="block text-sm font-medium text-gray-300 mb-2">Select Season</label>
						<select 
							bind:value={selectedSeason} 
							class="bg-gray-700 border border-gray-600 text-white rounded-lg px-4 py-2 w-48"
							on:change={() => loadSpecificSeasonDraft(selectedSeason)}
						>
							<option value="all">All Seasons Overview</option>
							{#each draftData.data.available_seasons as season}
								<option value={season}>{season} Season</option>
							{/each}
						</select>
					</div>
					
					<!-- Draft Overview -->
					{#if selectedSeason === 'all'}
						<div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
							<div class="bg-gray-700 p-4 rounded-lg">
								<div class="text-2xl font-bold text-blue-400">{draftData.data.meta.total_drafts}</div>
								<div class="text-sm text-gray-400">Total Picks</div>
							</div>
							<div class="bg-gray-700 p-4 rounded-lg">
								<div class="text-2xl font-bold text-green-400">{draftData.data.available_seasons.length}</div>
								<div class="text-sm text-gray-400">Seasons</div>
							</div>
						</div>
						
						<!-- Manager Draft Performance Summary -->
						<div class="space-y-6">
							<!-- Top-Level Manager Analytics -->
							<div class="bg-gray-700 p-4 rounded-lg">
								<h4 class="text-lg font-semibold text-white mb-4">Manager Draft Performance (Career)</h4>
								<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
									{#each managerAnalytics as manager}
										<div class="bg-gray-600 p-4 rounded-lg">
											<div class="flex items-center justify-between mb-2">
												<span class="font-bold text-white">{manager.name}</span>
												<span class="text-lg font-bold {getGradeColor(manager.grade)}">{manager.grade}</span>
											</div>
											<div class="grid grid-cols-2 gap-2 text-xs">
												<div>
													<div class="text-gray-400">Avg Pick</div>
													<div class="font-bold text-blue-400">#{manager.avgPick}</div>
												</div>
												<div>
													<div class="text-gray-400">Steals</div>
													<div class="font-bold text-green-400">{manager.steals}</div>
												</div>
												<div>
													<div class="text-gray-400">Busts</div>
													<div class="font-bold text-red-400">{manager.busts}</div>
												</div>
												<div>
													<div class="text-gray-400">Avg PPP</div>
													<div class="font-bold text-purple-400">{manager.avgPointsPerPick}</div>
												</div>
											</div>
											<div class="mt-2 pt-2 border-t border-gray-500">
												<div class="flex justify-between text-xs">
													<span class="text-gray-400">Best Pick:</span>
													<span class="text-amber-400">#{manager.bestPick}</span>
												</div>
												<div class="flex justify-between text-xs">
													<span class="text-gray-400">Hit Rate:</span>
													<span class="text-green-400">{manager.hitRate}%</span>
												</div>
											</div>
										</div>
									{/each}
								</div>
							</div>

							<!-- Draft Efficiency Metrics -->
							<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
								<div class="bg-gray-700 p-4 rounded-lg">
									<h5 class="text-md font-semibold text-white mb-3">🎯 Best Early Round Drafter</h5>
									{#if bestEarlyRoundDrafter}
										<div class="text-center">
											<div class="text-xl font-bold text-green-400">{bestEarlyRoundDrafter.name}</div>
											<div class="text-sm text-gray-400">{bestEarlyRoundDrafter.avgPoints} pts/pick (Rounds 1-3)</div>
											<div class="text-xs text-gray-500">{bestEarlyRoundDrafter.picks} early picks analyzed</div>
										</div>
									{/if}
								</div>

								<div class="bg-gray-700 p-4 rounded-lg">
									<h5 class="text-md font-semibold text-white mb-3">💎 Best Value Hunter</h5>
									{#if bestValueHunter}
										<div class="text-center">
											<div class="text-xl font-bold text-blue-400">{bestValueHunter.name}</div>
											<div class="text-sm text-gray-400">{bestValueHunter.steals} steals found</div>
											<div class="text-xs text-gray-500">{bestValueHunter.valueScore} value score</div>
										</div>
									{/if}
								</div>

								<div class="bg-gray-700 p-4 rounded-lg">
									<h5 class="text-md font-semibold text-white mb-3">🎲 Most Consistent</h5>
									{#if mostConsistent}
										<div class="text-center">
											<div class="text-xl font-bold text-purple-400">{mostConsistent.name}</div>
											<div class="text-sm text-gray-400">{mostConsistent.consistency}% hit rate</div>
											<div class="text-xs text-gray-500">Lowest bust rate</div>
										</div>
									{/if}
								</div>
							</div>
						</div>
					{:else if specificSeasonDraft?.data?.draft_board}
						<!-- Draft Board Recreation -->
						<DraftBoard 
							draftData={specificSeasonDraft.data.draft_board}
							season={selectedSeason}
							numTeams={specificSeasonDraft.data.draft_board[0]?.num_teams || 10}
						/>
						
						<!-- Draft Analysis -->
						{#if specificSeasonDraft.data}
							<div class="mt-6">
								<DraftAnalysis 
									analytics={specificSeasonDraft.data}
									season={selectedSeason}
								/>
							</div>
						{/if}
					{:else}
						<div class="text-center py-8">
							<div class="text-gray-400">
								{loadingDrafts ? 'Loading draft board...' : `Select ${selectedSeason} and wait for draft board to load`}
							</div>
							<div class="text-sm text-gray-500 mt-2">Snake-style draft board with performance analysis</div>
						</div>
					{/if}
				</div>
			{:else}
				<div class="text-center py-8">
					<div class="text-gray-400">No draft data available</div>
					<button 
						on:click={loadDraftData}
						class="mt-4 bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded-lg text-sm"
					>
						Retry Loading
					</button>
				</div>
			{/if}
		</section>

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
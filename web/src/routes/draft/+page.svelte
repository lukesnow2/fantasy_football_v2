<script lang="ts">
	import { Trophy, Target, BarChart3, Calendar, Zap } from 'lucide-svelte';
	import { onMount } from 'svelte';
	import DraftBoard from '$lib/components/DraftBoard.svelte';
	import DraftAnalysis from '$lib/components/DraftAnalysis.svelte';
	
	let draftData: any = null;
	let specificSeasonDraft: any = null;
	let loading = true;
	let loadingDrafts = false;
	let selectedSeason = 'all';

	onMount(async () => {
		loading = false;
		loadDraftData();
	});

	// Helper function to get grade color
	function getGradeColor(grade: string): string {
		if (grade.startsWith('A')) return 'text-green-400';
		if (grade.startsWith('B')) return 'text-blue-400';
		if (grade.startsWith('C')) return 'text-yellow-400';
		if (grade.startsWith('D')) return 'text-orange-400';
		return 'text-red-400';
	}

	// Data-driven expected points per draft slot (average across all drafts), so a pick's
	// value is judged against how that slot actually performs historically — not a
	// fabricated fixed curve. Returns expectedAt(pickNum) -> avg points (0 if unknown).
	function buildSlotBaseline(drafts: any[]) {
		const slotTotals: Record<number, { sum: number; n: number }> = {};
		(drafts || []).forEach((p: any) => {
			const n = p.overallPick;
			const pts = parseFloat(p.seasonPoints || 0);
			if (!slotTotals[n]) slotTotals[n] = { sum: 0, n: 0 };
			slotTotals[n].sum += pts;
			slotTotals[n].n++;
		});
		return (pickNum: number) =>
			slotTotals[pickNum] && slotTotals[pickNum].n > 0
				? slotTotals[pickNum].sum / slotTotals[pickNum].n
				: 0;
	}

	// Calculate comprehensive manager analytics
	function getManagerAnalytics(drafts: any[]) {
		if (!drafts?.length) return [];

		const expectedAt = buildSlotBaseline(drafts);

		const managerStats: any = {};

		drafts.forEach(pick => {
			const manager = pick.managerName;
			const points = parseFloat(pick.seasonPoints || 0);
			const pickNum = pick.overallPick;
			
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
			
			// Steals/busts/hits judged against the slot's historical average.
			const steals = manager.picks.filter((p: any) => {
				const expected = expectedAt(p.pickNum);
				return expected > 0 && p.points > expected * 1.5;
			}).length;

			const busts = manager.picks.filter((p: any) => {
				const expected = expectedAt(p.pickNum);
				return expected > 0 && p.points < expected * 0.5;
			}).length;

			const hits = manager.picks.filter((p: any) => {
				const expected = expectedAt(p.pickNum);
				return expected > 0 && p.points >= expected * 0.8;
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
		
		const earlyPicks = drafts.filter(pick => pick.roundNumber <= 3);
		const managerStats: any = {};
		
		earlyPicks.forEach(pick => {
			const manager = pick.managerName;
			const points = parseFloat(pick.seasonPoints || 0);
			
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

		const expectedAt = buildSlotBaseline(drafts);
		const managerStats: any = {};

		drafts.forEach(pick => {
			const manager = pick.managerName;
			const points = parseFloat(pick.seasonPoints || 0);
			const expected = expectedAt(pick.overallPick);
			const value = expected > 0 ? points - expected : 0;

			if (!managerStats[manager]) {
				managerStats[manager] = { name: manager, steals: 0, totalValue: 0 };
			}

			if (expected > 0 && points > expected * 1.5) managerStats[manager].steals++;
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

		const expectedAt = buildSlotBaseline(drafts);
		const managerStats: any = {};

		drafts.forEach(pick => {
			const manager = pick.managerName;
			const points = parseFloat(pick.seasonPoints || 0);
			const expected = expectedAt(pick.overallPick);

			if (!managerStats[manager]) {
				managerStats[manager] = { name: manager, hits: 0, total: 0 };
			}

			if (expected > 0 && points >= expected * 0.8) managerStats[manager].hits++;
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
			} else {
				console.error('Failed to fetch season draft data:', response.status, response.statusText);
			}
		} catch (err) {
			console.error('Error fetching season draft data:', err);
		} finally {
			loadingDrafts = false;
		}
	}

	// Reactive analytics calculations
	$: managerAnalytics = draftData?.data?.drafts ? getManagerAnalytics(draftData.data.drafts) : [];
	$: bestEarlyRoundDrafter = draftData?.data?.drafts ? getBestEarlyRoundDrafter(draftData.data.drafts) : null;
	$: bestValueHunter = draftData?.data?.drafts ? getBestValueHunter(draftData.data.drafts) : null;
	$: mostConsistent = draftData?.data?.drafts ? getMostConsistent(draftData.data.drafts) : null;
	$: draftValue = draftData?.data?.draftValue ?? null;
</script>

<svelte:head>
	<title>Draft Central - The League</title>
</svelte:head>

<div class="space-y-8">
	<!-- Page Header -->
	<div class="text-center py-8">
		<Trophy class="h-16 w-16 text-amber-400 mx-auto mb-4" />
		<h1 class="text-4xl font-bold text-white mb-4">Draft Central</h1>
		<p class="text-xl text-slate-300 max-w-3xl mx-auto">
			Complete draft history and analysis. Explore draft boards, manager performance, and value picks across all seasons.
		</p>
	</div>

	<!-- Draft Content -->
	<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
		<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
			<Target class="w-6 h-6 text-purple-400 mr-3" />
			Draft History & Analysis
		</h2>
		
		{#if loadingDrafts}
			<div class="flex items-center justify-center py-8">
				<div class="text-blue-400">Loading draft data...</div>
			</div>
		{:else if draftData?.data}
			<div class="space-y-6">
				<div class="text-sm text-gray-400 mb-4">
					{draftData.data.availableSeasons?.length || 0} seasons available 
					{draftData.data.availableSeasons?.length > 0 ? `(${draftData.data.availableSeasons[draftData.data.availableSeasons.length - 1]} - ${draftData.data.availableSeasons[0]})` : ''}
				</div>
				
				<!-- Season Selector -->
				<div class="mb-6">
					<label for="season-select" class="block text-sm font-medium text-gray-300 mb-2">Select Season</label>
					<select 
						id="season-select"
						bind:value={selectedSeason} 
						class="bg-gray-700 border border-gray-600 text-white rounded-lg px-4 py-2 w-48"
						on:change={() => loadSpecificSeasonDraft(selectedSeason)}
					>
						<option value="all">All Seasons Overview</option>
						{#each draftData.data.availableSeasons as season}
							<option value={season}>{season} Season</option>
						{/each}
					</select>
				</div>
				
				<!-- Draft Overview -->
				{#if selectedSeason === 'all'}
					<div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
						<div class="bg-gray-700 p-4 rounded-lg">
							<div class="text-2xl font-bold text-blue-400">{draftData.data.meta.totalDrafts}</div>
							<div class="text-sm text-gray-400">Total Picks</div>
						</div>
						<div class="bg-gray-700 p-4 rounded-lg">
							<div class="text-2xl font-bold text-green-400">{draftData.data.availableSeasons.length}</div>
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
												<div class="text-gray-400">
													<!-- No tooltip: steals, busts and points-per-pick have no row in
													     meta_data.metric_definitions, and the nearest seeded metric
													     (draft_value_score) measures something else. A plain label
													     beats a tooltip that reads "No definition available". -->
													Steals
												</div>
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

					<!-- Draft Value: value over positional replacement (VOR) -->
					{#if draftValue}
						<div class="bg-gray-700 p-4 rounded-lg mt-6">
							<h4 class="text-lg font-semibold text-white mb-1">Draft Value (Value Over Replacement)</h4>
							<p class="text-xs text-gray-400 mb-4">{draftValue.methodology}</p>

							<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
								<div>
									<h5 class="text-sm font-semibold text-green-400 mb-2">Best Value Picks</h5>
									<div class="space-y-1">
										{#each draftValue.bestValue.slice(0, 8) as p}
											<div class="flex items-center justify-between text-sm bg-gray-600/50 rounded px-3 py-1.5">
												<span class="text-white truncate">
													{p.playerName}
													<span class="text-gray-400">· {p.position} · {p.seasonYear} · R{p.roundNumber}</span>
												</span>
												<span class="text-green-400 font-bold ml-2 whitespace-nowrap">+{p.vor}</span>
											</div>
										{/each}
									</div>
								</div>

								<div>
									<h5 class="text-sm font-semibold text-red-400 mb-2">Biggest Busts (rounds 1–5)</h5>
									<div class="space-y-1">
										{#each draftValue.biggestBusts.slice(0, 8) as p}
											<div class="flex items-center justify-between text-sm bg-gray-600/50 rounded px-3 py-1.5">
												<span class="text-white truncate">
													{p.playerName}
													<span class="text-gray-400">· {p.position} · {p.seasonYear} · R{p.roundNumber}</span>
												</span>
												<span class="text-red-400 font-bold ml-2 whitespace-nowrap">{p.vor}</span>
											</div>
										{/each}
									</div>
								</div>
							</div>

							{#if draftValue.byManager?.length}
								<h5 class="text-sm font-semibold text-blue-400 mt-6 mb-2">Best Drafters (avg VOR per pick)</h5>
								<div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
									{#each draftValue.byManager.slice(0, 5) as m}
										<div class="bg-gray-600/50 rounded p-3 text-center">
											<div class="font-bold text-white truncate">{m.managerName}</div>
											<div class="text-blue-400 font-bold">{m.avgVor > 0 ? '+' : ''}{m.avgVor}</div>
											<div class="text-xs text-gray-400">{m.picks} picks</div>
										</div>
									{/each}
								</div>
							{/if}
						</div>
					{/if}
				{:else if specificSeasonDraft?.data?.draftBoard}
					<!-- Draft Board Recreation -->
					<DraftBoard 
						draftData={specificSeasonDraft.data.draftBoard}
						season={selectedSeason}
						numTeams={specificSeasonDraft.data.draftBoard[0]?.numTeams || specificSeasonDraft.data.draftBoard.filter((p: any) => p.roundNumber === 1).length || 10}
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
</div> 
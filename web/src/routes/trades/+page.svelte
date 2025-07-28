<script lang="ts">
	import { TrendingUp, Trophy, Calendar, BarChart3, Target } from 'lucide-svelte';
	import { onMount } from 'svelte';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	
	let tradeData: any = null;
	let loading = true;
	let loadingTrades = false;
	let selectedSeason = 'all';

	onMount(async () => {
		loading = false;
		loadTradeData();
	});

	// Load trade data
	async function loadTradeData(season = 'all') {
		loadingTrades = true;
		try {
			const response = await fetch(`/api/trades?analysis=all&season=${season}`);
			if (response.ok) {
				tradeData = await response.json();
				console.log('Trade data loaded:', tradeData);
				
				// Debug analytics data
				if (tradeData.analytics) {
					console.log('Analytics overview:', tradeData.analytics.overview);
					console.log('Best trades count:', tradeData.analytics.bestTrades?.length);
					console.log('Championship trades count:', tradeData.analytics.championshipTrades?.length);
					if (tradeData.analytics.bestTrades?.length > 0) {
						console.log('First best trade:', tradeData.analytics.bestTrades[0]);
						console.log('Best trade fields:', Object.keys(tradeData.analytics.bestTrades[0]));
					}
					
				}
				
				// Debug trades data
				if (tradeData.trades) {
					console.log('Trades count:', tradeData.trades.length);
					if (tradeData.trades.length > 0) {
						console.log('First trade:', tradeData.trades[0]);
						console.log('Trade fields:', Object.keys(tradeData.trades[0]));
					}
				}
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
</script>

<svelte:head>
	<title>Trade Center - The League</title>
</svelte:head>

<div class="space-y-8">
	<!-- Page Header -->
	<div class="text-center py-8">
		<Target class="h-16 w-16 text-amber-400 mx-auto mb-4" />
		<h1 class="text-4xl font-bold text-white mb-4">Trade Center</h1>
		<p class="text-xl text-slate-300 max-w-3xl mx-auto">
			Complete trade analysis and history. Explore deal winners, championship impact trades, 
			and comprehensive transaction analytics across all seasons.
		</p>
	</div>

	<!-- Trade Analysis Content -->
	{#if loadingTrades}
		<div class="flex items-center justify-center h-64">
			<div class="text-slate-400">Loading comprehensive trade analysis...</div>
		</div>
	{:else if tradeData}
		<div class="space-y-8">
			<!-- Season Filter -->
			<div class="flex items-center justify-between bg-slate-800/30 rounded-lg p-4">
				<div>
					<label for="season-filter" class="text-slate-300 font-medium">Filter by Season:</label>
					<select 
						id="season-filter"
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
					{tradeData.meta?.totalReturned || 0} trades • {selectedSeason === 'all' ? 'All seasons' : selectedSeason}
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
							<div class="text-3xl font-bold text-blue-400">{tradeData.analytics.overview.totalTrades || 0}</div>
							<div class="text-slate-400">Total Trades</div>
						</div>
						<div class="text-center">
							<div class="text-3xl font-bold text-green-400">{tradeData.analytics.overview.avgTradesPerSeason ? parseFloat(tradeData.analytics.overview.avgTradesPerSeason).toFixed(1) : (parseInt(tradeData.analytics.overview.totalTrades) / 20).toFixed(1)}</div>
							<div class="text-slate-400">Per Season</div>
						</div>
						<div class="text-center">
							<div class="text-3xl font-bold text-amber-400">{tradeData.analytics.overview.championshipImpactTrades || 0}</div>
							<div class="text-slate-400">Championship Impact</div>
						</div>
						<div class="text-center">
							<div class="text-3xl font-bold text-purple-400">{parseFloat(tradeData.analytics.overview.avgProductionImpact || 0).toFixed(1)}</div>
							<div class="text-slate-400">
								<MetricHelp metricId="trade_production_differential" position="top" theme="dark" className="text-slate-400" showIcon={false}>
									Avg Impact
								</MetricHelp>
							</div>
						</div>
					</div>
				</section>
			{/if}

			<div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
							<!-- Best Trades -->
			{#if tradeData.analytics?.bestTrades}
				<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
					<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
						<TrendingUp class="w-6 h-6 text-green-400 mr-3" />
						Most Decisive Trades ({tradeData.analytics.bestTrades.length})
					</h2>
					
					<div class="space-y-4">
						{#each tradeData.analytics.bestTrades as trade, index}
								{@const winnerScore = parseFloat(trade.teamAFinalScore || 0) > parseFloat(trade.teamBFinalScore || 0) ? parseFloat(trade.teamAFinalScore || 0) : parseFloat(trade.teamBFinalScore || 0)}
								{@const loserScore = parseFloat(trade.teamAFinalScore || 0) < parseFloat(trade.teamBFinalScore || 0) ? parseFloat(trade.teamAFinalScore || 0) : parseFloat(trade.teamBFinalScore || 0)}
								<!-- Debug trade {index}: {trade.tradeWinner} vs {trade.teamAManager} -->
								<div class="bg-green-500/10 border border-green-500/20 rounded-lg p-4">
									<div class="flex items-center justify-between mb-2">
										<span class="font-bold text-white">{trade.tradeWinner}</span>
										<div class="flex items-center space-x-2">
											<span class="bg-green-500 text-white px-2 py-1 rounded text-sm font-bold">
												+{parseFloat(trade.productionDifferential || 0).toFixed(1)}
											</span>
											<span class="text-xs text-slate-400">{trade.seasonYear}</span>
										</div>
									</div>
									<div class="text-sm text-slate-300 mb-2">
										<span class="text-blue-400">{trade.teamAManager}:</span> {trade.teamAGives}
									</div>
									<div class="text-sm text-slate-300 mb-2">
										<span class="text-purple-400">{trade.teamBManager}:</span> {trade.teamBGives}
									</div>
									<div class="flex justify-between text-xs">
										<span class="text-slate-500">Week {trade.transactionWeek}</span>
										<span class="text-green-400 font-bold">
											Final: {winnerScore.toFixed(1)} - {loserScore.toFixed(1)}
										</span>
									</div>
									<div class="text-xs text-slate-400 mt-2">{trade.tradeAnalysis}</div>
								</div>
							{/each}
						</div>
					</section>
				{/if}

				<!-- Championship Impact Trades -->
				{#if tradeData.analytics?.championshipTrades}
					<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
						<h2 class="text-2xl font-bold text-white mb-6 flex items-center">
							<Trophy class="w-6 h-6 text-amber-400 mr-3" />
							Championship Impact Trades
						</h2>
						
						<div class="text-sm text-slate-400 mb-4">
							Showing trades with positive 50+ point production differentials that involved championship winners
						</div>
						
						<div class="space-y-4">
							{#each tradeData.analytics.championshipTrades as trade}
								{@const champion = trade.teamAChampion ? trade.teamAManager : trade.teamBManager}
								<div class="bg-amber-500/10 border border-amber-500/20 rounded-lg p-4">
									<div class="flex items-center justify-between mb-2">
										<span class="font-bold text-white">{champion} 🏆</span>
										<div class="flex items-center space-x-2">
											<span class="bg-amber-500 text-black px-2 py-1 rounded text-sm font-bold">
												CHAMP
											</span>
											<span class="text-xs text-slate-400">{trade.seasonYear}</span>
										</div>
									</div>
									<div class="text-sm text-slate-300 mb-2">
										<span class="text-blue-400">{trade.teamAManager}:</span> {trade.teamAGives}
									</div>
									<div class="text-sm text-slate-300 mb-2">
										<span class="text-purple-400">{trade.teamBManager}:</span> {trade.teamBGives}
									</div>
									<div class="flex justify-between text-xs">
										<span class="text-slate-500">Week {trade.transactionWeek}</span>
										<span class="text-amber-400 font-bold">
											Production: {parseFloat(trade.productionDifferential || 0) > 0 ? '+' : ''}{parseFloat(trade.productionDifferential || 0).toFixed(1)}
										</span>
									</div>
									<div class="text-xs text-slate-400 mt-2">{trade.tradeAnalysis}</div>
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
						Recent Trade History ({tradeData.trades.length} total)
					</h2>
					
					<div class="space-y-3">
						{#each tradeData.trades.slice(0, 10) as trade, index}
							<!-- Debug trade {index}: {trade.tradeWinner} vs {trade.teamAManager} -->
							<div class="bg-slate-700/30 rounded-lg p-4 border-l-4 
								{trade.tradeWinner === 'Even Trade' ? 'border-amber-400' :
								 trade.teamAFinalScore > trade.teamBFinalScore ? 'border-green-400' : 'border-blue-400'}">
								<div class="flex items-center justify-between mb-2">
									<div class="flex items-center space-x-3">
										<span class="text-white font-bold">{trade.tradeType}</span>
										<span class="text-slate-400 text-sm">Week {trade.transactionWeek}, {trade.seasonYear}</span>
									</div>
									<span class="text-xs px-2 py-1 rounded
										{trade.tradeWinner === 'Even Trade' ? 'bg-amber-500 bg-opacity-20 text-amber-300' :
										 'bg-green-500 bg-opacity-20 text-green-300'}">
										{trade.tradeWinner}
									</span>
								</div>
								
								<div class="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
									<div>
										<span class="text-blue-400 font-medium">{trade.teamAManager}:</span>
										<span class="text-slate-300">{trade.teamAGives}</span>
										<div class="text-xs text-slate-500 mt-1">
											Score: {parseFloat(trade.teamAFinalScore || 0).toFixed(1)} 
											{#if trade.teamAMadePlayoffs} | Made Playoffs{/if}
											{#if trade.teamAChampion} | 🏆 Champion{/if}
										</div>
									</div>
									<div>
										<span class="text-purple-400 font-medium">{trade.teamBManager}:</span>
										<span class="text-slate-300">{trade.teamBGives}</span>
										<div class="text-xs text-slate-500 mt-1">
											Score: {parseFloat(trade.teamBFinalScore || 0).toFixed(1)}
											{#if trade.teamBMadePlayoffs} | Made Playoffs{/if}
											{#if trade.teamBChampion} | 🏆 Champion{/if}
										</div>
									</div>
								</div>
								
								{#if parseFloat(trade.productionDifferential || 0) !== 0}
									<div class="mt-2 text-xs text-slate-400">
										Production Impact: {parseFloat(trade.productionDifferential || 0) > 0 ? '+' : ''}{parseFloat(trade.productionDifferential || 0).toFixed(1)} points
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
			<button 
				on:click={() => loadTradeData()}
				class="mt-4 bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded-lg text-sm"
			>
				Retry Loading
			</button>
		</div>
	{/if}
</div> 
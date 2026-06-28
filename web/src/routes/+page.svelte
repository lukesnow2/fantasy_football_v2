<script lang="ts">
	import { Trophy, TrendingUp, Users, Zap, Calendar, BarChart3 } from 'lucide-svelte';
	import { onMount } from 'svelte';
	
	// Real data from API
	let leagueOverview: any = {};
	let loading = true;

	onMount(async () => {
		try {
			const response = await fetch('/api/leagues/overview');
			if (response.ok) {
				const data = await response.json();
				leagueOverview = data.overview;
			}
			loading = false;
		} catch (err) {
			console.error('Error fetching league overview:', err);
			loading = false;
		}
	});
</script>

<div class="space-y-12">
	<!-- Hero Section -->
	<section class="text-center py-12">
		<div class="max-w-4xl mx-auto">
			<Trophy class="h-20 w-20 text-amber-400 mx-auto mb-6" />
			<h1 class="text-5xl font-bold text-white mb-6">
				Welcome to <span class="text-amber-400">The League</span>
			</h1>
			<p class="text-xl text-slate-300 mb-8 leading-relaxed">
				Your fantasy football dynasty's digital home. Dive deep into years of epic battles, 
				legendary trades, and championship glory. This is where fantasy becomes legacy.
			</p>
			<div class="flex flex-col sm:flex-row gap-4 justify-center">
				<a 
					href="/this-season" 
					class="bg-amber-500 hover:bg-amber-600 text-black font-semibold px-8 py-3 rounded-lg transition-colors flex items-center justify-center"
				>
					<Calendar class="w-5 h-5 mr-2" />
					Current Season
				</a>
				<a 
					href="/historical" 
					class="bg-slate-700 hover:bg-slate-600 text-white font-semibold px-8 py-3 rounded-lg transition-colors flex items-center justify-center"
				>
					<BarChart3 class="w-5 h-5 mr-2" />
					Historical Deep Dive
				</a>
			</div>
		</div>
	</section>

	<!-- Quick Stats -->
	<section class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
		<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700/50">
			<div class="flex items-center justify-between">
				<div>
					<p class="text-slate-400 text-sm">Seasons Played</p>
					<p class="text-3xl font-bold text-white">
						{loading ? '...' : leagueOverview.totalSeasons || '0'}
					</p>
				</div>
				<Trophy class="h-8 w-8 text-amber-400" />
			</div>
		</div>
		
		<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700/50">
			<div class="flex items-center justify-between">
				<div>
					<p class="text-slate-400 text-sm">Total Transactions</p>
					<p class="text-3xl font-bold text-white">
						{loading ? '...' : (leagueOverview.totalTransactions || 0).toLocaleString()}
					</p>
				</div>
				<TrendingUp class="h-8 w-8 text-green-400" />
			</div>
		</div>
		
		<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700/50">
			<div class="flex items-center justify-between">
				<div>
					<p class="text-slate-400 text-sm">League Managers</p>
					<p class="text-3xl font-bold text-white">
						{loading ? '...' : leagueOverview.totalManagers || '0'}
					</p>
				</div>
				<Users class="h-8 w-8 text-blue-400" />
			</div>
		</div>
		
		<div class="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700/50">
			<div class="flex items-center justify-between">
				<div>
					<p class="text-slate-400 text-sm">Data Points</p>
					<p class="text-3xl font-bold text-white">
						{loading ? '...' : (leagueOverview.dataPoints || 0).toLocaleString()}
					</p>
				</div>
				<Zap class="h-8 w-8 text-purple-400" />
			</div>
		</div>
	</section>

	<!-- Features Grid -->
	<section class="grid grid-cols-1 lg:grid-cols-2 gap-8">
		<!-- This Season Preview -->
		<div class="bg-gradient-to-br from-slate-800/50 to-blue-900/20 rounded-xl p-8 border border-slate-700/50">
			<div class="flex items-center mb-6">
				<Calendar class="h-6 w-6 text-amber-400 mr-3" />
				<h2 class="text-2xl font-bold text-white">This Season</h2>
			</div>
			<div class="space-y-4">
				<div class="flex justify-between items-center py-2 border-b border-slate-700/30">
					<span class="text-slate-300">Current Week</span>
					<span class="text-white font-semibold">
						{loading ? '...' : `Week ${leagueOverview.currentWeek || 1}`}
					</span>
				</div>
				<div class="flex justify-between items-center py-2 border-b border-slate-700/30">
					<span class="text-slate-300">Playoff Picture</span>
					<span class="text-green-400 font-semibold">
						{loading ? '...' : leagueOverview.playoffStatus || 'Regular Season'}
					</span>
				</div>
				<div class="flex justify-between items-center py-2">
					<span class="text-slate-300">Trade Deadline</span>
					<span class="text-red-400 font-semibold">
						{loading ? '...' : leagueOverview.tradeDeadlineStatus || 'Active'}
					</span>
				</div>
			</div>
			<a 
				href="/this-season" 
				class="mt-6 inline-flex items-center text-amber-400 hover:text-amber-300 font-medium"
			>
				View Full Season →
			</a>
		</div>

		<!-- Historical Preview -->
		<div class="bg-gradient-to-br from-slate-800/50 to-purple-900/20 rounded-xl p-8 border border-slate-700/50">
			<div class="flex items-center mb-6">
				<BarChart3 class="h-6 w-6 text-purple-400 mr-3" />
				<h2 class="text-2xl font-bold text-white">League Legacy</h2>
			</div>
			<div class="space-y-4">
				<div class="flex justify-between items-center py-2 border-b border-slate-700/30">
					<span class="text-slate-300">Championships</span>
					<span class="text-white font-semibold">
						{loading ? '...' : `${leagueOverview.championshipGames || 0} Decided`}
					</span>
				</div>
				<div class="flex justify-between items-center py-2 border-b border-slate-700/30">
					<span class="text-slate-300">All-Time Leader</span>
					<span class="text-amber-400 font-semibold">
						{loading ? '...' : leagueOverview.allTimeLeader || 'TBD'}
					</span>
				</div>
				<div class="flex justify-between items-center py-2">
					<span class="text-slate-300">Biggest Rivalry</span>
					<span class="text-red-400 font-semibold">
						{loading ? '...' : leagueOverview.biggestRivalry || 'Analyzing...'}
					</span>
				</div>
			</div>
			<a 
				href="/historical" 
				class="mt-6 inline-flex items-center text-purple-400 hover:text-purple-300 font-medium"
			>
				Explore History →
			</a>
		</div>
	</section>

	<!-- Recent Activity Feed -->
	<section class="bg-slate-800/30 rounded-xl p-8 border border-slate-700/50">
		<h2 class="text-2xl font-bold text-white mb-6">Latest League Activity</h2>
		<div class="space-y-4">
			<div class="flex items-center justify-between py-3 border-b border-slate-700/30">
				<div class="flex items-center">
					<div class="w-2 h-2 bg-green-400 rounded-full mr-3"></div>
					<span class="text-slate-300">Weekly extraction completed</span>
				</div>
				<span class="text-slate-500 text-sm">2 hours ago</span>
			</div>
			<div class="flex items-center justify-between py-3 border-b border-slate-700/30">
				<div class="flex items-center">
					<div class="w-2 h-2 bg-blue-400 rounded-full mr-3"></div>
					<span class="text-slate-300">New trade data analyzed</span>
				</div>
				<span class="text-slate-500 text-sm">6 hours ago</span>
			</div>
			<div class="flex items-center justify-between py-3">
				<div class="flex items-center">
					<div class="w-2 h-2 bg-amber-400 rounded-full mr-3"></div>
					<span class="text-slate-300">Historical records updated</span>
				</div>
				<span class="text-slate-500 text-sm">1 day ago</span>
			</div>
		</div>
	</section>
</div>

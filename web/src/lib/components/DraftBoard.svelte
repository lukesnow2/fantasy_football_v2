<script lang="ts">
	import { onMount } from 'svelte';

	export let draftData: any[] = [];
	export let season: string = '';
	export let numTeams: number = 10;

	let selectedPick: any = null;
	let draftBoard: any[][] = [];

	// Calculate draft board layout
	function createDraftBoard(picks: any[]) {
		if (!picks.length) return [];
		
		const rounds = Math.max(...picks.map(p => p.round_number));
		const board: any[][] = [];
		
		// Sort all picks by overall_pick to ensure correct order
		const sortedPicks = [...picks].sort((a, b) => a.overall_pick - b.overall_pick);
		
		for (let round = 1; round <= rounds; round++) {
			const roundPicks = sortedPicks.filter(p => p.round_number === round);
			
			// Snake draft - odd rounds go left to right, even rounds go right to left
			if (round % 2 === 0) {
				roundPicks.reverse();
			}
			
			board.push(roundPicks);
		}
		
		return board;
	}

	function getPositionStyle(position: string): string {
		// Normalize position (trim whitespace, handle null/undefined)
		const normalizedPosition = position?.toString().trim().toUpperCase() || '';
		
		switch (normalizedPosition) {
			case 'QB': 
				return 'background-color: rgba(220, 38, 38, 0.2); border-color: rgb(239, 68, 68); color: rgb(252, 165, 165);';
			case 'RB': 
				return 'background-color: rgba(37, 99, 235, 0.2); border-color: rgb(59, 130, 246); color: rgb(147, 197, 253);';
			case 'WR': 
				return 'background-color: rgba(22, 163, 74, 0.2); border-color: rgb(34, 197, 94); color: rgb(134, 239, 172);';
			case 'TE': 
				return 'background-color: rgba(234, 179, 8, 0.3); border-color: rgb(250, 204, 21); color: rgb(254, 240, 138);';
			case 'K': 
				return 'background-color: rgba(120, 53, 15, 0.3); border-color: rgb(146, 64, 14); color: rgb(254, 215, 170);';
			case 'DEF': 
			case 'DEFENSE':
			case 'D/ST': 
				return 'background-color: rgba(147, 51, 234, 0.3); border-color: rgb(168, 85, 247); color: rgb(221, 214, 254);';
			default: 
				return 'background-color: rgba(107, 114, 128, 0.2); border-color: rgb(156, 163, 175); color: rgb(209, 213, 219);';
		}
	}

	function getPerformanceGrade(points: number): { grade: string; color: string } {
		if (points >= 200) return { grade: 'A+', color: 'text-green-400' };
		if (points >= 150) return { grade: 'A', color: 'text-green-300' };
		if (points >= 120) return { grade: 'B+', color: 'text-blue-400' };
		if (points >= 100) return { grade: 'B', color: 'text-blue-300' };
		if (points >= 80) return { grade: 'C+', color: 'text-yellow-400' };
		if (points >= 60) return { grade: 'C', color: 'text-yellow-300' };
		if (points >= 40) return { grade: 'D', color: 'text-orange-400' };
		return { grade: 'F', color: 'text-red-400' };
	}

	function getValueIndicator(pick: any): string {
		const expectedPoints = Math.max(180 - (pick.overall_pick * 2), 40);
		const actualPoints = parseFloat(pick.season_points || 0);
		const value = actualPoints - expectedPoints;
		
		if (value > 50) return '🔥'; // Steal
		if (value > 20) return '⭐'; // Good value
		if (value > -20) return '✓'; // Fair
		if (value > -50) return '⚠️'; // Disappointing
		return '💀'; // Bust
	}

	$: {
		console.log('Draft data received:', draftData?.length ? draftData.slice(0, 3) : 'No data');
		draftBoard = createDraftBoard(draftData);
	}
</script>

<div class="bg-slate-800 rounded-xl p-6 border border-slate-700">
	<div class="flex items-center justify-between mb-6">
		<h3 class="text-2xl font-bold text-white">
			{season} Draft Board Recreation
		</h3>
		<div class="text-sm text-slate-400">
			{numTeams} Teams • {draftData.length} Total Picks
		</div>
	</div>

	<!-- Position Legend -->
	<div class="flex items-center gap-2 mb-6 text-xs">
		<span class="text-slate-400">Positions:</span>
		<span class="px-2 py-1 rounded border" style={getPositionStyle('QB')}>QB</span>
		<span class="px-2 py-1 rounded border" style={getPositionStyle('RB')}>RB</span>
		<span class="px-2 py-1 rounded border" style={getPositionStyle('WR')}>WR</span>
		<span class="px-2 py-1 rounded border" style={getPositionStyle('TE')}>TE</span>
		<span class="px-2 py-1 rounded border" style={getPositionStyle('DEF')}>DEF</span>
		<span class="px-2 py-1 rounded border" style={getPositionStyle('K')}>K</span>
	</div>

	<!-- Draft Board -->
	<div class="overflow-x-auto">
		<div class="min-w-full">
			{#each draftBoard as round, roundIndex}
				<div class="mb-4">
					<div class="flex items-center gap-2 mb-2">
						<span class="text-sm font-bold text-slate-300">Round {roundIndex + 1}</span>
						<div class="flex-1 h-px bg-slate-600"></div>
					</div>
					
					<div class="grid gap-2" style="grid-template-columns: repeat({numTeams}, minmax(140px, 1fr));">
						{#each round as pick}
							<button
								class="p-3 rounded-lg border-2 transition-all hover:scale-105 cursor-pointer"
								style={getPositionStyle(pick.primary_position)}
								on:click={() => selectedPick = selectedPick?.overall_pick === pick.overall_pick ? null : pick}
							>
								<div class="text-xs font-bold mb-1">#{pick.overall_pick}</div>
								<div class="font-bold text-white text-sm mb-1 truncate" title={pick.player_name}>
									{pick.player_name}
								</div>
								<div class="text-xs opacity-75 mb-1">{pick.primary_position} • {pick.nfl_team}</div>
								<div class="text-xs text-slate-300 truncate" title={pick.manager_name}>
									{pick.manager_name}
								</div>
								<div class="flex items-center justify-between mt-2">
									<span class="text-xs font-bold {getPerformanceGrade(parseFloat(pick.season_points || 0)).color}">
										{getPerformanceGrade(parseFloat(pick.season_points || 0)).grade}
									</span>
									<span class="text-sm">
										{getValueIndicator(pick)}
									</span>
								</div>
								{#if pick.is_keeper_pick}
									<div class="text-xs text-amber-400 mt-1">🔒 Keeper</div>
								{/if}
							</button>
						{/each}
					</div>
				</div>
			{/each}
		</div>
	</div>

	<!-- Pick Detail Modal -->
	{#if selectedPick}
		<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50" on:click={() => selectedPick = null}>
			<div class="bg-slate-800 rounded-xl p-6 border border-slate-700 max-w-md mx-4" on:click|stopPropagation>
				<div class="flex items-center justify-between mb-4">
					<h4 class="text-xl font-bold text-white">Pick #{selectedPick.overall_pick}</h4>
					<button class="text-slate-400 hover:text-white" on:click={() => selectedPick = null}>✕</button>
				</div>
				
				<div class="space-y-4">
					<div class="text-center">
						<div class="text-lg font-bold text-white mb-1">{selectedPick.player_name}</div>
						<div class="text-sm text-slate-400">{selectedPick.primary_position} • {selectedPick.nfl_team}</div>
						<div class="text-sm text-blue-400">{selectedPick.manager_name} ({selectedPick.team_name})</div>
					</div>
					
					<div class="grid grid-cols-2 gap-4 text-sm">
						<div class="bg-slate-700/50 p-3 rounded">
							<div class="text-slate-400">Round</div>
							<div class="font-bold text-white">{selectedPick.round_number}.{selectedPick.pick_in_round}</div>
						</div>
						<div class="bg-slate-700/50 p-3 rounded">
							<div class="text-slate-400">Season Points</div>
							<div class="font-bold {getPerformanceGrade(parseFloat(selectedPick.season_points || 0)).color}">
								{parseFloat(selectedPick.season_points || 0).toFixed(1)}
							</div>
						</div>
						<div class="bg-slate-700/50 p-3 rounded">
							<div class="text-slate-400">Grade</div>
							<div class="font-bold {getPerformanceGrade(parseFloat(selectedPick.season_points || 0)).color}">
								{getPerformanceGrade(parseFloat(selectedPick.season_points || 0)).grade}
							</div>
						</div>
						<div class="bg-slate-700/50 p-3 rounded">
							<div class="text-slate-400">Value</div>
							<div class="text-lg">{getValueIndicator(selectedPick)}</div>
						</div>
					</div>
					
					{#if selectedPick.is_keeper_pick}
						<div class="bg-amber-500/10 border border-amber-500/20 rounded p-3 text-center">
							<span class="text-amber-400">🔒 Keeper Pick</span>
						</div>
					{/if}
					
					<div class="text-xs text-slate-500 text-center">
						Click outside to close
					</div>
				</div>
			</div>
		</div>
	{/if}
</div> 
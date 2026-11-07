<script lang="ts">
	export let analytics: any = null;
	export let season: string = '';

	function getGradeColor(grade: string): string {
		if (grade.startsWith('A')) return 'text-green-400';
		if (grade.startsWith('B')) return 'text-blue-400';
		if (grade.startsWith('C')) return 'text-yellow-400';
		if (grade.startsWith('D')) return 'text-orange-400';
		return 'text-red-400';
	}

	// Grade each manager's draft on the season's own curve. Absolute thresholds make
	// everyone an "A" (a full-season player naturally scores 100+), so grade by how far
	// a manager's avg points/pick sits from the season mean (z-score across managers).
	$: gradeByManager = computeGrades(analytics?.managerPerformance ?? []);
	function computeGrades(mgrs: any[]): Record<string, string> {
		const out: Record<string, string> = {};
		const vals = mgrs
			.map((m: any) => parseFloat(m.avgPointsPerPick || 0))
			.filter((v: number) => v > 0);
		if (!vals.length) return out;
		const mean = vals.reduce((a: number, b: number) => a + b, 0) / vals.length;
		const sd = Math.sqrt(vals.reduce((a: number, b: number) => a + (b - mean) ** 2, 0) / vals.length);
		const gradeOf = (v: number) => {
			if (sd === 0) return 'B';
			const z = (v - mean) / sd;
			if (z >= 1.0) return 'A';
			if (z >= 0.3) return 'B';
			if (z >= -0.3) return 'C';
			if (z >= -1.0) return 'D';
			return 'F';
		};
		mgrs.forEach((m: any) => {
			out[m.managerName] = gradeOf(parseFloat(m.avgPointsPerPick || 0));
		});
		return out;
	}
</script>

{#if analytics}
	<div class="space-y-6">
		<!-- Best and Worst Picks -->
		{#if analytics.bestWorstPicks?.length > 0}
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h3 class="text-xl font-bold text-white mb-1">🏆 Best & Worst Picks</h3>
				{#if analytics.seasonInProgress}
					<p class="mb-2 text-sm font-medium text-amber-300">
						This season is still being played: these are judged on points so far, and an early injury or slow start can still turn around.
					</p>
				{/if}
				<p class="mb-4 text-sm text-slate-400">
					Judged against what players at the same position, taken around the same round, have scored in this league. Busts are picks expected to beat an average pick, so late kickers and defenses don't count unless they were reached for early.
				</p>
				
				<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
					<!-- Best Picks -->
					<div>
						<h4 class="text-lg font-semibold text-green-400 mb-3">💎 Best Value</h4>
						<div class="space-y-3">
							{#each analytics.bestWorstPicks.filter((p: any) => p.pickType === 'best').slice(0, 3) as pick}
								<div class="bg-green-500/10 border border-green-500/20 rounded-lg p-4">
									<div class="flex items-center justify-between mb-2">
										<span class="font-bold text-white">{pick.playerName}</span>
										<span class="text-xs bg-green-500 text-black px-2 py-1 rounded">R{pick.roundNumber} · #{pick.overallPick}</span>
									</div>
									<div class="text-sm text-slate-300">{pick.primaryPosition} • {pick.managerName}</div>
									<div class="flex items-baseline justify-between gap-3">
										<span class="text-lg font-bold text-green-400">{parseFloat(pick.seasonPoints || 0).toFixed(1)} pts</span>
										{#if pick.expectedPoints != null}
											<span class="text-sm text-slate-400 tabular-nums">
												expected {pick.expectedPoints.toFixed(0)} · <span class="font-semibold text-green-400">{pick.valueOverExpected > 0 ? '+' : ''}{pick.valueOverExpected.toFixed(0)}</span>
											</span>
										{/if}
									</div>
								</div>
							{/each}
						</div>
					</div>

					<!-- Worst Picks -->
					<div>
						<h4 class="text-lg font-semibold text-red-400 mb-3">💀 Biggest Busts</h4>
						<div class="space-y-3">
							{#each analytics.bestWorstPicks.filter((p: any) => p.pickType === 'worst').slice(0, 3) as pick}
								<div class="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
									<div class="flex items-center justify-between mb-2">
										<span class="font-bold text-white">{pick.playerName}</span>
										<span class="text-xs bg-red-500 text-white px-2 py-1 rounded">R{pick.roundNumber} · #{pick.overallPick}</span>
									</div>
									<div class="text-sm text-slate-300">{pick.primaryPosition} • {pick.managerName}</div>
									<div class="flex items-baseline justify-between gap-3">
										<span class="text-lg font-bold text-red-400">{parseFloat(pick.seasonPoints || 0).toFixed(1)} pts</span>
										{#if pick.expectedPoints != null}
											<span class="text-sm text-slate-400 tabular-nums">
												expected {pick.expectedPoints.toFixed(0)} · <span class="font-semibold text-red-400">{pick.valueOverExpected > 0 ? '+' : ''}{pick.valueOverExpected.toFixed(0)}</span>
											</span>
										{/if}
									</div>
								</div>
							{/each}
						</div>
					</div>
				</div>
			</section>
		{/if}

		<!-- Manager Performance -->
		{#if analytics.managerPerformance?.length > 0}
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h3 class="text-xl font-bold text-white mb-4">👤 Manager Draft Grades</h3>
				
				<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
					{#each analytics.managerPerformance.slice(0, 9) as manager}
						{@const avgPoints = parseFloat(manager.avgPointsPerPick || 0)}
						{@const grade = gradeByManager[manager.managerName] ?? 'C'}
						<div class="bg-slate-700/30 rounded-lg p-4">
							<div class="flex items-center justify-between mb-3">
								<span class="font-bold text-white">{manager.managerName}</span>
								<span class="text-2xl font-bold {getGradeColor(grade)}">{grade}</span>
							</div>
							<div class="text-sm text-green-400">
								{avgPoints.toFixed(1)} avg points/pick
							</div>
						</div>
					{/each}
				</div>
			</section>
		{/if}
	</div>
{:else}
	<div class="text-center py-8">
		<div class="text-slate-400">No draft analytics available</div>
	</div>
{/if} 
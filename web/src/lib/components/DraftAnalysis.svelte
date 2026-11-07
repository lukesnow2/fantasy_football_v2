<script lang="ts">
	export let analytics: any = null;
	export let season: string = '';

	function getGradeColor(grade: string): string {
		if (grade.startsWith('A')) return 'text-green-400';
		if (grade.startsWith('B')) return 'text-blue-400';
		if (grade.startsWith('C')) return 'text-yellow-400';
		if (grade.startsWith('D')) return 'text-orange-400';
		if (grade.startsWith('F')) return 'text-red-400';
		return 'text-slate-400';
	}

	// Each manager's draft for this season, graded by the API ($lib/draft-value):
	// points over expectation in average picks, placed among every finished
	// manager-season in league history. It used to be a z-score of raw points
	// per pick within the season, which rewarded taking QBs early and forced a
	// curve onto every year.
	$: seasonGrades = (analytics?.draftGrades?.bySeason ?? [])
		.filter((g: any) => String(g.seasonYear) === String(season))
		.sort((a: any, b: any) => b.score - a.score);
	// One decimal, signed; a score that rounds to zero reads as level, not "-0.0".
	function formatScore(score: number): { text: string; color: string } {
		const r = Math.round(score * 10) / 10;
		if (r === 0) return { text: '±0.0', color: 'text-slate-300' };
		return r > 0
			? { text: `+${r.toFixed(1)}`, color: 'text-green-400' }
			: { text: r.toFixed(1), color: 'text-red-400' };
	}

	$: pointsPerPick = new Map<string, number>(
		(analytics?.managerPerformance ?? []).map((m: any): [string, number] => [
			m.managerName,
			parseFloat(m.avgPointsPerPick || 0)
		])
	);
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
					Judged against what players at the same position, taken around the same round, have scored in this league. Busts are picks from rounds 1–8 expected to beat an average pick, so late kickers, defenses and fliers don't count; a kicker or defense reached for early does.
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

		<!-- Manager draft grades -->
		{#if seasonGrades.length > 0}
			<section class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
				<h3 class="text-xl font-bold text-white mb-1">👤 Manager Draft Grades</h3>
				<p class="mb-4 text-sm text-slate-400">
					Each draft's points over what its picks' positions and rounds were expected to score, in average
					picks, graded against every manager's draft in league history{seasonGrades[0]?.inProgress
						? ', on points so far this season'
						: ''}.
				</p>

				<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
					{#each seasonGrades as g (g.managerName)}
						<div class="bg-slate-700/30 rounded-lg p-4">
							<div class="flex items-center justify-between mb-3">
								<span class="font-bold text-white">{g.managerName}</span>
								<span class="text-2xl font-bold {getGradeColor(g.grade)}">{g.grade}</span>
							</div>
							<div class="text-sm font-semibold {formatScore(g.score).color}">
								{formatScore(g.score).text} average picks over expectation
							</div>
							<div class="text-xs text-slate-400">
								Better than {Math.round(g.percentile * 100)}% of drafts{#if pointsPerPick.get(g.managerName)}{' · '}{pointsPerPick
										.get(g.managerName)
										?.toFixed(1)} points/pick{/if}
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
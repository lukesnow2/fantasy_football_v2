<script lang="ts">
	// Expected (Pythagorean) wins and luck for one manager: career, and the
	// season being played. The numbers are the power rankings' own, carried
	// over by /api/managers/performance, so the two pages always agree.
	import { Dices } from 'lucide-svelte';
	import MetricHelp from '$lib/components/MetricHelp.svelte';
	import { formatLuck, getLuckColor } from '$lib/utils/managerUtils';

	export let manager: any;
	// Stack career over this season, for narrow columns.
	export let stacked = false;

	$: current = manager?.currentSeason ?? null;

	function fixed(value: number | null | undefined, digits = 1): string {
		return value == null ? '—' : value.toFixed(digits);
	}
</script>

<div class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-6">
	<h3 class="mb-1 flex items-center text-lg font-bold text-white">
		<Dices class="mr-2 h-5 w-5 text-teal-400" />
		Expected Wins &amp; Luck
	</h3>
	<p class="mb-4 text-xs text-slate-400">
		Regular season. Expected wins come from points scored and allowed; luck is the gap.
	</p>

	<div class="grid grid-cols-1 gap-4 {stacked ? '' : 'sm:grid-cols-2'}">
		<div class="space-y-2 rounded-lg bg-slate-900/30 p-4">
			<div class="text-xs tracking-wide text-slate-400 uppercase">
				Career{manager?.luckSeasons ? ` · ${manager.luckSeasons} seasons` : ''}
			</div>
			<div class="flex justify-between">
				<span class="text-slate-400">Wins</span>
				<span class="font-bold text-white tabular-nums"
					>{fixed(manager?.careerRegularSeasonWins, 1)}</span
				>
			</div>
			<div class="flex justify-between">
				<span class="text-slate-400">
					<MetricHelp
						metricId="pythagorean_wins"
						position="left"
						theme="dark"
						className="text-slate-400"
						showIcon={false}
					>
						Expected wins
					</MetricHelp>
				</span>
				<span class="font-bold text-white tabular-nums"
					>{fixed(manager?.careerPythagoreanWins, 1)}</span
				>
			</div>
			<div class="flex justify-between">
				<span class="text-slate-400">
					<MetricHelp
						metricId="luck_factor"
						position="left"
						theme="dark"
						className="text-slate-400"
						showIcon={false}
					>
						Luck
					</MetricHelp>
				</span>
				<span class="font-bold tabular-nums {getLuckColor(manager?.careerLuckFactor)}">
					{formatLuck(manager?.careerLuckFactor)} W
				</span>
			</div>
		</div>

		<div class="space-y-2 rounded-lg bg-slate-900/30 p-4">
			{#if current}
				<div class="text-xs tracking-wide text-slate-400 uppercase">
					{current.seasonYear} · {current.final ? 'final' : `through week ${current.throughWeek}`}
				</div>
				<div class="flex justify-between">
					<span class="text-slate-400">Wins</span>
					<span class="font-bold text-white tabular-nums"
						>{fixed(current.regularSeasonWins, 1)}</span
					>
				</div>
				<div class="flex justify-between">
					<span class="text-slate-400">Expected wins</span>
					<span class="font-bold text-white tabular-nums">{fixed(current.pythagoreanWins, 1)}</span>
				</div>
				<div class="flex justify-between">
					<span class="text-slate-400">Luck</span>
					<span class="font-bold tabular-nums {getLuckColor(current.luckFactor)}"
						>{formatLuck(current.luckFactor)} W</span
					>
				</div>
				{#if !current.final}
					<div class="flex justify-between">
						<span class="text-slate-400">
							<MetricHelp
								metricId="playoff_probability"
								position="left"
								theme="dark"
								className="text-slate-400"
								showIcon={false}
							>
								Playoff odds
							</MetricHelp>
						</span>
						<span class="font-bold text-blue-400 tabular-nums">
							{current.playoffOdds == null ? '—' : `${Math.round(current.playoffOdds * 100)}%`}
						</span>
					</div>
				{/if}
			{:else}
				<!-- Between seasons this is the season just ended; a manager who
				     left the league gets this line, not their own last season. -->
				<div class="text-xs tracking-wide text-slate-400 uppercase">
					{manager?.latestSeason ?? 'This season'}
				</div>
				<div class="text-sm text-slate-400">
					{manager?.latestSeason ? `Didn't play in ${manager.latestSeason}.` : 'No season yet.'}
				</div>
			{/if}
		</div>
	</div>
</div>

<script lang="ts">
	// Expected (Pythagorean) wins and luck for one manager: career, and the
	// season being played. The numbers are the power rankings' own, carried
	// over by /api/managers/performance, so the two pages always agree.
	import { Dices } from 'lucide-svelte';
	import MetricHelp from '$lib/components/MetricHelp.svelte';

	export let manager: any;
	// Stack career over this season, for narrow columns.
	export let stacked = false;

	$: current = manager?.currentSeason ?? null;

	function signed(value: number | null | undefined, digits = 1): string {
		if (value == null) return '—';
		const fixed = value.toFixed(digits);
		return value > 0 ? `+${fixed}` : fixed;
	}

	function fixed(value: number | null | undefined, digits = 1): string {
		return value == null ? '—' : value.toFixed(digits);
	}

	// Within half a win either way is a record that matches the scoring.
	function luckColor(value: number | null | undefined): string {
		if (value == null || Math.abs(value) < 0.5) return 'text-slate-200';
		return value > 0 ? 'text-green-400' : 'text-red-400';
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
				<span class="font-bold tabular-nums {luckColor(manager?.careerLuckFactor)}">
					{signed(manager?.careerLuckFactor)} W
				</span>
			</div>
		</div>

		<div class="space-y-2 rounded-lg bg-slate-900/30 p-4">
			{#if current}
				<div class="text-xs tracking-wide text-slate-400 uppercase">
					{current.seasonYear} · through week {current.throughWeek}
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
					<span class="font-bold tabular-nums {luckColor(current.luckFactor)}"
						>{signed(current.luckFactor)} W</span
					>
				</div>
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
			{:else}
				<div class="text-xs tracking-wide text-slate-400 uppercase">This season</div>
				<div class="text-sm text-slate-400">Not playing a season in progress.</div>
			{/if}
		</div>
	</div>
</div>

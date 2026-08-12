<script lang="ts">
	import { onMount } from 'svelte';
	import { TrendingUp, TrendingDown, Minus, Trophy, Info } from 'lucide-svelte';
	import type { PowerRankingRow } from '../api/power-rankings/+server';

	let loading = $state(true);
	let error = $state('');

	let seasons = $state<number[]>([]);
	let weeks = $state<number[]>([]);
	let season = $state<number | null>(null);
	let week = $state<number | null>(null);
	let rankings = $state<PowerRankingRow[]>([]);
	let trend = $state<PowerRankingRow[]>([]);

	async function load(nextSeason?: number, nextWeek?: number) {
		loading = true;
		error = '';
		try {
			const params = new URLSearchParams();
			if (nextSeason != null) params.set('season', String(nextSeason));
			if (nextWeek != null) params.set('week', String(nextWeek));
			const response = await fetch(`/api/power-rankings?${params}`);
			if (!response.ok) {
				error = 'Could not load power rankings.';
				return;
			}
			const data = await response.json();
			seasons = data.seasons ?? [];
			weeks = data.weeks ?? [];
			season = data.season;
			week = data.week;
			rankings = data.rankings ?? [];
			trend = data.trend ?? [];
		} catch (err) {
			console.error('Error loading power rankings:', err);
			error = 'Could not load power rankings.';
		} finally {
			loading = false;
		}
	}

	onMount(() => load());

	function onSeasonChange(event: Event) {
		// Week numbers are not comparable across seasons, so let the API pick the
		// latest week of the newly selected season rather than carrying this one over.
		const next = Number((event.target as HTMLSelectElement).value);
		load(next);
	}

	function onWeekChange(event: Event) {
		const next = Number((event.target as HTMLSelectElement).value);
		if (season != null) load(season, next);
	}

	const isFinalWeek = $derived(week != null && week === weeks[weeks.length - 1]);

	function formatPct(value: number | null): string {
		return value == null ? '—' : `${(value * 100).toFixed(0)}%`;
	}

	function formatNum(value: number | null, digits = 2): string {
		return value == null ? '—' : value.toFixed(digits);
	}

	function record(row: PowerRankingRow): string {
		return row.ties > 0 ? `${row.wins}-${row.losses}-${row.ties}` : `${row.wins}-${row.losses}`;
	}

	// ---- rank-over-time chart -------------------------------------------------
	// Ten distinct hues, ordered so adjacent ranks don't get near-identical colors.
	const TEAM_COLORS = [
		'#f59e0b', '#38bdf8', '#a78bfa', '#34d399', '#fb7185',
		'#facc15', '#60a5fa', '#f472b6', '#4ade80', '#c084fc'
	];

	const CHART = { width: 900, height: 360, padTop: 16, padRight: 16, padBottom: 32, padLeft: 40 };

	interface TeamSeries {
		teamKey: number;
		teamName: string;
		managerName: string | null;
		color: string;
		points: { week: number; rank: number }[];
		path: string;
		finalRank: number;
	}

	const series = $derived.by<TeamSeries[]>(() => {
		if (trend.length === 0 || weeks.length === 0) return [];

		const maxRank = Math.max(...trend.map((r) => r.powerRank));
		const firstWeek = weeks[0];
		const lastWeek = weeks[weeks.length - 1];
		const spanX = Math.max(1, lastWeek - firstWeek);
		const spanY = Math.max(1, maxRank - 1);

		const x = (w: number) =>
			CHART.padLeft +
			((w - firstWeek) / spanX) * (CHART.width - CHART.padLeft - CHART.padRight);
		// Rank 1 sits at the top, so the y scale is inverted.
		const y = (rank: number) =>
			CHART.padTop + ((rank - 1) / spanY) * (CHART.height - CHART.padTop - CHART.padBottom);

		const byTeam = new Map<number, PowerRankingRow[]>();
		for (const row of trend) {
			const rows = byTeam.get(row.teamKey);
			if (rows) rows.push(row);
			else byTeam.set(row.teamKey, [row]);
		}

		const built = [...byTeam.values()].map((rows) => {
			const sorted = [...rows].sort((a, b) => a.weekNumber - b.weekNumber);
			const points = sorted.map((r) => ({ week: r.weekNumber, rank: r.powerRank }));
			const last = sorted[sorted.length - 1];
			return {
				teamKey: last.teamKey,
				teamName: last.teamName,
				managerName: last.managerName,
				color: '',
				points,
				path: points.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(p.week)},${y(p.rank)}`).join(' '),
				finalRank: last.powerRank
			};
		});

		built.sort((a, b) => a.finalRank - b.finalRank);
		return built.map((s, i) => ({ ...s, color: TEAM_COLORS[i % TEAM_COLORS.length] }));
	});

	const chartTicks = $derived.by(() => {
		if (trend.length === 0) return { xs: [] as { week: number; x: number }[], ys: [] as { rank: number; y: number }[] };
		const maxRank = Math.max(...trend.map((r) => r.powerRank));
		const firstWeek = weeks[0];
		const lastWeek = weeks[weeks.length - 1];
		const spanX = Math.max(1, lastWeek - firstWeek);
		const spanY = Math.max(1, maxRank - 1);
		return {
			xs: weeks.map((w) => ({
				week: w,
				x: CHART.padLeft + ((w - firstWeek) / spanX) * (CHART.width - CHART.padLeft - CHART.padRight)
			})),
			ys: Array.from({ length: maxRank }, (_, i) => i + 1).map((rank) => ({
				rank,
				y: CHART.padTop + ((rank - 1) / spanY) * (CHART.height - CHART.padTop - CHART.padBottom)
			}))
		};
	});

	let hoveredTeam = $state<number | null>(null);
</script>

<svelte:head>
	<title>Power Rankings · The League</title>
</svelte:head>

<div class="space-y-8">
	<div class="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
		<div>
			<h1 class="text-4xl font-bold text-white mb-2">Power Rankings</h1>
			<p class="text-slate-400">
				Ranked by power score, which blends record, scoring and recent form —
				not by standings alone.
			</p>
		</div>

		{#if seasons.length > 0}
			<div class="flex items-end gap-3">
				<label class="flex flex-col text-xs font-medium text-slate-400">
					Season
					<select
						class="mt-1 rounded-lg border border-slate-600 bg-slate-800 px-3 py-2 text-sm text-white focus:border-amber-500 focus:outline-none"
						value={season}
						onchange={onSeasonChange}
					>
						{#each seasons as s}
							<option value={s}>{s}</option>
						{/each}
					</select>
				</label>

				<label class="flex flex-col text-xs font-medium text-slate-400">
					Week <span class="text-slate-500">(regular season)</span>
					<select
						class="mt-1 rounded-lg border border-slate-600 bg-slate-800 px-3 py-2 text-sm text-white focus:border-amber-500 focus:outline-none"
						value={week}
						onchange={onWeekChange}
					>
						{#each weeks as w}
							<option value={w}>Week {w}</option>
						{/each}
					</select>
				</label>
			</div>
		{/if}
	</div>

	{#if loading}
		<div class="space-y-3">
			{#each Array(10) as _, i}
				<div class="h-14 animate-pulse rounded-lg bg-slate-800/50" style="animation-delay: {i * 40}ms"></div>
			{/each}
		</div>
	{:else if error}
		<div class="rounded-xl border border-red-500/30 bg-red-500/10 p-6 text-center">
			<p class="text-red-300">{error}</p>
			<button
				class="mt-3 rounded-lg bg-slate-700 px-4 py-2 text-sm font-medium text-white hover:bg-slate-600"
				onclick={() => load(season ?? undefined, week ?? undefined)}
			>
				Try again
			</button>
		</div>
	{:else if rankings.length === 0}
		<div class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-12 text-center">
			<Trophy class="mx-auto h-10 w-10 text-slate-600" />
			<p class="mt-3 font-medium text-slate-300">No power rankings for this season yet.</p>
			<p class="mt-1 text-sm text-slate-500">
				Rankings are built by the data pipeline once a season has been played.
			</p>
		</div>
	{:else}
		{#if isFinalWeek}
			<div class="flex items-start gap-2 rounded-lg border border-slate-700/50 bg-slate-800/40 px-4 py-3 text-sm text-slate-400">
				<Info class="mt-0.5 h-4 w-4 shrink-0 text-slate-500" />
				<span>
					Week {week} is the last ranked week of {season}. Power rankings cover the
					regular season only — playoff weeks aren't ranked.
				</span>
			</div>
		{/if}

		<section class="overflow-hidden rounded-xl border border-slate-700/50 bg-slate-800/50">
			<div class="overflow-x-auto">
				<table class="w-full min-w-[52rem] text-sm">
					<thead>
						<tr class="border-b border-slate-700 text-left text-xs uppercase tracking-wide text-slate-400">
							<th class="px-4 py-3 font-medium">#</th>
							<th class="px-2 py-3 font-medium" title="Change in power rank since last week">Move</th>
							<th class="px-4 py-3 font-medium">Team</th>
							<th class="px-4 py-3 font-medium">Record</th>
							<th class="px-4 py-3 text-right font-medium" title="Blend of record, scoring and recent form. Higher is better.">Power</th>
							<th class="px-4 py-3 text-right font-medium" title="Rank by points scored">Pts rank</th>
							<th class="px-4 py-3 text-right font-medium" title="Expected wins given points for and against">Pyth. W</th>
							<th class="px-4 py-3 text-right font-medium" title="Actual wins minus expected wins. Positive means the schedule has been kind.">Luck</th>
							<th class="px-4 py-3 text-right font-medium" title="Modelled chance of making the playoffs as of this week">Playoffs</th>
						</tr>
					</thead>
					<tbody>
						{#each rankings as row (row.teamKey)}
							<tr class="border-b border-slate-700/40 last:border-0 hover:bg-slate-700/20">
								<td class="px-4 py-3 text-lg font-bold text-slate-300 tabular-nums">{row.powerRank}</td>
								<td class="px-2 py-3">
									{#if row.rankChange == null}
										<span class="text-slate-600" title="No previous week">—</span>
									{:else if row.rankChange > 0}
										<span class="inline-flex items-center text-green-400 tabular-nums">
											<TrendingUp class="mr-1 h-3.5 w-3.5" />{row.rankChange}
										</span>
									{:else if row.rankChange < 0}
										<span class="inline-flex items-center text-red-400 tabular-nums">
											<TrendingDown class="mr-1 h-3.5 w-3.5" />{Math.abs(row.rankChange)}
										</span>
									{:else}
										<span class="inline-flex items-center text-slate-500">
											<Minus class="h-3.5 w-3.5" />
										</span>
									{/if}
								</td>
								<td class="px-4 py-3">
									<div class="font-medium text-white">{row.teamName}</div>
									{#if row.managerName}
										<a
											href="/managers/{encodeURIComponent(row.managerName)}"
											class="text-xs text-slate-400 hover:text-amber-400"
										>
											{row.managerName}
										</a>
									{/if}
								</td>
								<td class="px-4 py-3 text-slate-300 tabular-nums">{record(row)}</td>
								<td class="px-4 py-3 text-right font-semibold text-amber-400 tabular-nums">{formatNum(row.powerScore, 3)}</td>
								<td class="px-4 py-3 text-right text-slate-300 tabular-nums">{row.pointsRank ?? '—'}</td>
								<td class="px-4 py-3 text-right text-slate-300 tabular-nums">{formatNum(row.pythagoreanWins, 1)}</td>
								<td
									class="px-4 py-3 text-right tabular-nums {row.luckFactor == null
										? 'text-slate-500'
										: row.luckFactor > 0
											? 'text-green-400'
											: row.luckFactor < 0
												? 'text-red-400'
												: 'text-slate-400'}"
								>
									{row.luckFactor != null && row.luckFactor > 0 ? '+' : ''}{formatNum(row.luckFactor, 2)}
								</td>
								<td class="px-4 py-3 text-right text-slate-300 tabular-nums">{formatPct(row.playoffOdds)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		</section>

		{#if series.length > 0 && weeks.length > 1}
			<section class="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
				<h2 class="mb-1 text-xl font-bold text-white">Rank through the season</h2>
				<p class="mb-5 text-sm text-slate-400">Every ranked week of {season}. Rank 1 is at the top.</p>

				<div class="overflow-x-auto">
					<svg
						viewBox="0 0 {CHART.width} {CHART.height}"
						class="h-auto w-full min-w-[40rem]"
						role="img"
						aria-label="Line chart of each team's power rank by week"
					>
						{#each chartTicks.ys as tick}
							<line
								x1={CHART.padLeft} y1={tick.y} x2={CHART.width - CHART.padRight} y2={tick.y}
								stroke="#334155" stroke-width="1"
							/>
							<text x={CHART.padLeft - 10} y={tick.y + 4} text-anchor="end" fill="#64748b" font-size="11">
								{tick.rank}
							</text>
						{/each}

						{#each chartTicks.xs as tick}
							<text x={tick.x} y={CHART.height - 10} text-anchor="middle" fill="#64748b" font-size="11">
								{tick.week}
							</text>
						{/each}

						{#each series as s (s.teamKey)}
							<path
								d={s.path}
								fill="none"
								stroke={s.color}
								stroke-width={hoveredTeam === s.teamKey ? 3.5 : 2}
								stroke-linejoin="round"
								stroke-linecap="round"
								opacity={hoveredTeam === null || hoveredTeam === s.teamKey ? 1 : 0.15}
							/>
						{/each}
					</svg>
				</div>

				<ul class="mt-4 flex flex-wrap gap-x-5 gap-y-2">
					{#each series as s (s.teamKey)}
						<li>
							<button
								type="button"
								class="flex items-center gap-2 text-xs text-slate-300 transition-opacity hover:text-white"
								style="opacity: {hoveredTeam === null || hoveredTeam === s.teamKey ? 1 : 0.4}"
								onmouseenter={() => (hoveredTeam = s.teamKey)}
								onmouseleave={() => (hoveredTeam = null)}
								onfocus={() => (hoveredTeam = s.teamKey)}
								onblur={() => (hoveredTeam = null)}
							>
								<span class="h-2.5 w-2.5 shrink-0 rounded-full" style="background:{s.color}"></span>
								<span class="max-w-[14rem] truncate">{s.teamName}</span>
								{#if s.managerName}
									<span class="text-slate-500">({s.managerName})</span>
								{/if}
							</button>
						</li>
					{/each}
				</ul>
			</section>
		{/if}
	{/if}
</div>

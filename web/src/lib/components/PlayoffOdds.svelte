<script lang="ts">
	import { Trophy, ChevronDown, Info } from 'lucide-svelte';

	/**
	 * Simulated playoff odds for the This Season page. Replaces a "Playoff
	 * Picture" card that called any top-six team over .700 "clinched".
	 * The model lives in $lib/playoff-odds; this only presents it.
	 */

	interface TeamOdds {
		teamKey: number;
		teamName: string;
		managerName: string | null;
		wins: number;
		losses: number;
		ties: number;
		pointsFor: number;
		allPlayWins: number;
		allPlayLosses: number;
		luck: number;
		projectedPoints: number;
		projectedPointsRank: number;
		remainingOpponentPoints: number | null;
		remainingGames: number;
		projectedWins: number;
		playoffOdds: number;
		byeOdds: number;
		topSeedOdds: number;
		titleOdds: number;
		clinched: boolean;
		eliminated: boolean;
		previousOdds: number | null;
	}

	interface OddsResponse {
		season: number;
		available: boolean;
		reason?: string;
		throughWeek: number;
		regularSeasonWeeks: number;
		phase: 'regular' | 'postseason';
		sims: number;
		playoffTeams: number;
		byes: number;
		leagueAverage: number;
		teams: TeamOdds[];
	}

	type Metric = 'playoffOdds' | 'byeOdds' | 'titleOdds';
	const TABS: { id: Metric; label: string }[] = [
		{ id: 'playoffOdds', label: 'Playoffs' },
		{ id: 'byeOdds', label: 'Bye' },
		{ id: 'titleOdds', label: 'Title' }
	];

	let data = $state<OddsResponse | null>(null);
	let loading = $state(true);
	let failed = $state(false);
	let metric = $state<Metric>('playoffOdds');
	let open = $state<number | null>(null);
	let showMethod = $state(false);

	$effect(() => {
		let cancelled = false;
		(async () => {
			try {
				const res = await fetch('/api/playoff-odds');
				if (!res.ok) throw new Error(String(res.status));
				const body = await res.json();
				if (!cancelled) data = body;
			} catch (err) {
				console.error('Playoff odds failed to load:', err);
				if (!cancelled) failed = true;
			} finally {
				if (!cancelled) loading = false;
			}
		})();
		return () => {
			cancelled = true;
		};
	});

	const sorted = $derived.by(() => {
		if (!data?.teams) return [];
		return [...data.teams].sort(
			(a, b) => b[metric] - a[metric] || b.projectedWins - a.projectedWins
		);
	});

	// Final seeding, once the regular season is over: wins, then points for.
	const seeds = $derived.by(() => {
		if (!data?.teams) return [];
		return [...data.teams].sort(
			(a, b) => b.wins + b.ties / 2 - (a.wins + a.ties / 2) || b.pointsFor - a.pointsFor
		);
	});

	// Remaining-schedule difficulty ranked within the league (1 = toughest).
	const scheduleRank = $derived.by(() => {
		const withSched = (data?.teams ?? []).filter((t) => t.remainingOpponentPoints != null);
		const ranked = [...withSched].sort(
			(a, b) => b.remainingOpponentPoints! - a.remainingOpponentPoints!
		);
		return new Map(ranked.map((t, i) => [t.teamKey, i + 1]));
	});

	function pct(p: number, locked: boolean): string {
		if (locked) return p >= 1 ? '100%' : '0%';
		// The simulation isn't proof; don't print certainty the math hasn't earned.
		if (p > 0.995) return '>99%';
		if (p < 0.005) return '<1%';
		return `${Math.round(p * 100)}%`;
	}

	function tone(p: number): string {
		if (p >= 0.75) return 'bg-green-400';
		if (p >= 0.4) return 'bg-amber-400';
		if (p >= 0.15) return 'bg-orange-400';
		return 'bg-red-400/80';
	}

	function delta(t: TeamOdds): number | null {
		if (metric !== 'playoffOdds' || t.previousOdds == null) return null;
		const d = Math.round((t.playoffOdds - t.previousOdds) * 100);
		// A point either way is within the simulation's sampling noise.
		return Math.abs(d) < 2 ? null : d;
	}

	const record = (t: TeamOdds) => `${t.wins}-${t.losses}${t.ties ? `-${t.ties}` : ''}`;
	const signed = (x: number, digits = 1) => `${x >= 0 ? '+' : '−'}${Math.abs(x).toFixed(digits)}`;
</script>

<section
	class="rounded-xl border border-green-500/20 bg-gradient-to-br from-green-900/20 to-amber-900/20 p-6"
>
	<div class="mb-4 flex items-start justify-between gap-3">
		<h2 class="flex items-center text-xl font-bold text-white">
			<Trophy class="mr-3 h-5 w-5 shrink-0 text-amber-400" />
			{data?.phase === 'postseason' ? 'Playoff Field' : 'Playoff Odds'}
		</h2>
		{#if data?.available && data.phase === 'regular'}
			<span class="pt-1 text-right text-xs leading-tight text-slate-400">
				Through wk {data.throughWeek}<br />
				{data.sims.toLocaleString()} sims
			</span>
		{/if}
	</div>

	{#if loading}
		<div class="space-y-2" aria-busy="true">
			{#each [0, 1, 2, 3, 4, 5] as row (row)}
				<div class="h-8 animate-pulse rounded bg-slate-700/30"></div>
			{/each}
		</div>
	{:else if failed || !data}
		<p class="text-sm text-slate-400">Playoff odds couldn't be loaded right now.</p>
	{:else if !data.available}
		<p class="text-sm text-slate-400">{data.reason}</p>
	{:else if data.phase === 'postseason'}
		<div class="space-y-1.5">
			{#each seeds as t, i (t.teamKey)}
				<div
					class="flex items-center justify-between text-sm {i < data.playoffTeams
						? 'text-white'
						: 'text-slate-500'}"
				>
					<span class="flex min-w-0 items-center gap-2">
						<span class="w-5 text-right text-slate-400 tabular-nums">{i + 1}</span>
						<span class="truncate">{t.teamName}</span>
						{#if i < data.byes}<span class="text-[10px] tracking-wide text-amber-300 uppercase"
								>bye</span
							>{/if}
					</span>
					<span class="shrink-0 text-slate-400 tabular-nums">{record(t)}</span>
				</div>
				{#if i === data.playoffTeams - 1}
					<div class="my-2 border-t border-dashed border-slate-600"></div>
				{/if}
			{/each}
		</div>
	{:else}
		<div class="mb-4 flex rounded-lg bg-slate-900/40 p-0.5 text-xs font-medium" role="tablist">
			{#each TABS as tab (tab.id)}
				<button
					role="tab"
					aria-selected={metric === tab.id}
					class="flex-1 rounded-md px-2 py-1.5 transition-colors {metric === tab.id
						? 'bg-slate-700 text-white'
						: 'text-slate-400 hover:text-slate-200'}"
					onclick={() => (metric = tab.id)}
				>
					{tab.label}
				</button>
			{/each}
		</div>

		{#if data.throughWeek === 0}
			<p class="mb-3 text-xs text-slate-400">
				No games yet: every team starts level. Odds start moving after week 1.
			</p>
		{/if}

		<ul class="space-y-1">
			{#each sorted as t, i (t.teamKey)}
				{@const p = t[metric]}
				{@const locked = t.eliminated || (metric === 'playoffOdds' && t.clinched)}
				{@const d = delta(t)}
				<li>
					<button
						class="w-full rounded-lg px-2 py-1.5 text-left transition-colors hover:bg-slate-700/30"
						aria-expanded={open === t.teamKey}
						onclick={() => (open = open === t.teamKey ? null : t.teamKey)}
					>
						<span class="flex items-center justify-between gap-2">
							<span class="flex min-w-0 items-baseline gap-1.5">
								<span class="truncate text-sm {t.eliminated ? 'text-slate-500' : 'text-white'}"
									>{t.teamName}</span
								>
								<span class="shrink-0 text-xs text-slate-500 tabular-nums">{record(t)}</span>
							</span>
							<span class="flex shrink-0 items-center gap-1.5">
								{#if d != null}
									<span
										class="text-[11px] tabular-nums {d > 0 ? 'text-green-400' : 'text-red-400'}"
										title="Change since last week"
									>
										{d > 0 ? '▲' : '▼'}{Math.abs(d)}
									</span>
								{/if}
								{#if metric === 'playoffOdds' && t.clinched}
									<span
										class="rounded bg-green-500/15 px-1.5 py-0.5 text-[10px] font-semibold tracking-wide text-green-300 uppercase"
										>Clinched</span
									>
								{:else if metric === 'playoffOdds' && t.eliminated}
									<span
										class="rounded bg-slate-600/30 px-1.5 py-0.5 text-[10px] font-semibold tracking-wide text-slate-400 uppercase"
										>Out</span
									>
								{:else}
									<span class="w-10 text-right text-sm font-semibold text-white tabular-nums"
										>{pct(p, locked)}</span
									>
								{/if}
								<ChevronDown
									class="h-3.5 w-3.5 text-slate-500 transition-transform {open === t.teamKey
										? 'rotate-180'
										: ''}"
								/>
							</span>
						</span>
						<span class="mt-1 block h-1.5 overflow-hidden rounded-full bg-slate-700/50">
							<span
								class="block h-full rounded-full {tone(p)} transition-[width] duration-500"
								style="width: {Math.max(p * 100, p > 0 ? 1.5 : 0)}%"
							></span>
						</span>
					</button>

					{#if open === t.teamKey}
						<dl
							class="mx-2 mt-1 mb-2 grid grid-cols-2 gap-x-3 gap-y-1.5 rounded-lg bg-slate-900/40 p-3 text-xs"
						>
							<dt class="text-slate-400">Projected score</dt>
							<dd class="text-right text-white tabular-nums">
								{t.projectedPoints.toFixed(1)}/wk
								<span class="text-slate-500">#{t.projectedPointsRank}</span>
							</dd>

							<dt class="text-slate-400">All-play record</dt>
							<dd class="text-right text-white tabular-nums">
								{Math.round(t.allPlayWins)}-{Math.round(t.allPlayLosses)}
							</dd>

							<dt class="text-slate-400">Luck</dt>
							<dd
								class="text-right tabular-nums {t.luck >= 0.75
									? 'text-amber-300'
									: t.luck <= -0.75
										? 'text-sky-300'
										: 'text-white'}"
							>
								{signed(t.luck)} wins
							</dd>

							{#if t.remainingOpponentPoints != null}
								{@const r = scheduleRank.get(t.teamKey) ?? 0}
								<dt class="text-slate-400">Schedule left ({t.remainingGames})</dt>
								<dd class="text-right text-white tabular-nums">
									{t.remainingOpponentPoints.toFixed(1)}
									<span
										class={r <= 3
											? 'text-red-300'
											: r > scheduleRank.size - 3
												? 'text-green-300'
												: 'text-slate-500'}
										>{r <= 3 ? 'tough' : r > scheduleRank.size - 3 ? 'soft' : 'avg'}</span
									>
								</dd>
							{/if}

							<dt class="text-slate-400">Projected finish</dt>
							<dd class="text-right text-white tabular-nums">
								{t.projectedWins.toFixed(1)}-{(data.regularSeasonWeeks - t.projectedWins).toFixed(
									1
								)}
							</dd>

							<dt class="text-slate-400">Bye · #1 seed · Title</dt>
							<dd class="text-right text-white tabular-nums">
								{pct(t.byeOdds, t.eliminated)} · {pct(t.topSeedOdds, t.eliminated)} · {pct(
									t.titleOdds,
									t.eliminated
								)}
							</dd>
						</dl>
					{/if}
				</li>
				{#if metric === 'playoffOdds' && i === data.playoffTeams - 1}
					<li class="flex items-center gap-2 py-1" aria-hidden="true">
						<span class="flex-1 border-t border-dashed border-slate-600"></span>
						<span class="text-[10px] tracking-wide text-slate-500 uppercase">Projected cut</span>
						<span class="flex-1 border-t border-dashed border-slate-600"></span>
					</li>
				{/if}
			{/each}
		</ul>

		<button
			class="mt-4 flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-200"
			aria-expanded={showMethod}
			onclick={() => (showMethod = !showMethod)}
		>
			<Info class="h-3.5 w-3.5" /> How these odds work
		</button>
		{#if showMethod}
			<div class="mt-2 space-y-2 text-xs leading-relaxed text-slate-400">
				<p>
					Each team gets a projected weekly score: its season average, pulled toward the league
					average until there are enough games to trust it. Points matter more than record because
					there's no defense in fantasy. A 6-1 team that scores like a 4-3 team keeps its six wins
					but is projected to play like a 4-3 team from here on.
				</p>
				<p>
					The rest of the season is then played out {data.sims.toLocaleString()} times against the real
					remaining schedule (Yahoo repeats the round robin, so it's known). Each run varies how good
					each team really is, lets that drift week to week for injuries and trades, and draws weekly
					scores with each team's own boom-or-bust spread. Seeding is wins, then points for, then a 3v6
					/ 4v5 bracket for the title odds.
				</p>
				<p>
					<span class="text-slate-300">Luck</span> is actual wins minus the wins the team's scores would
					earn on average (all-play). "Clinched" and "Out" are mathematical, not just likely.
				</p>
				<p>
					Tested on every week of the 2008-2025 seasons: teams given about 90% made the playoffs 90%
					of the time, and it beats a record-and-schedule-only model.
				</p>
			</div>
		{/if}
	{/if}
</section>

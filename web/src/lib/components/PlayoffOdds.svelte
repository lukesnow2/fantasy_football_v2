<script lang="ts">
	import { Trophy, ChevronDown, Info } from 'lucide-svelte';
	import {
		compareSeeding,
		type OddsResult,
		type TeamOdds as EngineTeamOdds
	} from '$lib/playoff-odds';

	/**
	 * Simulated playoff odds for the This Season page. Replaces a "Playoff
	 * Picture" card that called any top-six team over .700 "clinched".
	 * The model lives in $lib/playoff-odds; this only presents it.
	 */

	/** What /api/playoff-odds adds to each engine row. */
	interface TeamOdds extends EngineTeamOdds {
		teamName: string;
		managerName: string | null;
		projectedPointsRank: number;
		previousOdds: number | null;
	}

	interface OddsResponse extends Omit<OddsResult, 'teams'> {
		season: number;
		available: boolean;
		reason?: string;
		regularSeasonWeeks: number;
		phase: 'regular' | 'postseason';
		teams: TeamOdds[];
	}

	type Metric = 'playoffOdds' | 'byeOdds' | 'titleOdds';
	const TABS: { id: Metric; label: string }[] = [
		{ id: 'playoffOdds', label: 'Playoffs' },
		{ id: 'byeOdds', label: 'Bye' },
		{ id: 'titleOdds', label: 'Title' }
	];

	interface Props {
		/**
		 * The /api/playoff-odds response, when the page has already fetched it
		 * (This Season shares it with its standings, stat strip and notes).
		 * Without it the component fetches for itself.
		 */
		data?: OddsResponse | null;
	}

	let { data: given }: Props = $props();

	let fetched = $state<OddsResponse | null>(null);
	let loading = $state(true);
	let failed = $state(false);
	let metric = $state<Metric>('playoffOdds');
	let open = $state<number | null>(null);
	let showMethod = $state(false);

	const data = $derived(given !== undefined ? given : fetched);

	$effect(() => {
		if (given !== undefined) {
			loading = false;
			failed = given === null;
			return;
		}
		let cancelled = false;
		(async () => {
			try {
				const res = await fetch('/api/playoff-odds');
				if (!res.ok) throw new Error(String(res.status));
				const body = await res.json();
				if (!cancelled) fetched = body;
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
		return [...data.teams].sort(compareSeeding);
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

	// All-play records can carry a half from a tied week; rounding both sides up
	// would add a game that wasn't played.
	const halves = (x: number) => (Number.isInteger(x) ? String(x) : x.toFixed(1));
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
		{#if data?.available}
			<!-- The season is always named: through the offseason the newest season
			     in the warehouse is last year's, and its field shouldn't pass for this year's. -->
			<span class="pt-1 text-right text-xs leading-tight text-slate-400">
				{#if data.phase === 'regular'}
					{data.season} · wk {data.throughWeek}<br />
					{data.sims.toLocaleString()} sims
				{:else}
					{data.season} season
				{/if}
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
		<!-- A segmented toggle, not ARIA tabs: there's one list that re-sorts, no panels. -->
		<div
			class="mb-4 flex rounded-lg bg-slate-900/40 p-0.5 text-xs font-medium"
			role="group"
			aria-label="Odds shown"
		>
			{#each TABS as tab (tab.id)}
				<button
					aria-pressed={metric === tab.id}
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
								{halves(t.allPlayWins)}-{halves(t.allPlayLosses)}
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
					Each team gets a projected weekly score: its season average, blended with the league
					average as if it had played three extra average games. After 3 games that's half its own
					scoring, after 12 it's 80%. Points matter more than record because there's no defense in
					fantasy, so a 6-1 team that scores like a 4-3 team keeps its six wins but is projected to
					play like a 4-3 team from here on.
				</p>
				<p>
					The rest of the season is then played out {data.sims.toLocaleString()} times. Each run varies
					how good each team really is, lets that drift week to week for injuries and trades, and draws
					weekly scores with each team's own week-to-week spread. The schedule repeats every {data
						.teams.length - 1} weeks, so once everyone has played everyone the rest of it is known; until
					then the unplayed pairings are drawn at random. Seeding is wins, then points for, simulated
					points included.
					{#if data.playoffTeams === 6 && data.byes === 2}
						The top 6 make it and 1 and 2 get byes. Then it's 3 v 6 and 4 v 5, with 1 playing the
						4/5 winner and 2 playing the 3/6 winner.
					{:else if data.byes > 0}
						The top {data.playoffTeams} make it and the top {data.byes} get byes.
					{:else}
						The top {data.playoffTeams} make it.
					{/if}
				</p>
				<dl class="space-y-1.5">
					<div>
						<dt class="inline text-slate-300">Luck:</dt>
						<dd class="inline">
							actual wins minus expected wins. Expected wins are the all-play win rate (how often
							the team's score beat each other team's score that week) times games played.
						</dd>
					</div>
					<div>
						<dt class="inline text-slate-300">Clinched and Out:</dt>
						<dd class="inline">
							no remaining result can change it. A tie on wins counts for neither, since points
							could still break it. Short of that, the most a team shows is &gt;99%.
						</dd>
					</div>
					<div>
						<dt class="inline text-slate-300">Schedule left:</dt>
						<dd class="inline">
							the average projected score of the opponents still to play. The 3 hardest are marked
							tough, the 3 easiest soft.
						</dd>
					</div>
					<div>
						<dt class="inline text-slate-300">Projected finish:</dt>
						<dd class="inline">the average final record across all the runs.</dd>
					</div>
					<div>
						<dt class="inline text-slate-300">Arrows:</dt>
						<dd class="inline">
							the change from last week's odds, run with the same random draws so the move comes
							from last week's results. Moves under 2 points aren't shown.
						</dd>
					</div>
				</dl>
				<p>
					Tested on 2008-2025, every week from week 3 on with at least two weeks left: teams given
					85-95% made the playoffs 90% of the time. That's slightly better than using record and
					schedule alone, but not by enough to rule out luck.
				</p>
			</div>
		{/if}
	{/if}
</section>

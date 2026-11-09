<script lang="ts">
	import { compareSeeding } from '$lib/playoff-odds';
	import { formatOdds, formatRecord } from '$lib/home';
	import {
		oddsDetail,
		oddsMethod,
		oddsTone,
		thousands,
		type OddsResponse,
		type OddsTeam
	} from '$lib/this-season';
	import Card from './ui/Card.svelte';
	import Segmented from './ui/Segmented.svelte';
	import Tag from './ui/Tag.svelte';

	/**
	 * Simulated playoff odds for the This Season page (WP4 S4.4), and the
	 * Playoff Field once the postseason starts (S4.9). The model lives in
	 * $lib/playoff-odds; this only presents it.
	 */

	type TeamOdds = OddsTeam;

	type Metric = 'playoffOdds' | 'byeOdds' | 'titleOdds';
	const TABS: { value: Metric; label: string }[] = [
		{ value: 'playoffOdds', label: 'Playoffs' },
		{ value: 'byeOdds', label: 'Bye' },
		{ value: 'titleOdds', label: 'Title' }
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
	/** The open row. Undefined until a reader picks one: then the top row is open. */
	let chosen = $state<number | null | undefined>(undefined);
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

	const sortBy = (m: Metric) =>
		[...(data?.teams ?? [])].sort((a, b) => b[m] - a[m] || b.projectedWins - a.projectedWins);

	const sorted = $derived(sortBy(metric));

	// The board's state starts with the top row of the Playoffs sort open, and
	// switching the metric keeps the same team open.
	const open = $derived(chosen === undefined ? (sortBy('playoffOdds')[0]?.teamKey ?? null) : chosen);

	// Final seeding, once the regular season is over: wins, then points for.
	const seeds = $derived.by(() => {
		if (!data?.teams) return [];
		return [...data.teams].sort(compareSeeding);
	});

	function delta(t: TeamOdds): number | null {
		if (metric !== 'playoffOdds' || t.previousOdds == null) return null;
		const d = Math.round((t.playoffOdds - t.previousOdds) * 100);
		// A point either way is within the simulation's sampling noise.
		return Math.abs(d) < 2 ? null : d;
	}

	const record = (t: TeamOdds) => formatRecord(t.wins, t.losses, t.ties);
	const postseason = $derived(data?.available === true && data.phase === 'postseason');
	const method = $derived(data?.available ? oddsMethod(data.sims, data.playoffTeams, data.byes) : null);
</script>

<Card
	title={postseason ? 'Playoff Field' : 'Playoff Odds'}
	headSpacing="8px"
	headGap={postseason ? 0 : 12}
>
	{#snippet action()}
		{#if data?.available}
			<!-- The season is always named: through the offseason the newest season
			     in the warehouse is last year's, and its field shouldn't pass for this year's. -->
			<span class="meta">
				{#if data.phase === 'regular'}
					{data.season} · wk {data.throughWeek} · {thousands(data.sims)} sims
				{:else}
					{data.season} season
				{/if}
			</span>
		{/if}
	{/snippet}

	{#if loading}
		<div class="loading" aria-busy="true">
			{#each [0, 1, 2, 3, 4, 5] as row (row)}
				<div class="bar-row"></div>
			{/each}
		</div>
	{:else if failed || !data}
		<p class="note">Playoff odds couldn't be loaded right now.</p>
	{:else if !data.available}
		<p class="note">{data.reason}</p>
	{:else if data.phase === 'postseason'}
		<p class="field-note">Seeded by wins, then points for.</p>
		<ol class="field">
			{#each seeds as t, i (t.teamKey)}
				{#if i === data.playoffTeams}
					<li class="field-cut" aria-hidden="true"></li>
				{/if}
				<li class="seed-row" class:out={i >= data.playoffTeams}>
					<span class="seed">{i + 1}</span>
					<span class="seed-name">{t.managerName ?? 'Unknown manager'}</span>
					{#if i < data.byes}<Tag variant="bye" size="small">Bye</Tag>{/if}
					<span class="seed-record">{record(t)}</span>
				</li>
			{/each}
		</ol>
	{:else}
		<!-- A segmented toggle, not ARIA tabs: there's one list that re-sorts, no panels. -->
		<Segmented label="Odds of" size="toggle" options={TABS} bind:value={metric} />

		{#if data.throughWeek === 0}
			<p class="note week-zero">
				No games yet: every team starts level. Odds start moving after week 1.
			</p>
		{/if}

		<ul class="list">
			{#each sorted as t, i (t.teamKey)}
				{@const p = t[metric]}
				{@const locked = t.eliminated || (metric === 'playoffOdds' && t.clinched)}
				{@const d = delta(t)}
				{@const isOpen = open === t.teamKey}
				<li>
					<button
						type="button"
						class="team"
						aria-expanded={isOpen}
						onclick={() => (chosen = isOpen ? null : t.teamKey)}
					>
						<span class="line">
							<span class="name"
								>{t.managerName ?? 'Unknown manager'}
								<span class="record">{record(t)}</span></span
							>
							{#if d != null}
								<span class="delta" class:down={d < 0} title="Change since last week"
									><span aria-hidden="true">{d > 0 ? '▲' : '▼'}{Math.abs(d)}</span><span
										class="sr-only">{d > 0 ? 'Up' : 'Down'} {Math.abs(d)} since last week</span
									></span
								>
							{/if}
							{#if metric === 'playoffOdds' && t.clinched}
								<Tag variant="clinched" size="row">Clinched</Tag>
							{:else if metric === 'playoffOdds' && t.eliminated}
								<Tag variant="eliminated" size="row">Eliminated</Tag>
							{:else}
								<span class="pct">{formatOdds(p, locked)}</span>
							{/if}
							<svg
								class="chevron"
								class:turned={isOpen}
								width="12"
								height="12"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2.5"
								stroke-linecap="round"
								aria-hidden="true"><path d="M6 9l6 6 6-6" /></svg
							>
						</span>
						<span class="track" aria-hidden="true">
							<span
								class="fill"
								style:width="{Math.max(p * 100, p > 0 ? 1.5 : 0)}%"
								style:background={oddsTone(p * 100)}
							></span>
						</span>
					</button>

					{#if isOpen}
						<dl class="detail">
							{#each oddsDetail(t, data.regularSeasonWeeks) as row (row.term)}
								<div class="pair">
									<dt>{row.term}</dt>
									<dd>{row.value}</dd>
								</div>
							{/each}
						</dl>
					{/if}
				</li>
				{#if metric === 'playoffOdds' && i === data.playoffTeams - 1}
					<li class="cut" aria-hidden="true">
						<span class="dash"></span>Projected cut<span class="dash"></span>
					</li>
				{/if}
			{/each}
		</ul>

		<button
			type="button"
			class="how"
			aria-expanded={showMethod}
			onclick={() => (showMethod = !showMethod)}
		>
			How these odds work {showMethod ? '▴' : '▾'}
		</button>
		{#if showMethod && method}
			<div class="method">
				<p>{method[0]}</p>
				<p>{method[1]}</p>
			</div>
		{/if}
	{/if}
</Card>

<style>
	.meta {
		font-size: 12px;
		color: var(--color-muted);
	}

	.note {
		margin: 0;
		font-size: 14px;
		color: var(--color-muted);
	}

	.week-zero {
		margin-top: 8px;
		font-size: 13px;
	}

	.loading {
		display: grid;
		gap: 12px;
	}

	.bar-row {
		height: 32px;
		border-radius: 6px;
		background: var(--color-line-soft);
	}

	ul,
	ol {
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.list {
		margin-top: 8px;
	}

	.team {
		display: block;
		width: 100%;
		padding: 8px 0;
		border: 0;
		border-bottom: 1px solid var(--color-line-soft);
		background: transparent;
		text-align: left;
		cursor: pointer;
		font-family: var(--font-sans);
		color: var(--color-ink);
	}

	.team:focus-visible,
	.how:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.line {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 14px;
	}

	.name {
		flex: 1;
		min-width: 0;
		font-weight: 600;
	}

	.record {
		font-weight: 500;
		color: var(--color-muted);
	}

	.delta {
		position: relative;
		font-size: 12px;
		font-weight: 700;
		color: var(--color-field);
	}

	.delta.down {
		color: var(--color-pennant);
	}

	.pct {
		width: 38px;
		text-align: right;
		font-weight: 700;
	}

	.chevron {
		flex: none;
		color: var(--color-muted);
	}

	.chevron.turned {
		transform: rotate(180deg);
	}

	.track {
		display: block;
		margin-top: 5px;
		height: 6px;
		border-radius: 3px;
		background: var(--color-wash);
	}

	.fill {
		display: block;
		height: 6px;
		border-radius: 3px;
	}

	.detail {
		margin: 0;
		padding: 10px 12px;
		border-radius: 0 0 8px 8px;
		background: var(--color-ground);
		display: grid;
		grid-template-columns: auto 1fr;
		gap: 5px 12px;
		font-size: 13px;
	}

	.pair {
		display: contents;
	}

	dt {
		color: var(--color-muted);
	}

	dd {
		margin: 0;
		text-align: right;
		font-weight: 600;
	}

	.cut {
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 4px 0;
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-field);
	}

	.dash {
		flex: 1;
		border-top: 1.5px dashed var(--color-field);
	}

	.how {
		margin-top: 10px;
		min-height: 40px;
		padding: 0;
		border: 0;
		background: transparent;
		cursor: pointer;
		font-family: var(--font-sans);
		font-size: 14px;
		font-weight: 600;
		color: var(--color-field);
	}

	.method {
		font-size: 13px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	.method p {
		margin: 8px 0 0;
	}

	.method p:first-child {
		margin-top: 4px;
	}

	/* Playoff Field */
	.field-note {
		margin: 4px 0 8px;
		font-size: 13px;
		color: var(--color-muted);
	}

	.seed-row {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 8px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 14px;
	}

	.seed-row.out {
		opacity: 0.55;
	}

	.seed {
		width: 20px;
		font-family: var(--font-slab);
		font-weight: 700;
	}

	.seed-name {
		flex: 1;
		min-width: 0;
		font-weight: 600;
	}

	.seed-record {
		font-weight: 600;
	}

	.field-cut {
		margin: 4px 0;
		border-top: 1.5px dashed var(--color-field);
	}

	@media (max-width: 639.98px) {
		.line {
			min-height: 20px;
		}

		.detail {
			display: flex;
			flex-direction: column;
			gap: 5px;
		}

		.pair {
			display: flex;
			justify-content: space-between;
			gap: 12px;
		}

		.how {
			margin-top: 6px;
			min-height: 44px;
		}
	}
</style>

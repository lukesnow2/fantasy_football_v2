<script lang="ts">
	import { onMount } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import OddsTrend from '$lib/components/OddsTrend.svelte';
	import {
		Avatar,
		Button,
		Card,
		DataTable,
		Segmented,
		StatTip,
		type Column
	} from '$lib/components/ui';
	import {
		formatOdds,
		formatRecord,
		thousands,
		type OddsResponse,
		type OddsTeam
	} from '$lib/this-season';
	import {
		gradeLines,
		rebuiltNote,
		seasonGrade,
		skillLine,
		type OddsHistory,
		type OddsMetric
	} from '$lib/odds-history';

	/**
	 * The Playoff Odds page: every team's playoff, bye and title odds through a
	 * season, charted the way Power Rankings charts rank, with the latest odds
	 * in a table and, once the title game is played, how the odds did. This
	 * Season shows the same odds in a card; this is the whole of them, for any
	 * season since 2008.
	 */

	const FIRST_SEASON = 2008;

	let loading = $state(true);
	let error = $state('');
	let firstLoad = $state(true);
	let odds = $state<OddsResponse | null>(null);
	let history = $state<OddsHistory | null>(null);
	/** The week-by-week odds failed on their own: the rest of the page still draws. */
	let historyFailed = $state(false);
	let latest = $state<number | null>(null);
	/** The season the select shows while it loads, so Try again loads that one. */
	let requestedSeason = $state<number | null>(null);
	let latestRequest = 0;

	async function load(season?: number) {
		const request = ++latestRequest;
		loading = true;
		error = '';
		try {
			const q = season != null ? `?season=${season}` : '';
			// The history is the slow, optional half: if it fails, the odds
			// table still shows, with a note where the chart and grade go.
			const historyRequest = fetch(`/api/playoff-odds/history${q}`)
				.then((h) => {
					if (!h.ok) throw new Error(String(h.status));
					return h.json() as Promise<OddsHistory>;
				})
				.catch((err) => {
					console.error('Playoff odds history failed to load:', err);
					return null;
				});
			const o = await fetch(`/api/playoff-odds${q}`);
			if (!o.ok) throw new Error(String(o.status));
			const [ob, hb] = await Promise.all([o.json(), historyRequest]);
			if (request !== latestRequest) return;
			odds = ob;
			history = hb;
			historyFailed = hb == null;
			if (latest == null) latest = ob.season;
			requestedSeason = null;
		} catch (err) {
			if (request !== latestRequest) return;
			console.error('Playoff odds page failed to load:', err);
			error = 'Could not load the playoff odds.';
		} finally {
			if (request === latestRequest) {
				loading = false;
				firstLoad = false;
			}
		}
	}

	onMount(() => load());

	const phoneQuery = new MediaQuery('max-width: 639.98px', false);
	const phone = $derived(phoneQuery.current);

	const seasons = $derived(
		latest == null ? [] : Array.from({ length: latest - FIRST_SEASON + 1 }, (_, i) => latest! - i)
	);

	const ready = $derived(odds?.available ? odds : null);
	const trend = $derived(history?.available ? history : null);
	const postseason = $derived(ready?.phase === 'postseason');
	const complete = $derived(ready?.complete === true);

	/** Undefined until the reader picks: the playoffs and after open on the title. */
	let pickedMetric = $state<OddsMetric | undefined>(undefined);
	const metric = $derived<OddsMetric>(pickedMetric ?? (postseason ? 'titleOdds' : 'playoffOdds'));
	const METRICS: { value: OddsMetric; label: string }[] = [
		{ value: 'playoffOdds', label: 'Playoffs' },
		{ value: 'byeOdds', label: 'Bye' },
		{ value: 'titleOdds', label: 'Title' }
	];

	const grade = $derived(complete && trend ? seasonGrade(trend) : null);
	const lines = $derived(grade && trend ? gradeLines(grade, trend.regularSeasonWeeks) : []);
	const skill = $derived(grade ? skillLine(grade) : null);
	const rebuilt = $derived(trend ? rebuiltNote(trend) : null);

	const kicker = $derived.by(() => {
		if (!ready) return undefined;
		if (complete) return `Odds · ${ready.season} · final`;
		if (postseason && ready.bracketWeek)
			return `Odds · ${ready.season} · through week ${ready.bracketWeek}`;
		return `Odds · ${ready.season} · through week ${ready.throughWeek}`;
	});

	/* ---------- The table ---------- */

	const settled = $derived(
		ready != null && postseason && ready.throughWeek >= ready.regularSeasonWeeks
	);
	// Strings ready to print: DataTable rows are plain records.
	type TableRow = {
		teamKey: number;
		rank: number;
		managerName: string;
		record: string;
		projectedWins: string;
		playoffOdds: string;
		byeOdds: string;
		topSeedOdds: string;
		titleOdds: string;
	};
	const tableRows = $derived.by<TableRow[]>(() => {
		if (!ready) return [];
		return [...ready.teams]
			.sort(
				(a, b) =>
					b[metric] - a[metric] ||
					b.playoffOdds - a.playoffOdds ||
					b.projectedWins - a.projectedWins
			)
			.map((t, i) => ({
				teamKey: t.teamKey,
				rank: i + 1,
				managerName: t.managerName ?? 'Unknown manager',
				record: formatRecord(t.wins, t.losses, t.ties),
				projectedWins: t.projectedWins.toFixed(1),
				playoffOdds: cellOdds(t, 'playoffOdds'),
				byeOdds: cellOdds(t, 'byeOdds'),
				topSeedOdds: cellOdds(t, 'topSeedOdds'),
				titleOdds: cellOdds(t, 'titleOdds')
			}));
	});
	const columns = $derived<Column[]>(
		phone
			? [
					{ key: 'manager', label: 'Manager' },
					{ key: 'playoffOdds', label: 'Playoffs', numeric: true, width: '64px' },
					{ key: 'titleOdds', label: 'Title', numeric: true, width: '52px' }
				]
			: [
					{ key: 'rank', label: '#', width: '28px' },
					{ key: 'manager', label: 'Manager' },
					{ key: 'record', label: 'W-L', numeric: true, width: '56px' },
					{ key: 'projectedWins', label: 'Proj. wins', numeric: true, width: '80px' },
					{ key: 'playoffOdds', label: 'Playoffs', numeric: true, width: '76px' },
					{ key: 'byeOdds', label: 'Bye', numeric: true, width: '64px' },
					{ key: 'topSeedOdds', label: '#1 seed', numeric: true, width: '70px' },
					{ key: 'titleOdds', label: 'Title', numeric: true, width: '64px' }
				]
	);

	function cellOdds(
		t: OddsTeam,
		key: 'playoffOdds' | 'byeOdds' | 'topSeedOdds' | 'titleOdds'
	): string {
		const locked =
			t.eliminated ||
			(key === 'playoffOdds' && t.clinched) ||
			(key === 'titleOdds' && (t.knockedOut || complete)) ||
			(settled && key !== 'titleOdds');
		return formatOdds(t[key], locked);
	}
</script>

<svelte:head>
	<title>Playoff Odds · The League</title>
</svelte:head>

<PageHeader {kicker} title="Playoff Odds" pending={firstLoad} phoneActionGap={14}>
	Every team's chances to make the playoffs, earn a bye and win the title, from the rest of the
	season played out {ready ? thousands(ready.sims) : '10,000'} times.
	{#snippet action()}
		{#if seasons.length > 0}
			<label class="season">
				Season
				<select
					value={requestedSeason ?? ready?.season ?? latest}
					onchange={(e) => {
						const next = Number((e.target as HTMLSelectElement).value);
						requestedSeason = next;
						pickedMetric = undefined;
						load(next);
					}}
				>
					{#each seasons as s (s)}
						<option value={s}>{s}</option>
					{/each}
				</select>
			</label>
		{/if}
	{/snippet}
</PageHeader>

{#if loading && !odds}
	<div class="skeleton" aria-busy="true" aria-label="Loading the playoff odds">
		<span class="sk-block"></span>
		<span class="sk-block short"></span>
	</div>
{:else if error}
	<div class="first">
		<Card title="The odds didn't load">
			<p class="muted-line">Check your connection, then try again.</p>
			<Button variant="secondary" class="retry" onclick={() => load(requestedSeason ?? undefined)}
				>Try again</Button
			>
		</Card>
	</div>
{:else if !ready}
	<div class="first">
		<Card title="No odds yet">
			<p class="muted-line">
				{odds && !odds.available ? odds.reason : 'Odds start once a season has teams loaded.'}
			</p>
		</Card>
	</div>
{:else}
	<div class="results" class:stale={loading} aria-busy={loading || undefined}>
		{#if grade && lines.length > 0}
			<div class="first">
				<Card title="How the odds did" headGap={10}>
					{#snippet action()}<span class="meta">{ready.season} season</span>{/snippet}
					<ul class="grade">
						{#each lines as line (line.id)}
							<li>
								<span class="grade-value">{line.value}</span>
								<span class="grade-note">{line.note}</span>
							</li>
						{/each}
					</ul>
					{#if skill}<p class="foot">{skill}</p>{/if}
				</Card>
			</div>
		{/if}

		<div class="first">
			<Card
				title="Odds through the season"
				headGap={16}
				phoneHeadGap={14}
				headSpacing="8px 16px"
				phoneHeadStack
			>
				{#snippet action()}
					<Segmented
						label="Chart the odds of"
						size="toggle"
						options={METRICS}
						value={metric}
						onchange={(v) => (pickedMetric = v)}
					/>
				{/snippet}
				{#if historyFailed}
					<p class="muted-line">The odds by week couldn't be loaded right now.</p>
				{:else if trend && trend.weeks.length >= 2}
					<OddsTrend history={trend} {metric} highlight={ready.champion ?? null} />
					{#if rebuilt}<p class="foot">{rebuilt}</p>{/if}
				{:else}
					<p class="muted-line">The chart starts once there are two weeks of odds.</p>
				{/if}
			</Card>
		</div>

		<div class="first">
			<Card
				title={complete ? 'Final odds' : 'Latest odds'}
				padding="22px 24px 12px"
				phoneHeadGap={0}
			>
				{#snippet action()}
					<span class="meta"
						>{complete
							? 'After the title game'
							: postseason && ready.bracketWeek
								? `Through week ${ready.bracketWeek}`
								: `Through week ${ready.throughWeek}`}</span
					>
				{/snippet}
				<DataTable
					{columns}
					rows={tableRows}
					label="Playoff odds by team"
					rowPadding={12}
					columnGap={14}
					rowKey={(r) => r.teamKey}
				>
					{#snippet header(column)}
						{#if column.key === 'playoffOdds'}
							<StatTip metricId="playoff_probability" label={column.label} />
						{:else}
							{column.label}
						{/if}
					{/snippet}
					{#snippet cell(r, column)}
						{#if column.key === 'rank'}
							<span class="muted">{r.rank}</span>
						{:else if column.key === 'manager'}
							<span class="manager"
								><Avatar name={r.managerName} size={28} /><span class="name">{r.managerName}</span
								>{#if phone}<span class="muted small">{r.record}</span>{/if}</span
							>
						{:else if column.key === 'record'}
							<span class="strong">{r.record}</span>
						{:else}
							<span class:strong={column.key === metric}>{r[column.key as keyof TableRow]}</span>
						{/if}
					{/snippet}
				</DataTable>
			</Card>
		</div>
	</div>
{/if}

<style>
	.season {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		font-size: 14px;
		font-weight: 600;
		color: var(--color-ink-2);
	}

	.season select {
		appearance: auto;
		min-height: 44px;
		padding: 0 12px;
		border-radius: var(--radius-control);
		border: 1px solid var(--color-line-strong);
		background: var(--color-card);
		font-family: var(--font-sans);
		font-size: 15px;
		color: var(--color-ink);
	}

	.season select:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 1px;
	}

	.skeleton {
		margin-top: 28px;
		display: grid;
		gap: 20px;
	}

	.sk-block {
		display: block;
		height: 420px;
		border-radius: var(--radius-control);
		background: var(--color-line-soft);
	}

	.sk-block.short {
		height: 260px;
	}

	.first {
		margin-top: 28px;
	}

	.first :global(.retry) {
		margin-top: 14px;
	}

	.muted-line {
		margin: 0;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	.results {
		transition: opacity 120ms ease;
	}

	.results.stale {
		opacity: 0.55;
	}

	@media (prefers-reduced-motion: reduce) {
		.results {
			transition: none;
		}
	}

	.meta {
		font-size: 13px;
		color: var(--color-muted);
	}

	.foot {
		margin: 12px 0 0;
		font-size: 13px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	/* How the odds did: four verdicts across, two on tablets, one on phones. */
	.grade {
		margin: 0;
		padding: 0;
		list-style: none;
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
		gap: 16px 24px;
	}

	.grade li {
		display: grid;
		align-content: start;
		gap: 4px;
	}

	.grade-value {
		font-family: var(--font-slab);
		font-size: 19px;
		font-weight: 700;
		color: var(--color-ink);
	}

	.grade-note {
		font-size: 14px;
		line-height: 1.45;
		color: var(--color-ink-2);
	}

	.manager {
		display: inline-flex;
		align-items: center;
		gap: 10px;
		min-width: 0;
		font-weight: 600;
	}

	.name {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.strong {
		font-weight: 700;
	}

	.muted {
		color: var(--color-muted);
	}

	.small {
		font-size: 12px;
		font-weight: 500;
	}

	@media (max-width: 639.98px) {
		.first,
		.skeleton {
			margin-top: 14px;
		}
	}
</style>

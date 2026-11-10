<script lang="ts">
	import { onMount } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import {
		Avatar,
		Button,
		Card,
		DataTable,
		EmptyValue,
		Movement,
		OddsBar,
		Segmented,
		StatTip,
		type Column
	} from '$lib/components/ui';
	import {
		formatLuck,
		luckTone,
		powerBarPct,
		RANK_LINE_COLORS,
		rankChartLabel,
		rankingCallouts
	} from '$lib/utils/power-rankings';
	import type { PowerRankingRow } from '../api/power-rankings/+server';

	let loading = $state(true);
	let error = $state('');

	let seasons = $state<number[]>([]);
	let weeks = $state<number[]>([]);
	let season = $state<number | null>(null);
	let week = $state<number | null>(null);
	let rankings = $state<PowerRankingRow[]>([]);
	let trend = $state<PowerRankingRow[]>([]);
	/** True until the first response, so the header knows its season and week. */
	let firstLoad = $state(true);

	// Only the latest request may write: a slow response for a week the reader
	// has already left must not replace the one they picked after it.
	let latestRequest = 0;
	// What the reader last asked for, so Try again retries the week they picked
	// rather than the one still on screen.
	let requested: { season?: number; week?: number } = {};

	async function load(nextSeason?: number, nextWeek?: number) {
		const request = ++latestRequest;
		requested = { season: nextSeason, week: nextWeek };
		loading = true;
		error = '';
		try {
			const params = new URLSearchParams();
			if (nextSeason != null) params.set('season', String(nextSeason));
			if (nextWeek != null) params.set('week', String(nextWeek));
			const response = await fetch(`/api/power-rankings?${params}`);
			if (request !== latestRequest) return;
			if (!response.ok) {
				error = 'Could not load power rankings.';
				return;
			}
			const data = await response.json();
			if (request !== latestRequest) return;
			seasons = data.seasons ?? [];
			weeks = data.weeks ?? [];
			season = data.season;
			week = data.week;
			requestedSeason = null;
			rankings = data.rankings ?? [];
			trend = data.trend ?? [];
		} catch (err) {
			if (request !== latestRequest) return;
			console.error('Error loading power rankings:', err);
			error = 'Could not load power rankings.';
		} finally {
			if (request === latestRequest) {
				loading = false;
				firstLoad = false;
			}
		}
	}

	onMount(() => load());

	function onSeasonChange(event: Event) {
		// Week numbers are not comparable across seasons, so let the API pick the
		// latest week of the newly selected season rather than carrying this one over.
		const next = Number((event.target as HTMLSelectElement).value);
		requestedSeason = next;
		load(next);
	}

	function onWeekChange(next: number) {
		if (season != null) load(season, next);
	}

	// The select keeps the reader's pick while it loads and after a failed
	// change, so it names the season Try again will load.
	let requestedSeason = $state<number | null>(null);

	const isFinalWeek = $derived(week != null && week === weeks[weeks.length - 1]);

	// The phone layout (below 640px) drops columns and moves W-L under the
	// name, which is markup, not spacing. The body only draws in the browser.
	const phoneQuery = new MediaQuery('max-width: 639.98px', false);
	const phone = $derived(phoneQuery.current);

	const kicker = $derived(
		season != null && week != null ? `Rankings · ${season} · through week ${week}` : undefined
	);
	const weekOptions = $derived(weeks.map((w) => ({ value: w, label: `Wk ${w}` })));

	function record(row: PowerRankingRow): string {
		return row.ties > 0 ? `${row.wins}-${row.losses}-${row.ties}` : `${row.wins}-${row.losses}`;
	}

	function profileHref(row: PowerRankingRow): string | null {
		return row.managerName && row.managerHasProfile
			? `/managers/${encodeURIComponent(row.managerName)}`
			: null;
	}

	// ---- S5.2 the three callouts --------------------------------------------
	const callouts = $derived.by(() => {
		const picks = rankingCallouts(rankings);
		return [
			{ key: 'riser', label: 'Biggest riser', row: picks.riser, empty: 'No one moved up' },
			{ key: 'faller', label: 'Biggest faller', row: picks.faller, empty: 'No one moved down' },
			{ key: 'luck', label: 'Luckiest so far', row: picks.luckiest, empty: 'No luck numbers yet' }
		];
	});

	// ---- S5.3 the table -------------------------------------------------------
	const desktopColumns: Column[] = [
		{ key: 'rank', label: '#', width: '32px' },
		{ key: 'move', label: 'Move', width: '52px' },
		{ key: 'manager', label: 'Manager' },
		{ key: 'record', label: 'W-L', numeric: true, width: '56px' },
		{ key: 'power', label: 'Power score', width: '200px' },
		{ key: 'points', label: 'Pts rk', numeric: true, width: '64px' },
		{ key: 'pyth', label: 'Pyth W', numeric: true, width: '64px' },
		{ key: 'luck', label: 'Luck', numeric: true, width: '64px' },
		{ key: 'playoffs', label: 'Playoffs', numeric: true, width: '72px' }
	];
	// The phone board keeps rank, manager, movement and luck, so the Luck
	// StatTip stays in view without scrolling sideways.
	const phoneColumns: Column[] = [
		{ key: 'rank', label: '#', width: '20px' },
		{ key: 'manager', label: 'Manager' },
		{ key: 'move', label: 'Move', width: '44px' },
		{ key: 'luck', label: 'Luck', numeric: true, width: '64px' }
	];
	const columns = $derived(phone ? phoneColumns : desktopColumns);
	const tableRows = $derived(rankings as unknown as (PowerRankingRow & Record<string, unknown>)[]);
	let tableWrap: HTMLDivElement | undefined = $state();

	// ---- S5.5 rank through the season ----------------------------------------
	// Taller than it was (360): ten lines crossing need room between ranks, and
	// each line now ends in its manager's picture, which needs ~50 units per rank
	// and a right margin to sit in.
	const CHART = { width: 960, height: 550, padTop: 28, padRight: 36, padBottom: 52, padLeft: 40 };

	interface TeamSeries {
		teamKey: number;
		teamName: string;
		managerName: string | null;
		color: string;
		points: { week: number; rank: number }[];
		path: string;
		finalRank: number;
		/** Where the line ends, for the manager's picture. */
		end: { x: number; y: number };
	}

	// One scale for the lines, the gridlines and the axis labels.
	const scale = $derived.by(() => {
		if (trend.length === 0 || weeks.length === 0) return null;
		const maxRank = Math.max(...trend.map((r) => r.powerRank));
		const firstWeek = weeks[0];
		const spanX = Math.max(1, weeks[weeks.length - 1] - firstWeek);
		const spanY = Math.max(1, maxRank - 1);
		return {
			maxRank,
			x: (w: number) =>
				CHART.padLeft + ((w - firstWeek) / spanX) * (CHART.width - CHART.padLeft - CHART.padRight),
			// Rank 1 sits at the top, so the y scale is inverted.
			y: (rank: number) =>
				CHART.padTop + ((rank - 1) / spanY) * (CHART.height - CHART.padTop - CHART.padBottom)
		};
	});

	const series = $derived.by<TeamSeries[]>(() => {
		if (!scale) return [];
		const { x, y } = scale;

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
				finalRank: last.powerRank,
				end: { x: x(last.weekNumber), y: y(last.powerRank) }
			};
		});

		built.sort((a, b) => a.finalRank - b.finalRank);
		return built.map((s, i) => ({ ...s, color: RANK_LINE_COLORS[i % RANK_LINE_COLORS.length] }));
	});

	const chartTicks = $derived.by(() => {
		if (!scale)
			return { xs: [] as { week: number; x: number }[], ys: [] as { rank: number; y: number }[] };
		const { x, y, maxRank } = scale;
		return {
			xs: weeks.map((w) => ({ week: w, x: x(w) })),
			ys: Array.from({ length: maxRank }, (_, i) => ({ rank: i + 1, y: y(i + 1) }))
		};
	});

	const chartLabel = $derived(rankChartLabel(series, weeks));

	let hoveredTeam = $state<number | null>(null);

	/** Percent of the chart's width or height, for the HTML labels and avatars over the SVG. */
	const pctX = (x: number) => `${(x / CHART.width) * 100}%`;
	const pctY = (y: number) => `${(y / CHART.height) * 100}%`;
</script>

<svelte:head>
	<title>Power Rankings · The League</title>
</svelte:head>

<PageHeader {kicker} title="Power Rankings" pending={firstLoad} phoneActionGap={14}>
	Ranked on points, record, schedule and recent form, so a lucky start doesn't count for as much.
	{#snippet action()}
		{#if seasons.length > 0}
			<div class="controls">
				<label class="season">
					Season
					<select value={requestedSeason ?? season} onchange={onSeasonChange}>
						{#each seasons as s (s)}
							<option value={s}>{s}</option>
						{/each}
					</select>
				</label>
				{#if weekOptions.length > 0}
					<Segmented label="Week" options={weekOptions} value={week ?? 0} onchange={onWeekChange} />
				{/if}
			</div>
		{/if}
	{/snippet}
</PageHeader>

{#if loading && rankings.length === 0}
	<!-- S5.7 Loading: ten rows where the table sits. A week change keeps the
	     last week on screen, dimmed, so the page doesn't collapse and jump. -->
	<div class="skeleton" aria-busy="true" aria-label="Loading power rankings">
		{#each Array(10) as _, i (i)}<span class="sk-row"></span>{/each}
	</div>
{:else if error}
	<div class="first">
		<Card title="Rankings didn't load">
			<p class="muted-line">Check your connection, then try again.</p>
			<Button
				variant="secondary"
				class="retry"
				onclick={() => load(requested.season, requested.week)}>Try again</Button
			>
		</Card>
	</div>
{:else if rankings.length === 0}
	<div class="first">
		<Card title="No rankings yet">
			<p class="muted-line">Rankings start once week 1 of a season is final.</p>
		</Card>
	</div>
{:else}
	<div class="results" class:stale={loading} aria-busy={loading || undefined}>
		<!-- S5.2 Three callouts -->
		<div class="callouts">
			{#each callouts as c (c.key)}
				{@const href = c.row ? profileHref(c.row) : null}
				<Card padding="18px 20px" phonePadding="14px 16px" class="callout">
					<div class="tile">
						{#if c.row}
							<Avatar name={c.row.managerName ?? ''} size={phone ? 44 : 52} />
						{/if}
						<div class="who">
							<div class="tile-label">{c.label}</div>
							{#if !c.row}
								<div class="tile-name none">{c.empty}</div>
							{:else if !c.row.managerName}
								<div class="tile-name none">Unknown manager</div>
							{:else if href}
								<a class="tile-name tile-link" {href}>{c.row.managerName}</a>
							{:else}
								<div class="tile-name">{c.row.managerName}</div>
							{/if}
						</div>
						{#if c.row}
							<span class="tile-value">
								{#if c.key === 'luck'}
									{formatLuck(c.row.luckFactor, 2)}
								{:else}
									<Movement change={c.row.rankChange} inherit />
								{/if}
							</span>
						{/if}
					</div>
				</Card>
			{/each}
		</div>

		<!-- S5.3 The rankings table -->
		<div class="table-card">
			<Card padding="22px 24px 12px" phonePadding="6px 16px 10px">
				<div bind:this={tableWrap}>
					<DataTable
						{columns}
						rows={tableRows}
						label="Power rankings, week {week}"
						rowPadding={phone ? 4 : 12}
						columnGap={phone ? 10 : 14}
						fontSize={phone ? 14 : 15}
						headerHeight={phone ? 44 : undefined}
						minWidth={phone ? undefined : '900px'}
						rowKey={(r) => r.teamKey}
					>
						{#snippet header(column)}
							{#if column.key === 'power'}
								<StatTip metricId="power_score" label="Power score" />
							{:else if column.key === 'pyth'}
								<StatTip metricId="pythagorean_wins" label="Pyth W" />
							{:else if column.key === 'luck'}
								<span class="luck-head">
									<StatTip
										metricId="luck_factor"
										label="Luck"
										endOffset={phone ? undefined : 40}
										topOffset={phone ? undefined : 26}
										spanOf={phone ? tableWrap : undefined}
										phoneTarget
									/>
								</span>
							{:else}
								{column.label}
							{/if}
						{/snippet}
						{#snippet cell(r, column)}
							{#if column.key === 'rank'}
								<span class="rank">{r.powerRank}</span>
							{:else if column.key === 'move'}
								<span class="move"><Movement change={r.rankChange} inherit /></span>
							{:else if column.key === 'manager'}
								{@const href = profileHref(r)}
								{#if !r.managerName}
									<span class="manager"><EmptyValue text="Unknown manager" /></span>
								{:else}
									<svelte:element this={href ? 'a' : 'span'} class="manager" {href}>
										<Avatar name={r.managerName} size={phone ? 28 : 30} />
										<span class="manager-text">
											<span class="manager-name">{r.managerName}</span>
											{#if phone}<span class="manager-record">{record(r)}</span>{/if}
										</span>
									</svelte:element>
								{/if}
							{:else if column.key === 'record'}
								<span class="strong">{record(r)}</span>
							{:else if column.key === 'power'}
								{#if r.powerScore == null}
									<EmptyValue />
								{:else}
									<OddsBar
										percent={powerBarPct(r.powerScore)}
										text={r.powerScore.toFixed(3)}
										numberWidth={44}
									/>
								{/if}
							{:else if column.key === 'points'}
								{#if r.pointsRank == null}<EmptyValue />{:else}<span class="soft"
										>{r.pointsRank}</span
									>{/if}
							{:else if column.key === 'pyth'}
								{#if r.pythagoreanWins == null}<EmptyValue />{:else}<span class="soft"
										>{r.pythagoreanWins.toFixed(1)}</span
									>{/if}
							{:else if column.key === 'luck'}
								{@const text = formatLuck(r.luckFactor, 2)}
								{#if text === null}<EmptyValue />{:else}<span
										class="luck {luckTone(r.luckFactor, 2)}">{text}</span
									>{/if}
							{:else if column.key === 'playoffs'}
								{#if r.playoffOdds == null}<EmptyValue text="Unknown" />{:else}<span class="strong"
										>{Math.round(r.playoffOdds * 100)}%</span
									>{/if}
							{/if}
						{/snippet}
					</DataTable>
				</div>
				<!-- The 10,000 is SIMS in $lib/server/playoff-odds-service.ts. -->
				<p class="footnote">
					{#if isFinalWeek}Week {week} is the last ranked week of {season}.{' '}{/if}Power rankings
					cover the regular season only. Luck is actual wins minus Pythagorean wins. Playoff odds
					come from the same 10,000-sim model as This Season.
				</p>
			</Card>
		</div>

		<!-- S5.5 Rank through the season -->
		{#if series.length > 0 && weeks.length > 1}
			<div class="chart-card">
				<Card
					title="Rank through the season"
					headGap={16}
					phoneHeadGap={14}
					headSpacing="8px 16px"
					phoneHeadStack
				>
					{#snippet action()}
						<span class="chart-note">Every ranked week of {season}. Rank 1 is at the top.</span>
					{/snippet}
					<div class="chart-row">
						<div class="chart-scroll">
							<div class="chart" role="img" aria-label={chartLabel}>
								<svg viewBox="0 0 {CHART.width} {CHART.height}" aria-hidden="true">
									{#each chartTicks.ys as tick (tick.rank)}
										<line
											x1={CHART.padLeft}
											y1={tick.y}
											x2={CHART.width - CHART.padRight}
											y2={tick.y}
											style:stroke="var(--color-line-soft)"
											stroke-width="1"
										/>
									{/each}
									{#each series as s (s.teamKey)}
										<path
											d={s.path}
											fill="none"
											style:stroke={s.color}
											stroke-width={hoveredTeam === s.teamKey ? 6 : 3}
											stroke-opacity={hoveredTeam !== null && hoveredTeam !== s.teamKey
												? 0.18
												: 0.9}
											stroke-linejoin="round"
											stroke-linecap="round"
										/>
									{/each}
								</svg>
								{#each chartTicks.ys as tick (tick.rank)}
									<span class="axis rank-label" style:top={pctY(tick.y)}>#{tick.rank}</span>
								{/each}
								{#each chartTicks.xs as tick (tick.week)}
									<span class="axis week-label" style:left={pctX(tick.x)}>Wk {tick.week}</span>
								{/each}
								<!-- Each line ends in its manager's picture (an initial when there
							     is none), ringed in the line's color. Hovering one highlights
							     its line. -->
								{#each series as s (s.teamKey)}
									<span
										class="end"
										role="presentation"
										title="{s.managerName ?? 'Unknown manager'}: #{s.finalRank}"
										style:left={pctX(s.end.x)}
										style:top={pctY(s.end.y)}
										style:opacity={hoveredTeam !== null && hoveredTeam !== s.teamKey ? 0.35 : 1}
										onmouseenter={() => (hoveredTeam = s.teamKey)}
										onmouseleave={() => (hoveredTeam = null)}
									>
										<Avatar name={s.managerName ?? ''} size={26} ring={2.5} ringColor={s.color} />
									</span>
								{/each}
							</div>
						</div>
						<div class="legend" role="group" aria-label="Legend">
							{#each series as s (s.teamKey)}
								<button
									type="button"
									class:on={hoveredTeam === s.teamKey}
									onmouseenter={() => (hoveredTeam = s.teamKey)}
									onmouseleave={() => (hoveredTeam = null)}
									onfocus={() => (hoveredTeam = s.teamKey)}
									onblur={() => (hoveredTeam = null)}
									onclick={() => (hoveredTeam = s.teamKey)}
								>
									<span class="dot" style:background={s.color}></span>
									<span class="legend-name">{s.managerName ?? 'Unknown manager'}</span>
									<span class="legend-rank">#{s.finalRank}</span>
								</button>
							{/each}
						</div>
					</div>
				</Card>
			</div>
		{/if}
	</div>
{/if}

<style>
	/* ---------- S5.1 Header controls ---------- */
	.controls {
		display: flex;
		flex-wrap: wrap;
		gap: 10px;
		align-items: center;
	}

	.season {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		font-size: 14px;
		font-weight: 600;
		color: var(--color-ink-2);
	}

	/* The board draws the native select, arrow and all; preflight strips it. */
	.season select {
		appearance: auto;
		min-height: 44px;
		padding: 0 12px;
		border-radius: var(--radius-control);
		border: 1px solid var(--color-line-strong);
		background: var(--color-card);
		font-family: var(--font-sans);
		font-size: 15px;
		font-weight: 600;
		color: var(--color-ink);
	}

	.season select:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 1px;
	}

	/* ---------- S5.7 Loading, error, empty ---------- */
	.skeleton {
		margin-top: 28px;
		display: grid;
		gap: 12px;
	}

	.sk-row {
		display: block;
		height: 52px;
		border-radius: var(--radius-control);
		background: var(--color-line-soft);
	}

	.first {
		margin-top: 28px;
	}

	.muted-line {
		margin: 0;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	.first :global(.retry) {
		margin-top: 14px;
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

	/* ---------- S5.2 Callouts ---------- */
	.callouts {
		margin-top: 28px;
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
		gap: 16px;
	}

	.tile {
		display: flex;
		align-items: center;
		gap: 14px;
	}

	.who {
		flex: 1;
		min-width: 0;
	}

	.tile-label {
		font-size: 13px;
		font-weight: 600;
		color: var(--color-muted);
	}

	.tile-name {
		display: block;
		margin-top: 2px;
		font-size: 17px;
		font-weight: 700;
		color: var(--color-ink);
	}

	a.tile-name:hover {
		color: var(--color-field);
	}

	.tile-name.none {
		font-weight: 600;
		color: var(--color-muted);
	}

	.tile-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 30px;
		color: var(--color-ink);
		white-space: nowrap;
	}

	/* ---------- S5.3 Table ---------- */
	.table-card,
	.chart-card {
		margin-top: 20px;
	}

	.rank {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 20px;
	}

	.move {
		font-size: 13px;
		font-weight: 700;
	}

	.manager {
		display: flex;
		align-items: center;
		gap: 10px;
		min-width: 0;
		font-weight: 600;
	}

	.manager-text {
		min-width: 0;
	}

	.manager-name {
		display: block;
	}

	.manager-record {
		display: block;
		margin-top: 1px;
		font-size: 12px;
		font-weight: 400;
		color: var(--color-muted);
	}

	.strong {
		font-weight: 600;
	}

	.soft {
		color: var(--color-ink-2);
	}

	.luck {
		font-weight: 600;
	}

	.luck.up {
		color: var(--color-field);
	}

	.luck.down {
		color: var(--color-pennant);
	}

	.luck.flat {
		color: var(--color-muted);
	}

	.footnote {
		margin: 14px 0 6px;
		font-size: 13px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	/* ---------- S5.5 Chart ---------- */
	.chart-note {
		font-size: 13px;
		color: var(--color-muted);
	}

	.chart-row {
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: flex-start;
	}

	.chart-scroll {
		flex: 999 1 560px;
		min-width: 0;
		overflow-x: auto;
	}

	.chart {
		position: relative;
		min-width: 560px;
	}

	.chart svg {
		display: block;
		width: 100%;
		height: auto;
	}

	.axis {
		position: absolute;
		font-family: var(--font-sans);
		font-size: 12px;
		font-weight: 600;
		color: var(--color-muted);
		white-space: nowrap;
	}

	.rank-label {
		left: 0;
		transform: translateY(-50%);
	}

	.week-label {
		bottom: 0;
		transform: translateX(-50%);
	}

	.end {
		position: absolute;
		display: inline-flex;
		transform: translate(-50%, -50%);
	}

	.legend {
		flex: 1 1 200px;
		display: flex;
		flex-direction: column;
		gap: 2px;
	}

	.legend button {
		display: flex;
		align-items: center;
		gap: 10px;
		min-height: 34px;
		padding: 0 10px;
		border: 0;
		border-radius: var(--radius-control);
		background: transparent;
		cursor: default;
		font-family: var(--font-sans);
		font-size: 14px;
		font-weight: 600;
		color: var(--color-ink);
		text-align: left;
	}

	.legend button.on {
		background: var(--color-field-tint);
	}

	.legend button:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 1px;
	}

	.dot {
		flex: none;
		width: 10px;
		height: 10px;
		border-radius: 50%;
	}

	.legend-name {
		flex: 1;
	}

	.legend-rank {
		color: var(--color-muted);
	}

	/* ---------- S5.8 Phones ---------- */
	@media (max-width: 639.98px) {
		.season select {
			min-height: 48px;
			border-radius: 10px;
			font-size: 16px;
		}

		.skeleton,
		.first {
			margin-top: 14px;
		}

		.callouts {
			margin-top: 14px;
			display: flex;
			flex-direction: column;
			gap: 10px;
		}

		/* The whole tile is the link to the profile. */
		.callouts :global(.callout) {
			position: relative;
		}

		.tile-link::after {
			content: '';
			position: absolute;
			inset: 0;
			border-radius: var(--radius-card);
		}

		.tile {
			gap: 12px;
		}

		.tile-name {
			font-size: 16px;
		}

		.tile-value {
			font-size: 26px;
		}

		.table-card,
		.chart-card {
			margin-top: 14px;
		}

		.rank {
			font-size: 18px;
		}

		a.manager,
		span.manager {
			min-height: 44px;
		}

		.manager-name {
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
		}

		.footnote {
			margin: 12px 0 4px;
		}

		.chart-row {
			gap: 14px;
		}

		/* Opens scrolled to its right end, so the latest week shows first. */
		.chart-scroll {
			flex-basis: 100%;
			direction: rtl;
		}

		.chart {
			direction: ltr;
		}

		.legend {
			flex-basis: 100%;
		}

		.legend button {
			min-height: 44px;
		}
	}
</style>

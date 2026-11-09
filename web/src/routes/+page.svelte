<script lang="ts">
	/**
	 * Home (WP3, the Home board). Each card fetches what it needs and draws its
	 * own loading and failed state, so one slow query never blanks the page.
	 * Every line is computed by a pure helper in $lib/home.
	 */
	import { onMount } from 'svelte';
	import {
		Avatar,
		Button,
		Card,
		EmptyValue,
		Pennant,
		StatStrip,
		StatTile,
		Tag
	} from '$lib/components/ui';
	import {
		bannerWall,
		featuredRecords,
		finishTag,
		formatOdds,
		formatRecord,
		meetingsSince,
		profileHref,
		RIVALRY_LINE,
		rivalry,
		statStrip,
		thisWeekMode,
		titlesLabel,
		tradeTag,
		type H2HRow,
		type HallOfFameRow,
		type Overview,
		type RecordRow,
		type TradeRow
	} from '$lib/home';

	/** One API response with its own loading and failed state. */
	class Resource<T> {
		status = $state<'loading' | 'ready' | 'failed'>('loading');
		data = $state.raw<T | null>(null);

		private url: string;

		constructor(url: string) {
			this.url = url;
		}

		load = async () => {
			this.status = 'loading';
			try {
				const res = await fetch(this.url);
				if (!res.ok) throw new Error(`${this.url} answered ${res.status}`);
				this.data = (await res.json()) as T;
				this.status = 'ready';
			} catch (error) {
				console.error(`Home couldn't load ${this.url}:`, error);
				this.status = 'failed';
			}
		};
	}

	interface StandingsRow {
		managerName: string;
		wins: number;
		losses: number;
		ties: number;
		playoffTier?: string | null;
	}

	interface OddsTeam {
		managerName: string;
		playoffOdds: number;
		clinched: boolean;
		eliminated: boolean;
	}

	const overview = new Resource<{ overview: Overview }>('/api/leagues/overview');
	const standings = new Resource<{
		standings: StandingsRow[];
		season: number;
		currentWeek: number;
		regularSeasonWeeks: number;
		isSeasonComplete: boolean;
	}>('/api/standings');
	const odds = new Resource<{ available?: boolean; phase?: string; teams?: OddsTeam[] }>(
		'/api/playoff-odds'
	);
	const hallOfFame = new Resource<{ hallOfFame: HallOfFameRow[] }>('/api/hall-of-fame');
	const recordBook = new Resource<{ records: RecordRow[] }>('/api/record-book');
	const headToHead = new Resource<{ headToHead: H2HRow[] }>('/api/head-to-head?analysis=none');
	const trades = new Resource<{
		trades: (TradeRow & {
			seasonYear: number;
			transactionWeek: number;
			teamAGives: string;
			teamBGives: string;
		})[];
	}>('/api/trades?limit=3&analysis=none');

	onMount(() => {
		for (const r of [overview, standings, odds, hallOfFame, recordBook, headToHead, trades])
			r.load();
	});

	let innerWidth = $state(1280);
	const phone = $derived(innerWidth < 640);

	const ov = $derived(overview.data?.overview ?? null);
	// " in 2005", or " in " before a placeholder bar while the overview loads.
	const introYear = $derived(
		ov ? ` in ${ov.firstSeason}` : overview.status === 'loading' ? ' in ' : ''
	);

	// ---- This week ----
	const weekData = $derived(standings.data);
	const mode = $derived(
		thisWeekMode(
			weekData?.isSeasonComplete,
			odds.status === 'ready' ? odds.data?.phase : undefined,
			weekData?.currentWeek,
			weekData?.regularSeasonWeeks
		)
	);
	const oddsByManager = $derived(
		new Map(
			odds.status === 'ready' && odds.data?.available !== false
				? (odds.data?.teams ?? []).map((t) => [t.managerName, t])
				: []
		)
	);
	const showOdds = $derived(
		mode === 'regular' && (odds.status === 'loading' || oddsByManager.size > 0)
	);
	const top4 = $derived((weekData?.standings ?? []).slice(0, 4));

	// ---- Banner wall, rivalry ----
	const banners = $derived(bannerWall(hallOfFame.data?.hallOfFame ?? []));
	const pair = $derived(headToHead.data?.headToHead?.[0] ?? null);
	const series = $derived(pair ? rivalry(pair) : null);
	const rivalAvatar = $derived(phone ? 64 : 84);

	const records = $derived(featuredRecords(recordBook.data?.records ?? []));
</script>

<svelte:window bind:innerWidth />

{#snippet failed(what: string, retry: () => void)}
	<div class="state">
		<p>Couldn't load {what}.</p>
		<Button variant="secondary" onclick={retry}>Try again</Button>
	</div>
{/snippet}

{#snippet bars(count: number, height: number, tone = 'soft')}
	<div class="bars" aria-hidden="true">
		{#each [...Array(count).keys()] as i (i)}
			<span class="bar {tone}" style:height="{height}px"></span>
		{/each}
	</div>
{/snippet}

{#snippet manager(name: string)}
	<a href={profileHref(name)}>{name}</a>
{/snippet}

<div class="home">
	<!-- S3.1 The hero band -->
	<div class="band">
		<div class="band-row">
			<div class="hero-text">
				<div class="kicker">
					{#if ov}
						Season {ov.totalSeasons} · Since {ov.firstSeason}
					{:else}
						<span
							class="band-bar kicker-bar"
							class:hidden={overview.status !== 'loading'}
							aria-hidden="true"
						></span>
					{/if}
				</div>
				<h1>The League</h1>
				<p class="intro">
					Standings, history and records for one fantasy football league, going back to the first
					draft{introYear}{#if !ov && overview.status === 'loading'}<span
							class="band-bar year-bar"
							aria-hidden="true"
						></span>{/if}.
				</p>
				<div class="hero-links">
					<Button variant="band-primary" size="large" href="/this-season">This season →</Button>
					<Button variant="band-outline" size="large" href="/historical">League history</Button>
				</div>
			</div>

			<!-- S3.2 This week -->
			<div class="this-week">
				<Card
					title={mode === 'final' ? 'Final standings' : 'This week'}
					titleSize={26}
					headGap={10}
					tone="borderless"
				>
					{#snippet action()}
						{#if weekData}
							<span class="week-label">
								{#if mode === 'final'}
									{weekData.season} · Final
								{:else if mode === 'playoffs'}
									Playoffs
								{:else}
									Wk {weekData.currentWeek} of {weekData.regularSeasonWeeks}
								{/if}
							</span>
						{/if}
					{/snippet}

					{#if standings.status === 'loading'}
						{@render bars(4, 45)}
					{:else if standings.status === 'failed'}
						{@render failed('the standings', standings.load)}
					{:else if top4.length === 0}
						<p class="empty">No games yet.</p>
					{:else}
						<ol class="week-rows">
							{#each top4 as row, i (row.managerName)}
								{@const team = oddsByManager.get(row.managerName)}
								<li class="week-row">
									<span class="week-rank">{i + 1}</span>
									<Avatar name={row.managerName} size={32} initialTone="field" />
									<span class="week-name">{@render manager(row.managerName)}</span>
									<span class="week-wl">{formatRecord(row.wins, row.losses, row.ties)}</span>
									{#if mode === 'final'}
										{@const tag = finishTag(row.playoffTier)}
										{#if tag}<Tag variant={tag.variant}>{tag.text}</Tag>{/if}
									{:else if showOdds}
										<span class="week-odds">
											{#if team}
												{formatOdds(team.playoffOdds, team.clinched || team.eliminated)}
											{:else if odds.status === 'ready'}
												<EmptyValue />
											{/if}
										</span>
									{/if}
								</li>
							{/each}
						</ol>
						<div class="week-foot">
							<span>
								{mode === 'final'
									? 'Record · finish'
									: showOdds
										? 'Record · playoff odds'
										: 'Record'}
							</span>
							<a class="field-link" href="/this-season">
								{mode === 'final' ? 'Final standings →' : 'Full standings →'}
							</a>
						</div>
					{/if}
				</Card>
			</div>
		</div>
	</div>

	<div class="main">
		<!-- S3.3 The stat strip -->
		<div class="strip">
			{#if ov}
				<StatStrip label="The league in numbers" twoUpOnPhones>
					{#each statStrip(ov) as stat (stat.label)}
						<StatTile valueFirst value={stat.value} label={stat.label} />
					{/each}
				</StatStrip>
			{:else}
				<Card padding="20px 22px">
					{#if overview.status !== 'loading'}
						{@render failed('the league numbers', overview.load)}
					{:else}
						{@render bars(1, 43)}
					{/if}
				</Card>
			{/if}
		</div>

		<!-- S3.4 The banner wall -->
		<Card title="The banner wall" padding="24px" headGap={16} class="gap-top">
			{#snippet action()}
				{#if ov}
					<span class="wall-note">
						{ov.championshipGames} titles decided · one pennant per championship
					</span>
				{/if}
			{/snippet}
			{#if hallOfFame.status === 'loading'}
				{@render bars(3, 46)}
			{:else if hallOfFame.status === 'failed'}
				{@render failed('the banner wall', hallOfFame.load)}
			{:else if banners.length === 0}
				<p class="empty">No titles decided yet.</p>
			{:else}
				<ul class="wall">
					{#each banners as b (b.name)}
						<li class="wall-row">
							<Avatar name={b.name} size={36} initialTone="field" initialSize={16} />
							<span class="wall-name">{@render manager(b.name)}</span>
							<span class="pennants" role="img" aria-label={titlesLabel(b.count)}>
								{#each [...Array(b.count).keys()] as i (i)}<Pennant />{/each}
							</span>
							<span class="wall-count">{b.count}</span>
						</li>
					{/each}
				</ul>
			{/if}
		</Card>

		<div class="pair gap-top">
			<!-- S3.5 From the record book -->
			<Card title="From the record book" padding="24px" headGap={6} class="half">
				{#if recordBook.status === 'loading'}
					{@render bars(3, 66)}
				{:else if recordBook.status === 'failed'}
					{@render failed('the record book', recordBook.load)}
				{:else if records.length === 0}
					<p class="empty">No records yet.</p>
				{:else}
					<ul class="records">
						{#each records as r (r.label)}
							<li class="record">
								<div class="record-label">{r.label}</div>
								<div class="record-line">
									<span class="record-value">{r.value}</span>
									<span class="record-who"><b>{@render manager(r.who)}</b> · {r.when}</span>
								</div>
							</li>
						{/each}
					</ul>
				{/if}
				<Button variant="link" size="tap" href="/historical?tab=records">All records →</Button>
			</Card>

			<!-- S3.6 Biggest rivalry -->
			<Card tone="pennant" padding="24px" class="half rivalry">
				<div class="rivalry-kicker">Biggest rivalry</div>
				{#if headToHead.status === 'loading'}
					<div class="rivalry-loading">{@render bars(2, 60, 'pennant')}</div>
				{:else if headToHead.status === 'failed'}
					{@render failed('the biggest rivalry', headToHead.load)}
				{:else if !series}
					<p class="empty">No rivalries yet.</p>
				{:else}
					<div class="faceoff">
						<div class="side">
							<Avatar name={series.left} size={rivalAvatar} ring={3} initialTone="field" />
							<span class="side-name">{@render manager(series.left)}</span>
						</div>
						<div class="score">
							<div class="score-num">{series.score}</div>
							<div class="score-note">{meetingsSince(series.meetings, series.since)}</div>
						</div>
						<div class="side">
							<Avatar name={series.right} size={rivalAvatar} ring={3} initialTone="field" />
							<span class="side-name">{@render manager(series.right)}</span>
						</div>
					</div>
					<div
						class="series-bar"
						role="img"
						aria-label="{series.left} {series.leftWins} wins, {series.right} {series.rightWins} wins"
					>
						<span class="seg left" style:width="{series.leftShare}%"></span>
						<span class="seg right" style:width="{100 - series.leftShare}%"></span>
					</div>
					<p class="rivalry-line">{RIVALRY_LINE}</p>
				{/if}
				<div class="rivalry-link">
					<Button variant="link-pennant" size="tap" href="/historical?tab=head-to-head"
						>Head to head →</Button
					>
				</div>
			</Card>
		</div>

		<!-- S3.7 Latest trades -->
		<Card title="Latest trades" padding="24px" headGap={8} class="gap-top">
			{#snippet action()}
				<Button variant="link" href="/trades">Trade Center →</Button>
			{/snippet}
			{#if trades.status === 'loading'}
				{@render bars(3, 48)}
			{:else if trades.status === 'failed'}
				{@render failed('the latest trades', trades.load)}
			{:else if (trades.data?.trades ?? []).length === 0}
				<p class="empty">No trades yet.</p>
			{:else}
				<ul class="trades">
					{#each trades.data?.trades ?? [] as t, i (i)}
						{@const tag = tradeTag(t)}
						<li class="trade">
							<span class="trade-when">Wk {t.transactionWeek} · {t.seasonYear}</span>
							<span class="trade-side"
								><b>{@render manager(t.teamAManager)}</b> sends {t.teamAGives}</span
							>
							<span class="trade-side"
								><b>{@render manager(t.teamBManager)}</b> sends {t.teamBGives}</span
							>
							{#if tag}<Tag variant={tag.even ? 'eliminated' : 'bye'}>{tag.text}</Tag>{/if}
						</li>
					{/each}
				</ul>
			{/if}
		</Card>
	</div>
</div>

<style>
	/* ---- S3.1 The hero band ---- */

	.band {
		background: var(--color-clubhouse);
		color: var(--color-white);
		border-top: 1px solid var(--color-clubhouse-3);
	}

	.band-row {
		box-sizing: border-box;
		max-width: 1200px;
		margin: 0 auto;
		padding: 56px 24px 64px;
		display: flex;
		flex-wrap: wrap;
		gap: 40px;
		align-items: flex-start;
	}

	.hero-text {
		flex: 999 1 520px;
		min-width: 0;
	}

	.kicker {
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-pennant-light);
	}

	.band-bar {
		display: inline-block;
		height: 1em;
		vertical-align: middle;
		border-radius: 4px;
		background: var(--color-clubhouse-2);
	}

	.kicker-bar {
		width: 190px;
	}

	.kicker-bar.hidden {
		visibility: hidden;
	}

	.year-bar {
		width: 2.4em;
	}

	h1 {
		margin: 12px 0 16px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 96px;
		line-height: 1;
		letter-spacing: -0.015em;
	}

	.intro {
		margin: 0 0 28px;
		max-width: 560px;
		font-size: 19px;
		line-height: 1.55;
		color: var(--color-on-clubhouse);
	}

	.hero-links {
		display: flex;
		flex-wrap: wrap;
		gap: 12px;
	}

	/* ---- S3.2 This week ---- */

	/* The board's card is content-box: 340px inside 24px of side padding. */
	.this-week {
		flex: 1 1 388px;
		min-width: 0;
		color: var(--color-ink);
	}

	.week-label {
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-field);
	}

	.week-rows,
	.wall,
	.records,
	.trades {
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.week-row {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 10px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.week-rank {
		width: 18px;
		flex: none;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 18px;
		color: var(--color-ink-2);
	}

	.week-name {
		flex: 1;
		min-width: 0;
		font-weight: 600;
	}

	.week-wl {
		font-weight: 600;
	}

	.week-odds {
		width: 44px;
		flex: none;
		text-align: right;
		font-size: 14px;
		font-weight: 600;
		color: var(--color-field);
	}

	.week-foot {
		margin-top: 10px;
		display: flex;
		justify-content: space-between;
		gap: 12px;
		font-size: 13px;
		color: var(--color-muted);
	}

	.week-foot a {
		font-weight: 600;
	}

	/* ---- The main column ---- */

	.main {
		box-sizing: border-box;
		max-width: 1200px;
		margin: 0 auto;
		padding: 0 24px 64px;
	}

	/* S3.3: the strip overlaps the band's bottom edge. */
	.strip {
		margin-top: -28px;
		position: relative;
	}

	.main :global(.gap-top) {
		margin-top: 20px;
	}

	/* ---- S3.4 The banner wall ---- */

	.wall-note {
		font-size: 14px;
		color: var(--color-muted);
	}

	.wall {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
		gap: 4px 32px;
	}

	.wall-row {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 10px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.wall-name {
		flex: 1;
		min-width: 0;
		font-weight: 600;
	}

	.pennants {
		display: inline-flex;
		gap: 3px;
	}

	.wall-count {
		width: 18px;
		flex: none;
		text-align: right;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 18px;
	}

	/* ---- S3.5 and S3.6, side by side ---- */

	.pair {
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: stretch;
	}

	/* The board's cards are content-box: 360px inside 24px of padding and a
	   1px border. */
	.pair :global(.half) {
		flex: 1 1 410px;
		min-width: 0;
	}

	.record {
		padding: 14px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.record-label {
		font-size: 13px;
		font-weight: 600;
		color: var(--color-muted);
	}

	.record-line {
		margin-top: 4px;
		display: flex;
		align-items: baseline;
		gap: 12px;
	}

	.record-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 32px;
		line-height: 1;
	}

	.record-who {
		font-size: 15px;
	}

	.pair :global(.rivalry) {
		display: flex;
		flex-direction: column;
	}

	.rivalry-kicker {
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-pennant-dark);
	}

	.rivalry-loading {
		margin-top: 18px;
	}

	.faceoff {
		margin-top: 18px;
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 16px;
	}

	.side {
		flex: 1;
		min-width: 0;
		display: flex;
		flex-direction: column;
		align-items: center;
		gap: 8px;
		text-align: center;
	}

	.side-name {
		font-weight: 700;
	}

	.score {
		text-align: center;
	}

	.score-num {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 56px;
		line-height: 1;
	}

	.score-note {
		margin-top: 6px;
		font-size: 14px;
		color: var(--color-ink-brown);
	}

	.series-bar {
		margin-top: 22px;
		height: 10px;
		border-radius: 5px;
		background: var(--color-card);
		display: flex;
		overflow: hidden;
	}

	.seg.left {
		background: var(--color-pennant);
	}

	.seg.right {
		background: var(--color-clubhouse);
	}

	.rivalry-line {
		margin: 16px 0 0;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	.rivalry-link {
		margin-top: auto;
		padding-top: 12px;
	}

	/* ---- S3.7 Latest trades ---- */

	.trade {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px 20px;
		padding: 14px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	.trade-when {
		width: 120px;
		font-size: 13px;
		font-weight: 600;
		color: var(--color-muted);
	}

	.trade-side {
		flex: 1 1 260px;
		min-width: 0;
	}

	/* ---- Loading, failed and empty ---- */

	.bars {
		display: flex;
		flex-direction: column;
		gap: 12px;
		padding: 10px 0;
	}

	.bar {
		display: block;
		border-radius: var(--radius-tag);
		background: var(--color-line-soft);
	}

	.bar.pennant {
		background: var(--color-pennant-line);
	}

	.state {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 12px 16px;
		padding: 10px 0;
	}

	.state p,
	.empty {
		margin: 0;
		font-size: 15px;
		color: var(--color-muted);
	}

	.empty {
		padding: 10px 0;
	}

	/* ---- S3.8 Phones ---- */

	@media (max-width: 639.98px) {
		.band-row {
			padding: 32px 16px 56px;
		}

		h1 {
			font-size: 56px;
		}

		/* One link per row. Equal rows keep the filled link as tall as the
		   outline one, whose border sits outside its 48px, as the desktop row
		   does by stretching. */
		.hero-links {
			display: grid;
			grid-auto-rows: 1fr;
		}

		.main {
			padding: 0 16px 64px;
		}

		.score-num {
			font-size: 44px;
		}

		.trade-when {
			width: 100%;
		}
	}
</style>

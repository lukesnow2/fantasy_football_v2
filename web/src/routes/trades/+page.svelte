<script lang="ts">
	import { onMount } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import {
		Avatar,
		Button,
		Card,
		OddsBar,
		Pennant,
		Select,
		StatStrip,
		StatTile,
		Tag
	} from '$lib/components/ui';
	import { profileHref } from '$lib/home';
	import {
		championSide,
		gotBy,
		inProgress,
		lopsidedRow,
		lopsidedTrades,
		managerOf,
		margin,
		oneDecimal,
		perSeason,
		recentTrades,
		tradeScoreLine,
		type Side,
		type TradeRow
	} from '$lib/trade-center';

	interface TradeResponse {
		trades: TradeRow[];
		analytics: {
			overview?: {
				totalTrades?: number | string;
				championshipImpactTrades?: number | string;
				avgProductionImpact?: number | string;
			};
			bestTrades?: TradeRow[];
			championshipTrades?: TradeRow[];
		};
		meta: { totalReturned: number; availableSeasons: number[] };
	}

	let tradeData = $state<TradeResponse | null>(null);
	let loadingTrades = $state(true);
	let failed = $state(false);
	let selectedSeason = $state('all');
	let expanded = $state(false);

	onMount(() => {
		loadTradeData();
	});

	// Load trade data
	async function loadTradeData(season = 'all') {
		loadingTrades = true;
		failed = false;
		try {
			const response = await fetch(`/api/trades?analysis=all&season=${season}`);
			if (response.ok) {
				tradeData = await response.json();
			} else {
				failed = true;
				console.error('Failed to fetch trade data:', response.status, response.statusText);
			}
		} catch (err) {
			failed = true;
			console.error('Error fetching trade data:', err);
		} finally {
			loadingTrades = false;
		}
	}

	// Handle season filter change
	function handleSeasonChange() {
		expanded = false;
		loadTradeData(selectedSeason);
	}

	// The phone layout (below 640px) makes each avatar and name block the link,
	// which is markup, not spacing. The data loads in the browser, so the body
	// never renders on the server.
	const phoneQuery = new MediaQuery('max-width: 639.98px', false);
	const phone = $derived(phoneQuery.current);

	const seasons = $derived(tradeData?.meta?.availableSeasons ?? []);
	const firstSeason = $derived(seasons.length ? Math.min(...seasons) : null);
	const oneSeason = $derived(selectedSeason !== 'all');
	const overview = $derived(tradeData?.analytics?.overview);
	const total = $derived(Number(overview?.totalTrades ?? 0) || 0);

	const seasonOptions = $derived([
		{ value: 'all', label: 'All seasons' },
		...seasons.map((s) => ({ value: String(s), label: String(s) }))
	]);

	// ---- S8.1 the numbers ----------------------------------------------------
	const stats = $derived(
		[
			{ label: oneSeason ? `Trades in ${selectedSeason}` : 'Trades, all time', value: String(total) },
			oneSeason ? null : { label: 'Per season', value: perSeason(total, seasons.length) },
			{
				label: 'Swung a title',
				value: String(Number(overview?.championshipImpactTrades ?? 0) || 0)
			},
			{
				label: 'Average impact',
				value: oneDecimal(overview?.avgProductionImpact),
				metricId: 'production_differential'
			}
		].filter((s) => s != null)
	);

	// ---- S8.2 Most lopsided trades -------------------------------------------
	const lopsided = $derived(lopsidedTrades(tradeData?.analytics?.bestTrades ?? []));
	const lopsidedRows = $derived(lopsided.map(lopsidedRow));

	// ---- S8.3 Trades that won a title, and recent trades ---------------------
	const titleTrades = $derived(
		(tradeData?.analytics?.championshipTrades ?? []).map((t) => {
			const side = championSide(t);
			return {
				champion: managerOf(t, side),
				year: t.seasonYear,
				got: gotBy(t, side),
				margin: margin(t.productionDifferential)
			};
		})
	);
	const allTrades = $derived(tradeData?.trades ?? []);
	const totalReturned = $derived(tradeData?.meta?.totalReturned ?? allTrades.length);
	const shownTrades = $derived(recentTrades(allTrades, expanded));
	const SIDES: Side[] = ['A', 'B'];
</script>

<svelte:head>
	<title>Trade Center · The League</title>
</svelte:head>

<PageHeader
	kicker="Activity · every season"
	title="Trade Center"
	introWidth={640}
	phoneActionGap={14}
	phoneActionFull
	pending={loadingTrades && !tradeData}
>
	{#if tradeData && firstSeason != null}
		{#if oneSeason}
			All {total} trades in {selectedSeason}, each scored on how the players did after the deal.
		{:else}
			All {total} trades since {firstSeason}, each scored on how the players did after the deal.
		{/if}
	{/if}
	{#snippet action()}
		{#if tradeData}
			<Select
				label="Season"
				options={seasonOptions}
				bind:value={selectedSeason}
				onchange={handleSeasonChange}
			/>
		{/if}
	{/snippet}
</PageHeader>

<!-- A manager's name, linked to their profile. -->
{#snippet name(manager: string)}<a class="name-link" href={profileHref(manager)}>{manager}</a>{/snippet}

{#if loadingTrades}
	<div class="first">
		<Card title="Loading trades" />
	</div>
{:else if failed || !tradeData}
	<div class="first">
		<Card title="Trades didn't load">
			<p class="muted-line">Try again in a minute.</p>
			<Button variant="secondary" class="retry" onclick={() => loadTradeData(selectedSeason)}>
				Try again
			</Button>
		</Card>
	</div>
{:else}
	<!-- S8.1 The numbers -->
	<div class="strip">
		<StatStrip label="Trades in numbers" twoUpOnPhones>
			{#each stats as s (s.label)}
				<StatTile label={s.label} value={s.value} metricId={s.metricId} compact={phone} tight />
			{/each}
		</StatStrip>
	</div>

	<!-- S8.2 Most lopsided trades -->
	{#if lopsidedRows.length > 0}
		<Card
			title="Most lopsided trades"
			class="card-gap"
			headGap={0}
			headSpacing="8px 16px"
			phoneHeadStack
		>
			{#snippet action()}
				<span class="head-note">Ranked by the points gap between the two sides.</span>
			{/snippet}
			{#each lopsidedRows as t, i (i)}
				<div class="lopsided">
					<div class="swing">
						<span class="swing-value {t.margin.tone}">{t.margin.text}</span>
						<span class="swing-label">{t.margin.label}</span>
					</div>
					<div class="sides">
						{#each [{ who: t.winner, got: t.winnerGot, won: true }, { who: t.loser, got: t.loserGot, won: false }] as side (side.who + side.won)}
							<svelte:element
								this={phone ? 'a' : 'div'}
								class="side"
								class:loser={!side.won}
								href={phone ? profileHref(side.who) : undefined}
							>
								<Avatar name={side.who} size={36} alt="" initialTone="field" initialSize={16} />
								<span class="side-text">
									<span class="side-name"
										>{#if phone}{side.who}{:else}{@render name(side.who)}{/if}
										{#if side.won}<span class="won">WON</span>{/if}</span
									>
									<span class="got">got {side.got}</span>
								</span>
							</svelte:element>
						{/each}
					</div>
					<div class="score">
						<div class="scores">
							<span>{t.winnerScore}</span><span class="loser-score">{t.loserScore}</span>
						</div>
						<div class="bar">
							<OddsBar
								percent={t.share}
								track="pennant"
								bare
								label="{t.winner} {t.winnerScore}, {t.loser} {t.loserScore}"
							/>
						</div>
						<div class="when">{t.when}</div>
					</div>
				</div>
			{/each}
		</Card>
	{/if}

	<!-- S8.3 Trades that won a title, and recent trades -->
	<div class="bottom">
		{#if titleTrades.length > 0}
			<Card title="Trades that won a title" tone="pennant" class="title-card" headGap={0}>
				<p class="title-sub">50+ point swings that went to that year's champion.</p>
				{#each titleTrades as c, i (i)}
					<div class="title-row">
						<Pennant width={14} height={20} />
						<svelte:element
							this={phone ? 'a' : 'div'}
							class="title-text"
							href={phone ? profileHref(c.champion) : undefined}
						>
							<span class="title-who"
								>{#if phone}{c.champion}{:else}{@render name(c.champion)}{/if} · {c.year}</span
							>
							<span class="got">got {c.got}</span>
						</svelte:element>
						<span class="title-margin">{c.margin}</span>
					</div>
				{/each}
			</Card>
		{/if}

		<Card title="Recent trades" class="recent-card" headGap={0} headSpacing="12px">
			{#snippet action()}<span class="total">{totalReturned} total</span>{/snippet}
			{#each shownTrades as trade, i (i)}
				<div class="recent">
					<div class="recent-top">
						<span>Week {trade.transactionWeek}, {trade.seasonYear} · {trade.tradeType}</span>
						{#if inProgress(trade)}
							<Tag variant="pending" size="trade">In progress</Tag>
						{:else}
							<Tag variant="final" size="trade">Final</Tag>
						{/if}
					</div>
					<div class="recent-sides">
						{#each SIDES as side (side)}
							{@const who = managerOf(trade, side)}
							{@const line = tradeScoreLine(trade, side)}
							<svelte:element
								this={phone ? 'a' : 'div'}
								class="recent-side"
								href={phone ? profileHref(who) : undefined}
							>
								<b>{#if phone}{who}{:else}{@render name(who)}{/if}</b>
								<span class="got-inline">got {gotBy(trade, side)}</span>
								<span class="score-line {line.tone}">{line.text}</span>
							</svelte:element>
						{/each}
					</div>
				</div>
			{/each}
			{#if allTrades.length > 3}
				<Button
					variant="link"
					size="tap"
					class="all-trades"
					aria-expanded={expanded}
					onclick={() => (expanded = !expanded)}
				>
					{expanded ? 'Show the latest three' : `All ${totalReturned} trades →`}
				</Button>
			{/if}
		</Card>
	</div>
{/if}

<style>
	.first,
	.strip {
		margin-top: 28px;
	}

	/* The board draws no ⓘ in the strip; G8 adds one to Average impact. Its
	   glyph comes from a fallback font with a taller line, so hold it to the
	   label's line and the strip keeps the board's height. */
	.strip :global(.i) {
		line-height: 1;
	}

	.muted-line {
		margin: 0 0 16px;
		color: var(--color-muted);
	}

	.name-link:hover {
		color: var(--color-field);
	}

	:global(.card-gap) {
		margin-top: 20px;
	}

	/* ---- S8.2 Most lopsided trades ---- */
	.head-note {
		font-size: 13px;
		color: var(--color-muted);
	}

	.lopsided {
		display: flex;
		flex-wrap: wrap;
		gap: 14px 28px;
		align-items: center;
		padding: 18px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.swing {
		display: flex;
		flex-direction: column;
		width: 110px;
	}

	.swing-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 30px;
		line-height: 1;
	}

	.swing-value.field {
		color: var(--color-field);
	}

	.swing-value.pennant {
		color: var(--color-pennant);
	}

	.swing-label {
		margin-top: 4px;
		font-size: 12px;
		color: var(--color-muted);
	}

	.sides {
		flex: 1 1 520px;
		min-width: 0;
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 16px;
	}

	.side {
		display: flex;
		gap: 10px;
		align-items: flex-start;
	}

	.side.loser :global(.avatar) {
		opacity: 0.85;
	}

	.side-text {
		display: flex;
		flex-direction: column;
		min-width: 0;
	}

	.side-name {
		font-weight: 700;
	}

	.won {
		font-size: 12px;
		font-weight: 700;
		color: var(--color-field);
	}

	.got {
		font-size: 14px;
		color: var(--color-ink-2);
	}

	.score {
		width: 180px;
	}

	.scores {
		display: flex;
		justify-content: space-between;
		font-size: 13px;
		font-weight: 600;
	}

	.loser-score {
		color: var(--color-muted);
	}

	.bar {
		margin-top: 5px;
	}

	.when {
		margin-top: 5px;
		font-size: 12px;
		color: var(--color-muted);
	}

	/* ---- S8.3 the bottom row ---- */
	.bottom {
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		margin-top: 20px;
		align-items: flex-start;
	}

	/* The boards are content-box: the flex basis is the content's width. */
	.bottom :global(.title-card) {
		box-sizing: content-box;
		flex: 1 1 360px;
	}

	.bottom :global(.recent-card) {
		box-sizing: content-box;
		flex: 2 1 520px;
	}

	.title-sub {
		margin: 6px 0 8px;
		font-size: 14px;
		color: var(--color-ink-brown);
	}

	.title-row {
		display: flex;
		gap: 14px;
		align-items: center;
		padding: 12px 0;
		border-bottom: 1px solid var(--color-pennant-line);
	}

	.title-row :global(.pennant) {
		flex: none;
	}

	.title-text {
		display: flex;
		flex-direction: column;
		flex: 1;
		min-width: 0;
	}

	.title-who {
		font-weight: 700;
	}

	.title-margin {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 22px;
	}

	.total {
		font-size: 13px;
		color: var(--color-muted);
	}

	.recent {
		padding: 14px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.recent-top {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		gap: 8px;
		font-size: 13px;
		color: var(--color-muted);
	}

	.recent-sides {
		margin-top: 8px;
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 16px;
		font-size: 15px;
	}

	.got-inline {
		color: var(--color-ink-2);
	}

	.score-line {
		display: block;
		margin-top: 2px;
		font-size: 13px;
		font-weight: 700;
	}

	.score-line.field {
		color: var(--color-field);
	}

	.score-line.muted {
		color: var(--color-muted);
	}

	/* ---- S8.10 phones ---- */
	@media (max-width: 639.98px) {
		.first,
		.strip {
			margin-top: 14px;
		}

		:global(.card-gap) {
			margin-top: 14px;
		}

		.lopsided {
			flex-direction: column;
			flex-wrap: nowrap;
			align-items: stretch;
			gap: 12px;
			padding: 14px 0;
		}

		.swing {
			flex-direction: row;
			align-items: baseline;
			gap: 8px;
			width: auto;
		}

		.swing-value {
			font-size: 26px;
		}

		.swing-label {
			margin-top: 0;
		}

		/* Winner and loser stack as their own rows, each a 44px link. */
		.sides {
			display: contents;
		}

		.side {
			min-height: 44px;
		}

		.side-name {
			font-size: 15px;
		}

		.score {
			width: auto;
		}

		.bottom {
			flex-direction: column;
			flex-wrap: nowrap;
			align-items: stretch;
			gap: 14px;
			margin-top: 14px;
		}

		.bottom :global(.title-card),
		.bottom :global(.recent-card) {
			box-sizing: border-box;
			flex: none;
		}

		.title-row {
			gap: 12px;
		}

		.title-text {
			min-height: 44px;
			justify-content: center;
		}

		.title-who {
			font-size: 15px;
		}

		.title-margin {
			flex: none;
		}

		.recent-top {
			flex-wrap: nowrap;
			align-items: center;
		}

		.recent-top :global(.tag) {
			flex: none;
		}

		.recent-sides {
			display: flex;
			flex-direction: column;
			gap: 10px;
		}

		.recent-side {
			display: block;
			min-height: 44px;
		}

		.bottom :global(.all-trades) {
			display: flex;
			justify-content: flex-start;
			width: 100%;
			margin-top: 4px;
		}
	}
</style>

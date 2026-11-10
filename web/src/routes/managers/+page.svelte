<script lang="ts">
	import { onMount } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import { page } from '$app/state';
	import { replaceState } from '$app/navigation';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import ManagerLuckCard from '$lib/components/ManagerLuckCard.svelte';
	import { Avatar, Button, Card, EmptyValue, StatTip, Tag, TitleFlags } from '$lib/components/ui';
	import {
		badgeTone,
		draftRange,
		formatOnePlace,
		formatPct3,
		formatPercent1,
		formatWhole,
		fullRecord,
		heroLine,
		profileHref,
		railOrder,
		rankMedal,
		rankMedals,
		rankPhrase,
		rosterCounts,
		sentenceCase,
		type DraftCareerGrade,
		type DraftSeasonScore
	} from '$lib/utils/managers';

	let { data } = $props();

	let managerData: any = $state(null);
	let loading = $state(true);
	let failed = $state(false);
	let selectedName = $state('');

	// Real draft figures for Career range (D7): optional, so a failure leaves
	// those rows at "None" rather than taking the page down.
	let draftBySeason = $state<DraftSeasonScore[]>([]);
	let draftByManager = $state<DraftCareerGrade[]>([]);
	let draftLoaded = $state(false);

	const phoneQuery = new MediaQuery('max-width: 639.98px', false);
	const phone = $derived(phoneQuery.current);

	async function loadManagerData() {
		loading = true;
		failed = false;
		try {
			const response = await fetch('/api/managers/performance?analysis=all');
			if (!response.ok) {
				console.error('Failed to fetch manager data:', response.status, response.statusText);
				failed = true;
				return;
			}
			managerData = await response.json();
			selectedName = initialSelection();
		} catch (err) {
			console.error('Error fetching manager data:', err);
			failed = true;
		} finally {
			loading = false;
		}
	}

	async function loadDraft() {
		try {
			const response = await fetch('/api/draft?analysis=all');
			if (!response.ok) return;
			const body = await response.json();
			const grades = body?.data?.draftGrades ?? body?.draftGrades;
			draftBySeason = grades?.bySeason ?? [];
			draftByManager = grades?.byManager ?? [];
		} catch (err) {
			console.error('Error fetching draft grades:', err);
		} finally {
			draftLoaded = true;
		}
	}

	onMount(() => {
		loadManagerData();
		loadDraft();
	});

	const rankings = $derived(
		((managerData?.data?.rankings ?? []) as any[]).filter((m) => m.managerName != null)
	);
	const rail = $derived(railOrder(rankings).map((e) => ({ ...e.manager, alum: e.alum })));
	const counts = $derived(rosterCounts(rankings));

	/** `?manager=` when it names a manager, else the signed-in user's, else the rail's first. */
	function initialSelection(): string {
		const all = ((managerData?.data?.rankings ?? []) as any[]).filter((m) => m.managerName);
		const has = (name: string | null | undefined) =>
			!!name && all.some((m) => m.managerName === name);
		const asked = page.url.searchParams.get('manager');
		if (has(asked)) return asked as string;
		if (has(data.userManagerName)) return data.userManagerName as string;
		return railOrder(all)[0]?.manager.managerName ?? all[0]?.managerName ?? '';
	}

	const current = $derived(rankings.find((m) => m.managerName === selectedName) ?? null);
	const inRail = $derived(rail.some((m) => m.managerName === current?.managerName));
	const achievements = $derived(
		(managerData?.data?.achievements ?? []).find((a: any) => a.managerName === current?.managerName)
			?.allAchievements ?? ([] as string[])
	);

	/** Select in place: no reload and no history entry (S6.1). */
	function select(name: string) {
		selectedName = name;
		replaceState(`/managers?manager=${encodeURIComponent(name)}`, {});
	}

	function onRailClick(event: MouseEvent, name: string) {
		if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
			return;
		}
		event.preventDefault();
		select(name);
	}

	const ranks = $derived(
		current
			? rankMedals([
					{ label: 'Championships', rank: current.championshipRank },
					{ label: 'Win rate', rank: current.winPctRank },
					{ label: 'Scoring', rank: current.scoringRank },
					{ label: 'Playoff win rate', rank: current.playoffRank }
				])
			: []
	);

	const draft = $derived(
		current ? draftRange(draftBySeason, draftByManager, current.managerName) : null
	);

	const record = $derived(
		current
			? fullRecord(current.totalWins ?? 0, current.totalLosses ?? 0, current.totalTies ?? 0)
			: ''
	);

	type Stat = { label: string; value: string | null; tone: 'ink' | 'field' | 'pennant' };

	const tenure = $derived<Stat[]>(
		current
			? [
					{
						label: 'Seasons',
						value: current.totalSeasons != null ? String(current.totalSeasons) : null,
						tone: 'ink'
					},
					{
						label: 'Playoff trips',
						value: current.playoffAppearances != null ? String(current.playoffAppearances) : null,
						tone: 'field'
					},
					{
						label: 'Championships',
						value: current.totalChampionships != null ? String(current.totalChampionships) : null,
						tone: 'pennant'
					},
					{
						label: 'Playoff win %',
						value:
							current.playoffWinPercentage != null
								? formatPercent1(current.playoffWinPercentage)
								: null,
						tone: 'ink'
					}
				]
			: []
	);

	const scoring = $derived<Stat[]>(
		current
			? [
					{
						label: 'Points per game',
						value: current.avgPointsPerGame != null ? current.avgPointsPerGame.toFixed(1) : null,
						tone: 'field'
					},
					{
						label: 'Points per season',
						value: current.avgPointsFor != null ? formatOnePlace(current.avgPointsFor) : null,
						tone: 'ink'
					},
					{
						label: 'Career points',
						value:
							current.totalPointsScored != null ? formatWhole(current.totalPointsScored) : null,
						tone: 'ink'
					}
				]
			: []
	);
</script>

<svelte:head>
	<title>Managers - The League</title>
</svelte:head>

{#snippet value(text: string | null | undefined)}
	{#if text == null || text === ''}<EmptyValue />{:else}{text}{/if}
{/snippet}

{#snippet statGrid(title: string, stats: Stat[])}
	<Card {title} level={3} titleSize={24} phoneTitleSize={18} headGap={12}>
		<div class="stat-grid">
			{#each stats as stat (stat.label)}
				<div>
					<div class="stat-value" style:color="var(--color-{stat.tone})">
						{@render value(stat.value)}
					</div>
					<div class="stat-label">{stat.label}</div>
				</div>
			{/each}
		</div>
	</Card>
{/snippet}

<PageHeader
	kicker={managerData
		? `The roster · ${counts.active} active, ${counts.allTime} all-time`
		: undefined}
	title="Managers"
	introWidth={640}
	pending={loading && !managerData}
>
	Career records, championships and season-by-season history for every manager in the league.
</PageHeader>

{#if loading && !managerData}
	<div class="layout" aria-busy="true" aria-label="Loading managers">
		<div class="rail skeleton-rail">
			{#each { length: 10 }, i (i)}<span class="sk-row"></span>{/each}
		</div>
		<div class="content">
			<span class="sk-hero"></span>
			<span class="sk-card"></span>
		</div>
	</div>
{:else if failed}
	<div class="state">
		<Card title="Managers didn't load">
			<p class="muted-line">Check your connection, then try again.</p>
			<Button variant="secondary" onclick={loadManagerData}>Try again</Button>
		</Card>
	</div>
{:else if !current}
	<div class="state">
		<Card title="No managers yet">
			<p class="muted-line">Managers show up here once a season is loaded.</p>
		</Card>
	</div>
{:else}
	<div class="layout">
		{#if phone}
			<label class="picker">
				Manager
				<select
					value={current.managerName}
					onchange={(event) => select((event.currentTarget as HTMLSelectElement).value)}
				>
					{#each rail as m (m.managerName)}
						<option value={m.managerName}>{m.managerName}</option>
					{/each}
					{#if !inRail}
						<option value={current.managerName}>{current.managerName}</option>
					{/if}
				</select>
			</label>
		{:else}
			<nav class="rail" aria-label="Pick a manager">
				{#each rail as m (m.managerName)}
					{@const on = m.managerName === current.managerName}
					<a
						href="/managers?manager={encodeURIComponent(m.managerName)}"
						class="rail-item"
						class:on
						aria-current={on ? 'page' : undefined}
						onclick={(event) => onRailClick(event, m.managerName)}
					>
						<Avatar
							name={m.managerName}
							size={40}
							alt=""
							ring={on ? 2 : 0}
							ringStyle="shadow"
							ringColor="var(--color-pennant-light)"
						/>
						<span class="rail-text">
							<span class="rail-name">{m.managerName}</span>
							<span class="rail-flags">
								<TitleFlags
									count={m.totalChampionships ?? 0}
									width={9}
									height={13}
									stroke={1.8}
									gap={2}
									pole={on ? 'white' : 'ink'}
									flag={on ? 'pennant-light' : 'pennant'}
								/>
								{#if m.alum}<span class="alum">League alum</span>{/if}
							</span>
						</span>
					</a>
				{/each}
			</nav>
		{/if}

		<div class="content">
			<section class="hero" aria-label={current.managerName}>
				<Avatar
					name={current.managerName}
					size={phone ? 84 : 120}
					ring={phone ? 3 : 4}
					ringColor="var(--color-pennant-light)"
					alt={current.managerName}
				/>
				<div class="hero-main">
					{#if !phone && current.tierClassification}
						<Tag variant="band-champion" caps>{sentenceCase(current.tierClassification)}</Tag>
					{/if}
					<h2 class="hero-name">{current.managerName}</h2>
					{#if phone}
						<div class="hero-line">
							{heroLine(record, current.winPercentage ?? 0, current.totalChampionships ?? 0)}
						</div>
					{:else}
						<div class="hero-meta">
							<span>{current.firstSeason} to {current.lastSeason}</span>
							<TitleFlags
								count={current.totalChampionships ?? 0}
								width={14}
								height={20}
								pole="white"
								flag="pennant-light"
								gap={4}
							/>
						</div>
					{/if}
				</div>
				{#if !phone}
					<div class="hero-record">
						<div class="record">{record}</div>
						<div class="rate">
							<b>{formatPct3(current.winPercentage ?? 0)}</b> win rate{#if current.winPctRank != null},
								{rankPhrase(current.winPctRank)}{/if}
						</div>
					</div>
				{/if}
				<nav class="hero-nav" aria-label="{current.managerName}'s full profile">
					<Button variant="band-white" class="open-profile" href={profileHref(current.managerName)}
						>Open full profile →</Button
					>
					<span class="jump">Or jump to</span>
					<span class="jump-links">
						<a href={profileHref(current.managerName, 'seasons')}><span>Season history</span></a>
						<a href={profileHref(current.managerName, 'head-to-head')}><span>Head to head</span></a>
						<a href={profileHref(current.managerName, 'achievements')}><span>Achievements</span></a>
					</span>
				</nav>
			</section>

			{#if phone}
				<div class="ranks-phone">
					{#each ranks as r (r.label)}
						{@const m = rankMedal(r.rank ?? 99)}
						<div class="rank-tile-phone" style:background="var(--color-{m.tile})">
							<div class="rank-number">#{r.rank}</div>
							<div class="rank-label-phone">{r.label}</div>
						</div>
					{/each}
				</div>
			{:else}
				<Card title="League rankings" level={3} titleSize={24} headGap={14}>
					<div class="ranks">
						{#each ranks as r (r.label)}
							{@const m = rankMedal(r.rank ?? 99)}
							<div class="rank-tile" style:background="var(--color-{m.tile})">
								<span
									class="medal"
									style:background="var(--color-{m.medal})"
									style:color="var(--color-{m.text})">#{r.rank}</span
								>
								<span class="rank-label">{r.label}</span>
							</div>
						{/each}
					</div>
				</Card>
			{/if}

			<div class="card-row">
				{@render statGrid('Tenure and impact', tenure)}
				{@render statGrid('Scoring', scoring)}
			</div>

			<div class="card-row">
				<Card title="Career range" level={3} titleSize={24} phoneTitleSize={18} headGap={6}>
					<div class="line">
						<span class="line-label">Best season</span>
						<span class="line-value field">{@render value(current.bestSeasonRecord)}</span>
					</div>
					<div class="line">
						<span class="line-label">Worst season</span>
						<span class="line-value pennant">{@render value(current.worstSeasonRecord)}</span>
					</div>
					<div class="line">
						<span class="line-label">Best draft</span>
						<span class="line-value field"
							>{#if draftLoaded}{@render value(
									draft?.bestYear != null ? String(draft.bestYear) : null
								)}{/if}</span
						>
					</div>
					<div class="line">
						<span class="line-label">Worst draft</span>
						<span class="line-value pennant"
							>{#if draftLoaded}{@render value(
									draft?.worstYear != null ? String(draft.worstYear) : null
								)}{/if}</span
						>
					</div>
				</Card>

				<Card title="Waiver wire and form" level={3} titleSize={24} phoneTitleSize={18} headGap={6}>
					<div class="line tap-row">
						<span class="line-label"
							><StatTip
								metricId="faab_efficiency_rating"
								label="FAAB efficiency"
								phoneTap="inline"
							/></span
						>
						<span class="line-value"
							>{@render value(
								current.faabEfficiencyRating != null
									? `${current.faabEfficiencyRating.toFixed(2)} pts/$`
									: null
							)}</span
						>
					</div>
					<div class="line tap-row">
						<span class="line-label"
							><StatTip metricId="transactions" label="Moves per season" phoneTap="inline" /></span
						>
						<span class="line-value"
							>{@render value(
								current.avgTransactionsPerSeason != null
									? current.avgTransactionsPerSeason.toFixed(1)
									: null
							)}</span
						>
					</div>
					<div class="line tap-row">
						<span class="line-label"
							><StatTip metricId="transactions" label="Total moves" phoneTap="inline" /></span
						>
						<span class="line-value"
							>{@render value(
								current.totalTransactions != null ? formatWhole(current.totalTransactions) : null
							)}</span
						>
					</div>
					<div class="line tap-row">
						<span class="line-label"
							><StatTip
								metricId="season_consistency_score"
								label="Year-to-year swing"
								phoneTap="inline"
							/></span
						>
						<span class="line-value"
							>{@render value(
								current.seasonConsistencyScore != null
									? `±${formatPct3(current.seasonConsistencyScore)}`
									: null
							)}</span
						>
					</div>
					<div class="line tap-row">
						<span class="line-label"
							><StatTip metricId="draft_value_score" label="Draft grade" phoneTap="inline" /></span
						>
						<span class="line-value"
							>{#if draftLoaded}{@render value(draft?.grade)}{/if}</span
						>
					</div>
				</Card>

				<ManagerLuckCard manager={current} variant="compact" />
			</div>

			<Card
				title="Achievements"
				level={3}
				titleSize={24}
				phoneTitleSize={18}
				headGap={12}
				headSpacing="8px"
			>
				{#snippet action()}
					<a class="see-all" href={profileHref(current.managerName, 'achievements')}>See all →</a>
				{/snippet}
				{#if achievements.length > 0}
					<div class="chips">
						{#each achievements as name (name)}
							<Tag variant={badgeTone(name)} size="chip">{sentenceCase(name)}</Tag>
						{/each}
					</div>
				{:else}
					<p class="muted-line">No achievements yet.</p>
				{/if}
			</Card>
		</div>
	</div>
{/if}

<style>
	.layout {
		margin-top: 28px;
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: flex-start;
	}

	.state {
		margin-top: 28px;
	}

	.muted-line {
		margin: 0 0 16px;
		font-size: 15px;
		color: var(--color-muted);
	}

	/* ---------- The rail (S6.1) ---------- */
	.rail {
		/* Content-box, as the board draws it: 210px plus its padding and border. */
		flex: 1 1 210px;
		min-width: 0;
		box-sizing: content-box;
		display: flex;
		flex-direction: column;
		gap: 2px;
		padding: 10px;
		background: var(--color-card);
		border: 1px solid var(--color-line);
		border-radius: var(--radius-card);
	}

	.rail-item {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 8px 10px;
		border-radius: 10px;
		color: var(--color-ink);
		text-decoration: none;
	}

	.rail-item:hover {
		background: var(--color-field-wash);
	}

	.rail-item.on {
		background: var(--color-clubhouse);
		color: var(--color-white);
	}

	.rail-item:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.rail-text {
		flex: 1;
		min-width: 0;
	}

	.rail-name {
		display: block;
		font-size: 15px;
		font-weight: 700;
	}

	.rail-flags {
		display: flex;
		align-items: center;
		gap: 6px;
		margin-top: 3px;
		min-height: 13px;
	}

	/* Former managers stay on the rail as an honor (Luke, 2026-11-10). */
	.alum {
		font-size: 11px;
		font-weight: 600;
		line-height: 13px;
		letter-spacing: 0.04em;
		text-transform: uppercase;
		color: var(--color-muted);
	}

	.rail-item.on .alum {
		color: var(--color-on-clubhouse-2);
	}

	.content {
		flex: 999 1 640px;
		min-width: 0;
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	/* ---------- The hero (S6.2) ---------- */
	.hero {
		display: flex;
		flex-wrap: wrap;
		gap: 24px;
		align-items: center;
		padding: 28px;
		border-radius: var(--radius-card);
		background: var(--color-clubhouse);
		color: var(--color-white);
	}

	.hero-main {
		flex: 999 1 280px;
		min-width: 0;
	}

	.hero-name {
		margin: 10px 0 6px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 52px;
		line-height: 1;
	}

	.hero-meta {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px 16px;
		font-size: 15px;
		color: var(--color-on-clubhouse);
	}

	.hero-record {
		flex: 1 1 200px;
		text-align: right;
	}

	.record {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 44px;
		line-height: 1;
	}

	.rate {
		margin-top: 6px;
		font-size: 15px;
		color: var(--color-on-clubhouse);
	}

	.rate b {
		font-size: 20px;
		color: var(--color-white);
	}

	.hero-nav {
		flex: 1 1 100%;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 10px 22px;
		padding-top: 18px;
		border-top: 1px solid var(--color-clubhouse-2);
	}

	.jump {
		font-size: 14px;
		color: var(--color-on-clubhouse-2);
	}

	.jump-links {
		display: contents;
	}

	.jump-links a {
		font-size: 15px;
		font-weight: 600;
		color: var(--color-white);
		text-decoration: none;
	}

	.jump-links a span {
		border-bottom: 1px solid var(--color-pennant-light);
	}

	.jump-links a:focus-visible {
		outline: 2px solid var(--color-white);
		outline-offset: 2px;
	}

	/* ---------- League rankings (S6.3) ---------- */
	.ranks {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
		gap: 12px;
	}

	.rank-tile {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 14px;
		border-radius: 10px;
	}

	.medal {
		flex: none;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 52px;
		height: 52px;
		border-radius: 50%;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 22px;
	}

	.rank-label {
		font-size: 15px;
		font-weight: 700;
	}

	/* ---------- The cards ---------- */
	.card-row {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
		gap: 20px;
	}

	.stat-grid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 16px 12px;
	}

	.stat-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 32px;
		line-height: 1;
	}

	.stat-label {
		margin-top: 4px;
		font-size: 13px;
		color: var(--color-muted);
	}

	.line {
		display: flex;
		justify-content: space-between;
		gap: 12px;
		padding: 9px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	.line-label {
		color: var(--color-ink-2);
	}

	.line-value {
		font-weight: 700;
		text-align: right;
	}

	.line-value.field {
		color: var(--color-field);
	}

	.line-value.pennant {
		color: var(--color-pennant);
	}

	.see-all {
		font-size: 14px;
		font-weight: 600;
		color: var(--color-field);
		text-decoration: none;
	}

	.see-all:hover {
		color: var(--color-clubhouse);
	}

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
	}

	/* ---------- Loading ---------- */
	.skeleton-rail {
		gap: 8px;
	}

	.sk-row,
	.sk-hero,
	.sk-card {
		display: block;
		border-radius: 10px;
		background: var(--color-line-soft);
	}

	.sk-row {
		height: 48px;
	}

	.sk-hero {
		height: 248px;
		border-radius: var(--radius-card);
	}

	.sk-card {
		height: 140px;
		border-radius: var(--radius-card);
	}

	/* ---------- Phones (S6.9) ---------- */
	@media (max-width: 639.98px) {
		.layout {
			margin-top: 14px;
			gap: 14px;
		}

		.state {
			margin-top: 20px;
		}

		.skeleton-rail {
			display: none;
		}

		.picker {
			flex: 1 1 100%;
			display: flex;
			flex-direction: column;
			gap: 6px;
			font-size: 13px;
			font-weight: 600;
		}

		.picker select {
			appearance: auto;
			min-height: 48px;
			padding: 0 12px;
			border: 1px solid var(--color-line-strong);
			border-radius: 10px;
			background: var(--color-card);
			color: var(--color-ink);
			font-family: var(--font-sans);
			font-size: 16px;
			font-weight: 600;
		}

		.content {
			flex: 1 1 100%;
			gap: 14px;
		}

		.hero {
			display: block;
			padding: 18px;
			text-align: center;
		}

		.hero-name {
			margin: 8px 0 0;
			font-size: 28px;
			line-height: normal;
		}

		.hero-line {
			margin-top: 4px;
			font-size: 14px;
			color: var(--color-on-clubhouse);
		}

		.hero-nav {
			display: block;
			margin-top: 16px;
			padding-top: 16px;
		}

		.hero-nav :global(.open-profile) {
			display: flex;
			width: 100%;
			box-sizing: border-box;
		}

		.jump {
			display: block;
			margin-top: 12px;
		}

		.jump-links {
			display: flex;
			flex-wrap: wrap;
			justify-content: center;
			gap: 0 10px;
		}

		.jump-links a {
			display: inline-flex;
			align-items: center;
			min-height: 44px;
		}

		.ranks-phone {
			margin-top: -2px;
			display: grid;
			grid-template-columns: repeat(2, minmax(0, 1fr));
			gap: 10px;
		}

		.rank-tile-phone {
			padding: 12px;
			border-radius: 10px;
			text-align: center;
		}

		.rank-number {
			font-family: var(--font-slab);
			font-weight: 700;
			font-size: 22px;
		}

		.rank-label-phone {
			font-size: 12px;
		}

		.card-row {
			gap: 14px;
		}

		.stat-value {
			font-size: 24px;
		}

		.stat-label {
			font-size: 12px;
		}

		.tap-row {
			align-items: center;
			min-height: 44px;
			padding: 0;
			box-sizing: content-box;
		}

		.see-all {
			display: inline-block;
			padding: 12px 0;
			margin: -12px 0;
			line-height: 20px;
		}
	}
</style>

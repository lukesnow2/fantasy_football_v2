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
		OddsBar,
		StatStrip,
		StatTile,
		StatTip,
		TitleFlags,
		type Column
	} from '$lib/components/ui';
	import {
		fixed,
		groupByTier,
		indexBarPct,
		managerCount,
		numberWord,
		podiumStyle,
		tierKey,
		toNumber
	} from '$lib/utils/hall-of-fame';
	import { formatPct3, formatPercent1, profileHref } from '$lib/utils/managers';
	import { formatCount, formatRecord } from '$lib/home';

	type HallOfFameRow = {
		managerName: string;
		totalSeasons: number;
		championshipsWon: number;
		careerWins: number;
		careerLosses: number;
		careerTies: number;
		careerWinPercentage: number | string;
		avgPointsPerGame: number | string;
		playoffAppearances: number;
		hallOfFameIndex: number | string;
		hallOfFameRank: number;
	};

	type TierRow = {
		tier: string;
		avgChampionships: number | string;
		avgWinPct: number | string;
		avgHallOfFameIndex: number | string;
		totalWins: number | string;
		totalLosses: number | string;
	};

	interface HallOfFameResponse {
		hallOfFame: HallOfFameRow[];
		analytics: Record<string, number | string | null>;
		tiers: TierRow[];
		seasonRange?: { first: number | null; last: number | null };
	}

	let hallOfFameData = $state<HallOfFameResponse | null>(null);
	let loading = $state(true);
	let error = $state(false);

	// Load Hall of Fame data
	async function loadHallOfFameData() {
		loading = true;
		error = false;
		try {
			const response = await fetch('/api/hall-of-fame');
			if (response.ok) {
				hallOfFameData = await response.json();
			} else {
				error = true;
				console.error('Failed to fetch Hall of Fame data:', response.status, response.statusText);
			}
		} catch (err) {
			error = true;
			console.error('Error fetching Hall of Fame data:', err);
		} finally {
			loading = false;
		}
	}

	onMount(loadHallOfFameData);

	// The phone layout (below 640px) drops table columns and moves the titles
	// under the name, which is markup, not spacing. The data only loads in the
	// browser, so the body never renders on the server.
	const phoneQuery = new MediaQuery('max-width: 639.98px', false);
	const phone = $derived(phoneQuery.current);

	const managers = $derived(hallOfFameData?.hallOfFame ?? []);
	const analytics = $derived(hallOfFameData?.analytics ?? {});
	const tiers = $derived(hallOfFameData?.tiers ?? []);
	const range = $derived(hallOfFameData?.seasonRange);

	const totalManagers = $derived(Number(analytics.totalManagers) || 0);

	/** Win % as the board writes it (".563"), or null when missing. */
	function rate(value: number | string | null | undefined): string | null {
		const n = toNumber(value);
		return n == null ? null : formatPct3(n);
	}

	/** A rate as a percent to one place ("57.5%"), or null when missing. */
	function percent(value: number | string | null | undefined): string | null {
		const n = toNumber(value);
		return n == null ? null : formatPercent1(n);
	}

	function record(wins: unknown, losses: unknown, ties: unknown = 0): string {
		return formatRecord(Number(wins) || 0, Number(losses) || 0, Number(ties) || 0);
	}

	// ---- S7.2 header and strip ----------------------------------------------
	const kicker = $derived(
		range?.first != null && range?.last != null
			? `Rankings · all time · ${range.first} to ${range.last}`
			: undefined
	);

	const stats = $derived([
		{ label: 'managers ranked', value: String(totalManagers) },
		{ label: 'titles per manager', value: fixed(analytics.avgChampionships, 1) },
		{ label: 'average win rate', value: percent(analytics.avgWinPercentage) },
		{ label: 'seasons per manager', value: fixed(analytics.avgSeasonsPlayed, 1) },
		{
			label: 'average HoF index',
			value: fixed(analytics.avgHallOfFameIndex, 3),
			metricId: 'hall_of_fame_index'
		}
	]);

	// ---- S7.3 the podium ----------------------------------------------------
	const podium = $derived(
		managers.slice(0, 3).map((m, i) => ({ m, style: podiumStyle(i + 1, phone) }))
	);

	// ---- S7.4 official rankings ---------------------------------------------
	const totalTitles = $derived(
		managers.reduce((sum, m) => sum + (Number(m.championshipsWon) || 0), 0)
	);
	const caption = $derived(
		`${managerCount(totalManagers)} · ${formatCount(analytics.totalCareerWins)} wins · ${totalTitles} ${totalTitles === 1 ? 'title' : 'titles'}`
	);

	const topIndex = $derived(
		managers.reduce((top, m) => Math.max(top, Number(m.hallOfFameIndex) || 0), 0)
	);

	const groups = $derived(groupByTier(managers));
	const tierStats = $derived(new Map(tiers.map((t) => [t.tier, t])));

	/** Every row, with the tier it opens when it's its group's first. */
	const tableRows = $derived(
		groups.flatMap((g) =>
			g.rows.map((m, i) => ({
				...m,
				opens: i === 0 ? { name: g.name, color: g.color, count: g.rows.length } : null
			}))
		)
	);

	const desktopColumns: Column[] = [
		{ key: 'rank', label: '#', width: '32px' },
		{ key: 'manager', label: 'Manager' },
		{ key: 'titles', label: 'Titles', width: '120px' },
		{ key: 'record', label: 'Record', numeric: true, width: '92px' },
		{ key: 'pct', label: 'Win %', numeric: true, width: '56px' },
		{ key: 'ppg', label: 'Pts/G', numeric: true, width: '56px' },
		{ key: 'playoffs', label: 'Playoffs', numeric: true, width: '64px', fixedHeader: true },
		{ key: 'seasons', label: 'Seasons', numeric: true, width: '60px', fixedHeader: true },
		{ key: 'index', label: 'HoF index', width: '160px' }
	];

	const phoneColumns: Column[] = [
		{ key: 'rank', label: '#', width: '24px' },
		{ key: 'manager', label: 'Manager' },
		{ key: 'pct', label: 'Win %', numeric: true, width: '38px' },
		{ key: 'index', label: 'HoF index', width: '86px' }
	];

	const columns = $derived(phone ? phoneColumns : desktopColumns);
</script>

<!-- A missing number reads "None", never a blank or a dash (G6). -->
{#snippet orNone(text: string | null)}{#if text == null}<EmptyValue />{:else}{text}{/if}{/snippet}

<svelte:head>
	<title>Hall of Fame · The League</title>
</svelte:head>

<PageHeader
	{kicker}
	title="Hall of Fame"
	introWidth={640}
	phoneActionGap={14}
	pending={loading && !hallOfFameData}
>
	{#if managers.length > 0}
		{numberWord(totalManagers)}
		{totalManagers === 1 ? 'manager' : 'managers'} ranked by the Hall of Fame index, which is built from
		championships and consistency.
	{/if}
	{#snippet action()}
		{#if managers.length > 0}
			<Button variant="secondary" href="#how-the-index-works">How the index works</Button>
		{/if}
	{/snippet}
</PageHeader>

{#if loading && !hallOfFameData}
	<!-- S7.6 Loading: blocks where the strip, the podium and the table sit. -->
	<div class="skeleton" role="status" aria-busy="true" aria-label="Loading the Hall of Fame">
		<span class="sk sk-strip"></span>
		<div class="sk-podium">
			<span class="sk sk-first"></span>
			<span class="sk sk-second"></span>
			<span class="sk sk-third"></span>
		</div>
		<span class="sk sk-table"></span>
	</div>
{:else if error}
	<div class="first">
		<Card title="The Hall of Fame didn't load">
			<p class="muted-line">Check your connection, then try again.</p>
			<Button variant="secondary" class="retry" onclick={loadHallOfFameData}>Try again</Button>
		</Card>
	</div>
{:else if managers.length === 0}
	<div class="first">
		<Card title="No one's ranked yet">
			<p class="muted-line">Managers join the Hall of Fame after three finished seasons.</p>
		</Card>
	</div>
{:else}
	<!-- S7.2 The league strip -->
	<div class="strip">
		<StatStrip
			label="The Hall of Fame in numbers"
			minColumn={170}
			twoUpOnPhones
			phoneRowRules="all"
		>
			{#each stats as s (s.label)}
				<StatTile small label={s.label} value={s.value} metricId={s.metricId} />
			{/each}
		</StatStrip>
	</div>

	<!-- S7.3 The podium -->
	<ol class="podium" aria-label="The top three">
		{#each podium as { m, style } (m.managerName)}
			<li>
				<a
					class="podium-card"
					href={profileHref(m.managerName)}
					style:--bg="var(--color-{style.bg})"
					style:--fg="var(--color-{style.fg})"
					style:--border="var(--color-{style.border})"
					style:--accent="var(--color-{style.accent})"
					style:--muted="var(--color-{style.muted})"
					style:--pad-y="{style.padY}px"
					style:--rank-size="{style.rankSize}px"
					style:--name-size="{style.nameSize}px"
					style:--stat-size="{style.statSize}px"
				>
					<span class="podium-rank">{m.hallOfFameRank}</span>
					<Avatar
						name={m.managerName}
						size={style.avatar}
						ring={phone ? 3 : 4}
						ringColor="var(--color-{style.ring})"
						initialTone="field"
						alt=""
					/>
					<span class="podium-name-block">
						<span class="podium-name">{m.managerName}</span>
						{#if Number(m.championshipsWon) > 0}
							<span class="podium-flags">
								<TitleFlags
									count={Number(m.championshipsWon)}
									width={14}
									height={20}
									pole={style.pole}
									flag={style.flag}
									gap={4}
								/>
							</span>
						{/if}
					</span>
					<span class="podium-stats">
						<span>
							<span class="podium-value">{@render orNone(fixed(m.hallOfFameIndex, 3))}</span>
							<span class="podium-label">HoF index</span>
						</span>
						<span>
							<span class="podium-value">{@render orNone(rate(m.careerWinPercentage))}</span>
							<span class="podium-label">Win %</span>
						</span>
						<span>
							<span class="podium-value">{Number(m.championshipsWon) || 0}</span>
							<span class="podium-label">Titles</span>
						</span>
					</span>
				</a>
			</li>
		{/each}
	</ol>

	<!-- S7.4 Official rankings -->
	<div class="rankings">
		<Card
			title="Official rankings"
			padding="22px 24px 12px"
			phonePadding="16px 16px 8px"
			phoneHeadStack
		>
			{#snippet action()}<span class="caption">{caption}</span>{/snippet}
			<DataTable
				{columns}
				rows={tableRows}
				label="Official rankings"
				rowPadding={phone ? 4 : 11}
				columnGap={phone ? 6 : 14}
				fontSize={phone ? 14 : 15}
				minWidth={phone ? undefined : '900px'}
				rowKey={(r) => r.managerName}
				divider={(r) => r.opens?.name ?? null}
			>
				{#snippet header(column)}
					{#if column.key === 'index'}
						<StatTip metricId="hall_of_fame_index" label="HoF index" phoneTap="inline-small" />
					{:else}
						{column.label}
					{/if}
				{/snippet}
				{#snippet dividerCell(name, r)}
					{@const stat = tierStats.get(name)}
					<div class="tier-row">
						<span class="tier-name" style:color="var(--color-{r.opens?.color})">{name}</span>
						<span class="tier-count">{managerCount(r.opens?.count ?? 0)}</span>
						{#if stat}
							<span class="tier-stats">
								<span>Avg titles <b>{@render orNone(fixed(stat.avgChampionships, 1))}</b></span>
								<span>Win % <b>{@render orNone(percent(stat.avgWinPct))}</b></span>
								<span>Index <b>{@render orNone(fixed(stat.avgHallOfFameIndex, 3))}</b></span>
								<span>Record <b>{record(stat.totalWins, stat.totalLosses)}</b></span>
							</span>
						{/if}
					</div>
				{/snippet}
				{#snippet cell(r, column)}
					{@const titles = Number(r.championshipsWon) || 0}
					{#if column.key === 'rank'}
						<span class="rank">{r.hallOfFameRank}</span>
					{:else if column.key === 'manager'}
						<a class="manager" href={profileHref(r.managerName)}>
							<Avatar
								name={r.managerName}
								size={phone ? 26 : 30}
								initialTone="field"
								initialSize={phone ? 12 : 13}
								alt=""
							/>
							{#if phone}
								<span class="manager-text">
									<span class="manager-name">{r.managerName}</span>
									<span class="manager-flags">
										<TitleFlags count={titles} width={9} height={13} stroke={1.8} gap={2} />
									</span>
								</span>
							{:else}
								{r.managerName}
							{/if}
						</a>
					{:else if column.key === 'titles'}
						{#if titles > 0}
							<span class="titles"
								><TitleFlags count={titles} width={10} height={16} gap={2} /></span
							>
						{:else}
							<EmptyValue />
						{/if}
					{:else if column.key === 'record'}
						<span class="strong">{record(r.careerWins, r.careerLosses, r.careerTies)}</span>
					{:else if column.key === 'pct'}
						{@render orNone(rate(r.careerWinPercentage))}
					{:else if column.key === 'ppg'}
						<span class="soft">{@render orNone(fixed(r.avgPointsPerGame, 1))}</span>
					{:else if column.key === 'playoffs'}
						<span class="soft">{r.playoffAppearances}</span>
					{:else if column.key === 'seasons'}
						<span class="soft">{r.totalSeasons}</span>
					{:else if column.key === 'index'}
						{@const index = fixed(r.hallOfFameIndex, 3)}
						{#if index == null}
							<EmptyValue />
						{:else}
							<OddsBar
								percent={indexBarPct(Number(r.hallOfFameIndex), topIndex)}
								text={index}
								numberWidth={phone ? 38 : 40}
								gap={phone ? 8 : 10}
							/>
						{/if}
					{/if}
				{/snippet}
			</DataTable>
		</Card>
	</div>

	<!-- S7.5 How the index works, and the tiers -->
	<div class="explain">
		<div class="how" id="how-the-index-works">
			<Card title="How the index works" titleSize={26} class="fill">
				<code class="formula"
					>HoF index = 60% × (titles ÷ most titles by anyone) + 40% × win rate</code
				>
				<p class="note">
					Only managers with three or more seasons are ranked. Titles are divided by the current
					league best, so every new championship rescales everyone's score a little.
				</p>
			</Card>
		</div>
		<div class="tiers">
			<Card title="Tiers" titleSize={26} headGap={6} class="fill">
				{#each tierKey() as k (k.name)}
					<div class="tier-key">
						<span class="tier-key-name" style:color="var(--color-{k.color})">{k.name}</span>
						<span class="tier-key-ranks">{k.ranks}</span>
					</div>
				{/each}
			</Card>
		</div>
	</div>
{/if}

<style>
	/* ---- S7.6 loading, error and empty ---- */
	.skeleton {
		margin-top: 28px;
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.sk {
		display: block;
		border-radius: var(--radius-card);
		background: var(--color-line-soft);
	}

	.sk-strip {
		height: 87px;
	}

	.sk-podium {
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.sk-first {
		height: 182px;
	}

	.sk-second {
		height: 130px;
	}

	.sk-third {
		height: 106px;
	}

	.sk-table {
		margin-top: 4px;
		height: 860px;
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

	/* ---- S7.2 the strip ---- */
	.strip {
		margin-top: 28px;
	}

	/* ---- S7.3 the podium ---- */
	.podium {
		margin: 20px 0 0;
		padding: 0;
		list-style: none;
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.podium-card {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 16px 24px;
		background: var(--bg);
		color: var(--fg);
		border: 1px solid var(--border);
		border-radius: var(--radius-card);
		padding: var(--pad-y) 28px;
		text-decoration: none;
	}

	.podium-card:hover,
	.podium-card:focus-visible {
		color: var(--fg);
	}

	.podium-card:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.podium-rank {
		width: 64px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: var(--rank-size);
		line-height: 1;
		color: var(--accent);
	}

	.podium-name-block {
		flex: 999 1 220px;
		min-width: 0;
	}

	.podium-name {
		display: block;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: var(--name-size);
		line-height: 1.05;
	}

	.podium-flags {
		margin-top: 10px;
		display: flex;
	}

	.podium-stats {
		flex: 1 1 320px;
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 12px;
		text-align: right;
	}

	.podium-value {
		display: block;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: var(--stat-size);
		line-height: 1;
	}

	.podium-label {
		display: block;
		margin-top: 4px;
		font-size: 12px;
		color: var(--muted);
	}

	/* ---- S7.4 official rankings ---- */
	.rankings {
		margin-top: 24px;
	}

	.caption {
		font-size: 13px;
		color: var(--color-muted);
	}

	.tier-row {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 4px 16px;
		padding: 18px 0 6px;
	}

	.tier-name {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
	}

	.tier-count {
		font-size: 13px;
		color: var(--color-muted);
	}

	.tier-stats {
		margin-left: auto;
		display: flex;
		gap: 14px;
		font-size: 12px;
		color: var(--color-ink-2);
	}

	.tier-stats b {
		font-weight: 700;
	}

	.rank {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 20px;
	}

	.manager {
		display: flex;
		align-items: center;
		gap: 10px;
		font-weight: 600;
		color: var(--color-ink);
		text-decoration: none;
	}

	.manager:hover {
		color: var(--color-field);
	}

	/* A block, so the flags center in the row like the board's grid cell
	   instead of riding the text baseline. */
	.titles {
		display: flex;
	}

	.strong {
		font-weight: 600;
	}

	.soft {
		color: var(--color-ink-2);
	}

	/* ---- S7.5 how the index works, and the tiers ---- */
	.explain {
		margin-top: 20px;
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
	}

	/* The board's bases are on content-box cards (22px 24px of padding and a
	   1px border), so each adds 50px here. */
	.how {
		flex: 2 1 470px;
		min-width: 0;
	}

	.tiers {
		flex: 1 1 330px;
		min-width: 0;
	}

	.explain :global(.fill) {
		height: 100%;
	}

	.formula {
		display: block;
		padding: 12px 14px;
		border-radius: var(--radius-control);
		background: var(--color-ground);
		font-size: 14px;
		line-height: normal;
		color: var(--color-ink);
	}

	.note {
		margin: 12px 0 0;
		font-size: 14px;
		line-height: 1.55;
		color: var(--color-ink-2);
	}

	.tier-key {
		display: flex;
		justify-content: space-between;
		padding: 8px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 14px;
	}

	.tier-key-name {
		font-weight: 600;
	}

	.tier-key-ranks {
		color: var(--color-ink-2);
	}

	/* ---- S7.7 phones ---- */
	@media (max-width: 639.98px) {
		.skeleton {
			margin-top: 20px;
			gap: 14px;
		}

		.sk-strip {
			height: 158px;
		}

		.sk-first {
			height: 200px;
		}

		.sk-second {
			height: 156px;
		}

		.sk-third {
			height: 142px;
		}

		.sk-table {
			margin-top: 0;
			height: 940px;
		}

		.first {
			margin-top: 20px;
		}

		.strip {
			margin-top: 20px;
		}

		.podium {
			margin-top: 14px;
		}

		.podium-card {
			gap: 14px 12px;
			padding: var(--pad-y) 16px;
		}

		.podium-rank {
			width: 40px;
			flex: none;
		}

		.podium-name-block {
			flex: 1 1 0;
		}

		.podium-flags {
			margin-top: 8px;
		}

		.podium-stats {
			flex: 1 1 100%;
			text-align: left;
		}

		.rankings {
			margin-top: 14px;
		}

		.tier-row {
			gap: 4px 12px;
			padding: 16px 0 6px;
		}

		.tier-stats {
			flex: 1 1 100%;
			margin-left: 0;
			display: grid;
			grid-template-columns: repeat(2, max-content);
			gap: 2px 16px;
		}

		.rank {
			font-size: 18px;
		}

		.manager {
			gap: 8px;
			min-height: 44px;
			min-width: 0;
		}

		.manager-text {
			min-width: 0;
		}

		.manager-name {
			display: block;
		}

		.manager-flags {
			display: flex;
			margin-top: 3px;
			min-height: 13px;
		}

		.explain {
			margin-top: 14px;
			flex-direction: column;
			gap: 14px;
		}

		.how,
		.tiers {
			flex: none;
		}

		.formula {
			line-height: 1.5;
		}
	}
</style>

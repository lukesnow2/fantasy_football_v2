<script lang="ts">
	import { untrack } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import { page } from '$app/state';
	import { afterNavigate, replaceState } from '$app/navigation';
	import ManagerLuckCard from '$lib/components/ManagerLuckCard.svelte';
	import {
		Avatar,
		Card,
		DataTable,
		EmptyValue,
		Pennant,
		StatStrip,
		StatTile,
		StatTip,
		TabNav,
		Tag,
		TitleFlags,
		Button,
		type Column,
		type TagVariant
	} from '$lib/components/ui';
	import { formatLuck, luckTone } from '$lib/utils/power-rankings';
	import {
		PROFILE_TABS,
		bestWorstSeason,
		careerMarks,
		formatPercent1,
		formatSigned,
		formatWhole,
		fullRecord,
		h2hCaption,
		h2hRows,
		h2hTiles,
		profileHref,
		profileKicker,
		profileTab,
		rankMedal,
		rankMedals,
		rivalryHref,
		seasonOutcome,
		seasonPickText,
		sentenceCase,
		tenureLine,
		titleGamesSummary,
		winsBars,
		winsChartLabel,
		type OutcomeTag,
		type ProfileTab,
		type TitleGame
	} from '$lib/utils/managers';

	let managerData: any = $state(null);
	let hallOfFame: any[] = $state([]);
	let loading = $state(true);
	let failed = $state(false);

	// Get manager name from URL
	const managerName = $derived(decodeURIComponent(page.params.slug ?? ''));

	let tab = $state<ProfileTab>(profileTab(page.url));

	// A real navigation (a link with ?tab=, or another manager's profile)
	// lands on the tab its URL names. Tab clicks below are shallow, so they
	// don't come through here.
	afterNavigate(() => {
		tab = profileTab(page.url);
	});

	function selectTab(id: string) {
		tab = id as ProfileTab;
		// Shallow: no reload, no refetch, no scroll and no history entry, so
		// Back leaves the profile (S6.4).
		replaceState(profileHref(managerName, tab), {});
	}

	// Only the latest request may write, so a slow response for the manager
	// the reader just left can't replace the one they opened after it.
	let latestRequest = 0;

	async function loadManagerData(name: string) {
		const request = ++latestRequest;
		loading = true;
		failed = false;
		// Another manager's profile: drop the one on screen, so its numbers
		// don't sit under the new name while this loads. Untracked, so the
		// effect below doesn't rerun when the data arrives.
		if (untrack(() => managerData?.data?.meta?.requestedManager) !== name) {
			managerData = null;
			hallOfFame = [];
		}
		try {
			const [perf, hof] = await Promise.allSettled([
				fetch(`/api/managers/performance?manager=${encodeURIComponent(name)}&analysis=all`),
				// The kicker's tier and rank. Matched by exact name below, not with
				// the API's `?manager=`, which is a substring match.
				fetch('/api/hall-of-fame')
			]);
			if (request !== latestRequest) return;
			if (perf.status === 'rejected' || !perf.value.ok) {
				console.error('Failed to fetch manager data:', perf);
				failed = true;
				return;
			}
			const body = await perf.value.json();
			let hofRows: any[] = [];
			if (hof.status === 'fulfilled' && hof.value.ok) {
				try {
					hofRows = (await hof.value.json())?.hallOfFame ?? [];
				} catch (err) {
					console.error('Error reading the Hall of Fame:', err);
				}
			}
			if (request !== latestRequest) return;
			managerData = body;
			hallOfFame = hofRows;
		} catch (err) {
			if (request !== latestRequest) return;
			console.error('Error fetching manager data:', err);
			failed = true;
		} finally {
			if (request === latestRequest) loading = false;
		}
	}

	$effect(() => {
		loadManagerData(managerName);
	});

	const phoneQuery = new MediaQuery('max-width: 639.98px', false);
	const phone = $derived(phoneQuery.current);

	// Get manager's main data
	const manager = $derived(managerData?.data?.performance?.[0] ?? null);
	const achievements = $derived<string[]>(
		managerData?.data?.achievements?.[0]?.allAchievements ?? []
	);
	const badges = $derived(managerData?.data?.achievements?.[0] ?? null);
	const seasons = $derived<any[]>(managerData?.data?.seasons ?? []);
	const championshipSeasons = $derived<number[]>(managerData?.data?.championshipSeasons ?? []);
	const streaks = $derived(managerData?.data?.streaks ?? null);
	const titleGames = $derived<TitleGame[]>(managerData?.data?.titleGames ?? []);
	const withProfile = $derived(new Set<string>(managerData?.data?.availableManagers ?? []));

	const hofRow = $derived(hallOfFame.find((r) => r.managerName === manager?.managerName) ?? null);
	const kicker = $derived(
		manager ? profileKicker(hofRow ? Number(hofRow.hallOfFameRank) : null, manager.firstSeason) : ''
	);
	// "Most in the league" needs rank 1 held alone.
	const sharesTopRank = $derived(
		manager != null &&
			hallOfFame.some(
				(r) =>
					r.managerName !== manager.managerName &&
					Number(r.championshipsWon ?? 0) === Number(manager.totalChampionships ?? 0)
			)
	);

	const titleYears = $derived([...championshipSeasons].sort((a, b) => a - b));
	const record = $derived(
		manager
			? fullRecord(manager.totalWins ?? 0, manager.totalLosses ?? 0, manager.totalTies ?? 0)
			: ''
	);

	// Fields the mart doesn't carry, derived from the per-season breakdown.
	const careerPointsFor = $derived(
		manager?.totalPointsScored
			? Number(manager.totalPointsScored)
			: seasons.reduce((s: number, x: any) => s + (Number(x.pointsFor) || 0), 0)
	);
	const careerPointsAgainst = $derived(
		seasons.reduce((s: number, x: any) => s + (Number(x.pointsAgainst) || 0), 0)
	);
	const range = $derived(bestWorstSeason(seasons, championshipSeasons));

	const headline = $derived(
		manager
			? [
					{ label: 'Championships', value: String(manager.totalChampionships ?? 0) },
					{
						label: 'Win rate',
						value: manager.winPercentage != null ? formatPercent1(manager.winPercentage) : null
					},
					{
						label: 'Points per game',
						value: manager.avgPointsPerGame != null ? manager.avgPointsPerGame.toFixed(1) : null
					},
					{ label: 'Playoff trips', value: String(manager.playoffAppearances ?? 0) }
				]
			: []
	);

	const career = $derived(
		manager
			? [
					{ label: 'Total record', value: record },
					{
						label: 'Playoff win %',
						value:
							manager.playoffWinPercentage != null
								? formatPercent1(manager.playoffWinPercentage)
								: null
					},
					{ label: 'Points for', value: formatWhole(careerPointsFor) },
					{ label: 'Points against', value: formatWhole(careerPointsAgainst) },
					{
						label: 'Point differential',
						value: formatSigned(careerPointsFor - careerPointsAgainst)
					},
					{ label: 'Best season', value: range.best ? seasonPickText(range.best) : null },
					{ label: 'Worst season', value: range.worst ? seasonPickText(range.worst) : null },
					{
						label: 'Longest win streak',
						value: streaks ? `${streaks.longestWinStreak} games` : null
					},
					{
						label: 'Longest losing streak',
						value: streaks ? `${streaks.longestLossStreak} games` : null
					},
					{
						label: 'Transactions',
						value: manager.totalTransactions != null ? formatWhole(manager.totalTransactions) : null
					}
				]
			: []
	);

	const bars = $derived(winsBars(seasons, championshipSeasons));
	const h2h = $derived(h2hRows(managerData?.data?.head_to_head ?? []));
	const tiles = $derived(h2hTiles(h2h, titleGames));
	// One record column width for the whole list, so the bars line up.
	const h2hHasTies = $derived(h2h.some((r) => r.ties > 0));
	// The league's latest season, as far as this page can see: a manager who
	// left before it is marked "League alum", as on the Managers rail (Luke,
	// 2026-11-10; the board wrote "Left after {season}").
	const leagueLatest = $derived(
		Math.max(
			Number(manager?.latestSeason ?? 0),
			Number(manager?.lastSeason ?? 0),
			...h2h.map((r) => Number(r.opponentLastSeason ?? 0))
		)
	);

	const summary = $derived(titleGamesSummary(titleGames, manager?.championshipRank, sharesTopRank));
	const ranks = $derived(
		manager
			? rankMedals([
					{ label: 'Overall', rank: manager.overallRank },
					{ label: 'Championships', rank: manager.championshipRank },
					{ label: 'Win rate', rank: manager.winPctRank },
					{ label: 'Scoring', rank: manager.scoringRank },
					{ label: 'Playoffs', rank: manager.playoffRank }
				])
			: []
	);
	const badgeRows = $derived<
		{ label: string; metricId: string; value: string | null; variant: TagVariant }[]
	>([
		{
			label: 'Championships',
			metricId: 'badge_championships',
			value: badges?.championshipAchievement ?? null,
			variant: 'band-champion'
		},
		{
			label: 'Scoring',
			metricId: 'badge_scoring',
			value: badges?.scoringAchievement ?? null,
			variant: 'bye'
		},
		{
			label: 'Consistency',
			metricId: 'badge_consistency',
			value: badges?.consistencyAchievement ?? null,
			variant: 'eliminated'
		},
		{
			label: 'Longevity',
			metricId: 'badge_longevity',
			value: badges?.longevityAchievement ?? null,
			variant: 'eliminated'
		}
	]);
	const marks = $derived(
		manager
			? careerMarks({
					streaks,
					seasons,
					championshipSeasons,
					playoffWinPercentage: manager.playoffWinPercentage,
					playoffRank: manager.playoffRank
				})
			: []
	);

	const tabItems = $derived(
		PROFILE_TABS.map((t) => ({ id: t.id, label: t.label, href: profileHref(managerName, t.id) }))
	);

	const OUTCOME_TAG: Record<OutcomeTag, TagVariant> = {
		champion: 'champion',
		playoffs: 'playoffs',
		missed: 'missed',
		'in-progress': 'in-progress'
	};

	const seasonColumns = $derived<Column[]>(
		phone
			? [
					{ key: 'season', label: 'Season', width: '54px' },
					{ key: 'record', label: 'Record', numeric: true, width: '50px' },
					{ key: 'luck', label: 'Luck', numeric: true, width: '50px' },
					{ key: 'outcome', label: 'Outcome' }
				]
			: [
					{ key: 'season', label: 'Season', width: '70px' },
					{ key: 'record', label: 'Record', numeric: true, width: '90px' },
					{ key: 'pf', label: 'PF', numeric: true, width: '90px' },
					{ key: 'pa', label: 'PA', numeric: true, width: '90px' },
					{ key: 'xw', label: 'Reg W / exp', numeric: true, width: '130px' },
					{ key: 'luck', label: 'Luck', numeric: true, width: '80px' },
					{ key: 'outcome', label: 'Outcome' }
				]
	);

	const h2hColumns = $derived<Column[]>(
		phone
			? [
					{ key: 'opponent', label: 'Opponent', width: '168px' },
					{ key: 'games', label: 'Games', numeric: true, width: '44px' },
					{ key: 'record', label: 'Record', numeric: true, width: '56px' },
					{ key: 'pct', label: 'Win %', width: '140px' },
					{ key: 'avg', label: 'Avg score', numeric: true, width: '104px' },
					{ key: 'playoffs', label: 'Playoffs', numeric: true, width: '64px' },
					{ key: 'streak', label: 'Streak', numeric: true, width: '52px' },
					{ key: 'rivalry', label: '', numeric: true, width: '76px', ariaLabel: 'Rivalry' }
				]
			: [
					{ key: 'opponent', label: 'Opponent' },
					{ key: 'games', label: 'Games', numeric: true, width: '64px' },
					{ key: 'record', label: 'Record', numeric: true, width: '84px' },
					{ key: 'pct', label: 'Win %', width: '180px' },
					{ key: 'avg', label: 'Avg score', numeric: true, width: '130px' },
					{ key: 'playoffs', label: 'Playoffs', numeric: true, width: '80px' },
					{ key: 'streak', label: 'Streak', numeric: true, width: '70px' },
					{ key: 'rivalry', label: '', numeric: true, width: '90px', ariaLabel: 'Rivalry' }
				]
	);

	const LEAN_COLOR = { up: 'field', down: 'pennant', even: 'ink-2' } as const;
</script>

<svelte:head>
	<title>{managerName} - The League</title>
</svelte:head>

{#snippet value(text: string | null | undefined)}
	{#if text == null || text === ''}<EmptyValue />{:else}{text}{/if}
{/snippet}

{#snippet opponentName(name: string)}
	{#if withProfile.has(name)}<a class="name-link" href={profileHref(name)}>{name}</a
		>{:else}{name}{/if}
{/snippet}

<div class="profile">
	<a class="back" href="/managers">← All managers</a>

	{#if loading && !manager}
		<div class="sk-header" aria-busy="true" aria-label="Loading {managerName}'s profile"></div>
	{:else if failed}
		<div class="state">
			<Card title="This profile didn't load">
				<p class="muted-line">Check your connection, then try again.</p>
				<Button variant="secondary" onclick={() => loadManagerData(managerName)}>Try again</Button>
			</Card>
		</div>
	{:else if !manager}
		<div class="state">
			<Card title="No manager by that name">
				<p class="muted-line">The link may be old, or the name may be spelled differently.</p>
				<Button variant="secondary" href="/managers">See every manager</Button>
			</Card>
		</div>
	{:else}
		<!-- S6.4 The header: the full one on Overview, the compact one elsewhere. -->
		<section
			class="header"
			class:compact={tab !== 'overview'}
			aria-label="{manager.managerName}'s record"
		>
			<Avatar
				name={manager.managerName}
				size={phone ? 84 : tab === 'overview' ? 128 : 64}
				ring={phone || tab !== 'overview' ? 3 : 4}
				ringColor="var(--color-pennant-light)"
				alt={manager.managerName}
				class="head-photo"
			/>
			<div class="head-main">
				<div class="kicker">{kicker}</div>
				<h1>{manager.managerName}</h1>
				{#if tab === 'overview'}
					<div class="tenure">
						{tenureLine(
							Number(manager.firstSeason),
							Number(manager.lastSeason),
							Number(manager.totalSeasons ?? 0)
						)}
					</div>
					{#if achievements.length > 0}
						<div class="head-chips">
							{#each achievements as name (name)}
								<Tag variant={name === 'Champion' ? 'band-champion' : 'band'} size="chip-small"
									>{sentenceCase(name)}</Tag
								>
							{/each}
						</div>
					{/if}
					{#if titleYears.length > 0}
						<div class="title-years">
							{#each titleYears as year (year)}
								<span class="title-year"
									><Pennant
										staff="var(--color-white)"
										fill="var(--color-pennant-light)"
									/>{year}</span
								>
							{/each}
						</div>
					{/if}
				{/if}
			</div>
			{#if tab === 'overview'}
				<div class="head-tiles">
					{#each headline as h (h.label)}
						<div class="head-tile">
							<div class="head-tile-value">{@render value(h.value)}</div>
							<div class="head-tile-label">{h.label}</div>
						</div>
					{/each}
				</div>
			{:else}
				<div class="head-stats">
					<div>
						<div class="head-stat-value">{manager.totalChampionships ?? 0}</div>
						<div class="head-stat-label">Titles</div>
					</div>
					<div>
						<div class="head-stat-value">{record}</div>
						<div class="head-stat-label">Career</div>
					</div>
				</div>
			{/if}
		</section>

		<div class="tabs">
			<TabNav label="Profile sections" tabs={tabItems} current={tab} onselect={selectTab} />
		</div>

		{#if tab === 'overview'}
			<!-- S6.5 Overview -->
			<div class="first">
				<Card title="Wins by season" headGap={18} phoneHeadGap={16} headSpacing="8px 16px">
					{#snippet action()}
						<div class="legend">
							<span><i class="swatch title"></i>Champion</span>
							<span><i class="swatch playoffs"></i>Made playoffs</span>
							<span><i class="swatch missed"></i>Missed</span>
							<span><i class="swatch progress"></i>In progress</span>
						</div>
					{/snippet}
					<div class="chart-scroll">
						<div
							class="bars"
							role="img"
							aria-label={winsChartLabel(bars)}
							style:grid-template-columns="repeat({bars.length}, minmax(0, 1fr))"
						>
							{#each bars as bar (bar.year)}
								<div class="bar-col">
									<span class="bar-wins" aria-hidden="true">{bar.wins}</span>
									<div class="bar {bar.tone}" style:height="{bar.height}%"></div>
								</div>
							{/each}
						</div>
						<div
							class="bar-labels"
							aria-hidden="true"
							style:grid-template-columns="repeat({bars.length}, minmax(0, 1fr))"
						>
							{#each bars as bar (bar.year)}<span>{bar.label}</span>{/each}
						</div>
					</div>
				</Card>
			</div>

			<div class="split">
				<div class="split-left">
					<Card title="Career" headGap={6}>
						{#each career as row (row.label)}
							<div class="line">
								<span class="line-label">{row.label}</span>
								<span class="line-value">{@render value(row.value)}</span>
							</div>
						{/each}
					</Card>
				</div>
				<div class="split-right">
					<ManagerLuckCard {manager} variant="full" />
					<Card title="Head to head" headGap={0} headSpacing="4px 16px">
						{#snippet action()}
							<span class="note">{manager.managerName}'s share of wins</span>
						{/snippet}
						{#if h2h.length === 0}
							<p class="muted-line tight">No rivalries of five or more games yet.</p>
						{/if}
						{#each h2h as row (row.opponent)}
							<div class="share-row" class:with-ties={h2hHasTies}>
								<span class="share-name">{@render opponentName(row.opponent)}</span>
								<span class="share-bar" aria-hidden="true">
									<span
										class="share-fill"
										style:width="{row.pct * 100}%"
										style:background="var(--color-{row.bar.tone === 'up' ? 'field' : 'pennant'})"
									></span>
									<span class="share-tick"></span>
								</span>
								<span class="share-record">{row.record}</span>
							</div>
						{/each}
					</Card>
				</div>
			</div>
		{:else if tab === 'seasons'}
			<!-- S6.6 Season history -->
			<div class="first">
				<Card
					title="Season by season"
					padding="22px 24px 12px"
					phonePadding="16px 16px 8px"
					headSpacing="4px 16px"
					phoneHeadStack={false}
				>
					{#snippet action()}
						<span class="note">Expected wins and luck are regular season only.</span>
					{/snippet}
					{#if seasons.length === 0}
						<p class="muted-line tight">No seasons yet.</p>
					{:else}
						<DataTable
							label="Season by season"
							columns={seasonColumns}
							rows={seasons}
							rowKey={(r) => `${r.seasonYear}-${r.teamKey}`}
							minWidth={phone ? undefined : '820px'}
							columnGap={phone ? 8 : 14}
							rowPadding={8}
							fontSize={phone ? 14 : 15}
							highlighted={(r) => seasonOutcome(r as any, championshipSeasons).champion}
						>
							{#snippet header(column)}
								{#if column.key === 'xw'}
									<StatTip metricId="pythagorean_wins" label="Reg W / exp" />
								{:else if column.key === 'luck'}
									<StatTip metricId="luck_factor" label="Luck" phoneTap="inline-small" />
								{:else}{column.label}{/if}
							{/snippet}
							{#snippet cell(row, column)}
								{#if column.key === 'season'}
									<span class="season-year">{row.seasonYear}</span>
								{:else if column.key === 'record'}
									<span class="strong"
										>{fullRecord(Number(row.wins), Number(row.losses), Number(row.ties))}</span
									>
								{:else if column.key === 'pf'}
									{Number(row.pointsFor).toFixed(1)}
								{:else if column.key === 'pa'}
									<span class="muted">{Number(row.pointsAgainst).toFixed(1)}</span>
								{:else if column.key === 'xw'}
									{#if row.pythagoreanWins == null || row.regularSeasonWins == null}<EmptyValue
										/>{:else}<span class="soft"
											>{row.regularSeasonWins} / {Number(row.pythagoreanWins).toFixed(1)}</span
										>{/if}
								{:else if column.key === 'luck'}
									{@const text = formatLuck(row.luckFactor as number | null)}
									{#if text === null}<EmptyValue />{:else}<span
											class="strong"
											style:color="var(--color-{luckTone(row.luckFactor as number | null) === 'down'
												? 'pennant'
												: 'field'})">{text}</span
										>{/if}
								{:else if column.key === 'outcome'}
									{@const outcome = seasonOutcome(row as any, championshipSeasons)}
									<span class="outcome">
										{#if outcome.champion}<Pennant width={11} height={16} />{/if}
										<Tag variant={OUTCOME_TAG[outcome.tag]} size="row" wrapOnPhone
											>{outcome.label}</Tag
										>
									</span>
								{/if}
							{/snippet}
						</DataTable>
					{/if}
				</Card>
			</div>
		{:else if tab === 'head-to-head'}
			<!-- S6.7 Head to head -->
			<div class="first">
				<StatStrip label="Head to head highlights" stackOnPhones>
					{#each tiles as t (t.label)}
						<StatTile
							layout="avatar"
							label={t.label}
							value={t.value}
							sub={t.sub}
							avatar={t.opponent}
							tone={t.tone}
						/>
					{/each}
				</StatStrip>
			</div>
			<div class="next">
				<Card
					title="Against every manager"
					padding="22px 24px 12px"
					phonePadding="16px 16px 8px"
					headSpacing="4px 16px"
				>
					{#snippet action()}
						<span class="note">{h2hCaption(manager.currentSeason)}</span>
					{/snippet}
					{#if h2h.length === 0}
						<p class="muted-line tight">No rivalries of five or more games yet.</p>
					{:else}
						<DataTable
							label="Against every manager"
							columns={h2hColumns}
							rows={h2h}
							rowKey={(r) => r.opponent}
							minWidth={phone ? '788px' : '900px'}
							columnGap={phone ? 12 : 14}
							rowPadding={phone ? 0 : 10}
							fontSize={phone ? 14 : 15}
							stickyFirst={phone}
						>
							{#snippet cell(row, column)}
								{#if column.key === 'opponent'}
									{@const left =
										row.opponentLastSeason != null && row.opponentLastSeason < leagueLatest
											? 'League alum'
											: null}
									{#if phone}
										<svelte:element
											this={withProfile.has(row.opponent) ? 'a' : 'span'}
											href={withProfile.has(row.opponent) ? profileHref(row.opponent) : undefined}
											class="opp-phone"
										>
											<Avatar name={row.opponent} size={26} initialSize={12} alt="" />
											<span class="opp-text"
												>{row.opponent}{#if left}<span class="left-note block">{left}</span
													>{/if}</span
											>
										</svelte:element>
									{:else}
										<span class="opp">
											<Avatar name={row.opponent} size={30} initialSize={13} alt="" />
											{@render opponentName(row.opponent)}
											{#if left}<span class="left-note">{left}</span>{/if}
										</span>
									{/if}
								{:else if column.key === 'games'}
									<span class="soft">{row.games}</span>
								{:else if column.key === 'record'}
									<span class="bold">{row.record}</span>
								{:else if column.key === 'pct'}
									<span class="pct">
										<span class="pct-bar" aria-hidden="true">
											<span
												class="pct-fill"
												style:left="{row.bar.left}%"
												style:width="{row.bar.width}%"
												style:background="var(--color-{row.bar.tone === 'up'
													? 'field'
													: 'pennant'})"
											></span>
											<span class="pct-center"></span>
										</span>
										<span class="pct-text" style:color="var(--color-{LEAN_COLOR[row.pctTone]})"
											>{row.pctText}</span
										>
									</span>
								{:else if column.key === 'avg'}
									{#if row.avgPoints == null}<EmptyValue />{:else}{row.avgPoints.toFixed(1)}
										<span class="muted"
											>to {row.opponentAvgPoints != null
												? row.opponentAvgPoints.toFixed(1)
												: 'None'}</span
										>{/if}
								{:else if column.key === 'playoffs'}
									<span class="soft">{@render value(row.playoffs)}</span>
								{:else if column.key === 'streak'}
									<span class="bold" style:color="var(--color-{LEAN_COLOR[row.streak.tone]})"
										>{row.streak.text}</span
									>
								{:else if column.key === 'rivalry'}
									<a class="rivalry" href={rivalryHref(manager.managerName, row.opponent)}
										>Rivalry →</a
									>
								{/if}
							{/snippet}
						</DataTable>
						<p class="footnote">
							Avg score is {manager.managerName}'s points, then the opponent's, per meeting.
							Playoffs counts bracket games only. Each Rivalry link opens the full series on the
							League History head to head page.
						</p>
					{/if}
				</Card>
			</div>
		{:else if tab === 'achievements'}
			<!-- S6.8 Achievements -->
			<div class="split first">
				<div class="split-titles">
					<Card
						title="Title games"
						padding="22px 24px 12px"
						phonePadding="16px 16px 8px"
						headGap={10}
						headSpacing="4px 16px"
					>
						{#snippet action()}
							{#if summary}
								<span class="summary"><b>{summary.wins}</b>{summary.rest}</span>
							{/if}
						{/snippet}
						{#if titleGames.length === 0}
							<p class="muted-line tight">No title games yet.</p>
						{:else}
							{#each titleGames as g (g.seasonYear)}
								<div class="final" class:won={g.won}>
									<span class="final-year">{g.seasonYear}</span>
									<span class="final-flag">
										{#if g.won}<TitleFlags
												count={1}
												width={14}
												height={20}
												pole="ink"
												flag="pennant"
												label="Won"
											/>{/if}
									</span>
									{#if phone}
										<span class="final-body">
											<span class="final-opp">
												<span class="verb">{g.won ? 'Beat' : 'Lost to'}</span>
												{#if g.opponent}
													<Avatar name={g.opponent} size={26} alt="" />
													{#if withProfile.has(g.opponent)}
														<a class="name-link tap truncate" href={profileHref(g.opponent)}
															>{g.opponent}</a
														>
													{:else}<span class="name truncate">{g.opponent}</span>{/if}
												{:else}<span class="name truncate">a former manager</span>{/if}
											</span>
											<span class="final-score">{@render score(g)}</span>
										</span>
									{:else}
										<span class="final-opp">
											<span class="verb">{g.won ? 'Beat' : 'Lost to'}</span>
											{#if g.opponent}
												<Avatar name={g.opponent} size={26} alt="" />
												<span class="name">{@render opponentName(g.opponent)}</span>
											{:else}<span class="name">a former manager</span>{/if}
										</span>
										<span class="final-score">{@render score(g)}</span>
									{/if}
									<span class="final-tag">
										<Tag variant={g.won ? 'champion-strong' : 'runner-up'} size="row"
											>{g.won ? 'Champion' : 'Runner-up'}</Tag
										>
									</span>
								</div>
							{/each}
						{/if}
					</Card>
				</div>
				<div class="split-right">
					<Card title="League rankings" headGap={14} phoneHeadGap={12}>
						<div class="ach-ranks">
							{#each ranks as r (r.label)}
								{@const m = rankMedal(r.rank ?? 99)}
								<div class="ach-rank" style:background="var(--color-{m.tile})">
									{#if phone}
										<div class="rank-number">#{r.rank}</div>
										<div class="rank-label-phone">{r.label}</div>
									{:else}
										<span
											class="medal"
											style:background="var(--color-{m.medal})"
											style:color="var(--color-{m.text})">#{r.rank}</span
										>
										<span class="ach-rank-label">{r.label}</span>
									{/if}
								</div>
							{/each}
						</div>
					</Card>
					<Card title="Badges" headGap={6}>
						{#each badgeRows as b (b.label)}
							<div class="badge-row">
								<span class="line-label"
									><StatTip metricId={b.metricId} label={b.label} phoneTap="inline" /></span
								>
								{#if b.value}
									<Tag variant={b.variant} size="chip-badge">{sentenceCase(b.value)}</Tag>
								{:else}<EmptyValue />{/if}
							</div>
						{/each}
					</Card>
				</div>
			</div>
			<div class="next">
				<Card title="Career marks" headGap={14} phoneHeadGap={12}>
					<div class="marks">
						{#each marks as m (m.label)}
							<div>
								<div class="mark-value" style:color="var(--color-{m.tone})">
									{@render value(m.value)}
								</div>
								<div class="mark-label">{m.label}</div>
								<div class="mark-sub">{m.sub}</div>
							</div>
						{/each}
					</div>
				</Card>
			</div>
		{/if}
	{/if}
</div>

{#snippet score(g: TitleGame)}
	{g.points != null ? g.points.toFixed(1) : 'None'} to {g.opponentPoints != null
		? g.opponentPoints.toFixed(1)
		: 'None'}
{/snippet}

<style>
	/* Main is 28px on top on every profile tab, as drawn (40px elsewhere). */
	.profile {
		margin-top: -12px;
	}

	.back {
		display: inline-flex;
		align-items: center;
		min-height: 36px;
		font-size: 15px;
		font-weight: 600;
		color: var(--color-field);
		text-decoration: none;
	}

	.back:hover {
		color: var(--color-clubhouse);
	}

	.state,
	.first,
	.next {
		margin-top: 20px;
	}

	.muted-line {
		margin: 0 0 16px;
		font-size: 15px;
		color: var(--color-muted);
	}

	.muted-line.tight {
		margin: 8px 0 10px;
	}

	.note {
		font-size: 13px;
		color: var(--color-muted);
	}

	.soft {
		color: var(--color-ink-2);
	}

	.muted {
		color: var(--color-muted);
	}

	.strong {
		font-weight: 600;
	}

	.bold {
		font-weight: 700;
	}

	.name-link {
		color: inherit;
		text-decoration: none;
	}

	.name-link:hover {
		color: var(--color-field);
	}

	.sk-header {
		margin-top: 12px;
		height: 220px;
		border-radius: var(--radius-card);
		background: var(--color-line-soft);
	}

	/* ---------- The header (S6.4) ---------- */
	/* Sits on its line like the board's <img>: an inline-block that clips
	   puts its baseline under the ring, not under the photo inside it. */
	.header :global(.head-photo) {
		display: inline-block;
	}

	.header {
		margin-top: 12px;
		display: flex;
		flex-wrap: wrap;
		gap: 28px;
		align-items: center;
		padding: 28px;
		border-radius: var(--radius-card);
		background: var(--color-clubhouse);
		color: var(--color-white);
	}

	.head-main {
		flex: 999 1 360px;
		min-width: 0;
	}

	.kicker {
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-pennant-light);
	}

	h1 {
		margin: 6px 0;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 64px;
		line-height: 1;
	}

	.tenure {
		font-size: 16px;
		color: var(--color-on-clubhouse);
	}

	.head-chips {
		margin-top: 12px;
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.title-years {
		margin-top: 16px;
		display: flex;
		flex-wrap: wrap;
		gap: 10px 16px;
	}

	.title-year {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		font-size: 14px;
		font-weight: 700;
	}

	.head-tiles {
		flex: 1 1 320px;
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 12px;
	}

	.head-tile {
		padding: 14px 16px;
		border-radius: 10px;
		background: var(--color-clubhouse-2);
	}

	.head-tile-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 32px;
		line-height: 1;
	}

	.head-tile-label {
		margin-top: 4px;
		font-size: 13px;
		color: var(--color-on-clubhouse-2);
	}

	.header.compact {
		gap: 16px 20px;
		padding: 20px 24px;
	}

	.compact .head-main {
		flex: 1 1 240px;
	}

	.compact .kicker {
		font-size: 12px;
	}

	.compact h1 {
		margin: 4px 0 0;
		font-size: 40px;
	}

	.head-stats {
		display: flex;
		gap: 24px;
		text-align: right;
	}

	.head-stat-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 26px;
	}

	.head-stat-label {
		font-size: 12px;
		color: var(--color-on-clubhouse-2);
	}

	.tabs {
		margin-top: 20px;
	}

	/* ---------- Overview (S6.5) ---------- */
	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 6px 16px;
		font-size: 13px;
		color: var(--color-ink-2);
	}

	.legend span {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}

	.swatch {
		width: 12px;
		height: 12px;
		border-radius: 3px;
		box-sizing: border-box;
	}

	.swatch.title,
	.bar.title {
		background: var(--color-pennant);
	}

	.swatch.playoffs,
	.bar.playoffs {
		background: var(--color-field);
	}

	.swatch.missed,
	.bar.missed {
		background: var(--color-line-strong);
	}

	.swatch.progress {
		border: 1.5px dashed var(--color-field);
	}

	.bar.progress {
		background: transparent;
		border: 1.5px dashed var(--color-field);
	}

	.chart-scroll {
		overflow-x: auto;
	}

	/* Content-box, as drawn: 220px of bars, then the 2px rule. */
	.bars {
		box-sizing: content-box;
		min-width: 760px;
		display: grid;
		gap: 6px;
		align-items: end;
		height: 220px;
		border-bottom: 2px solid var(--color-ink);
	}

	.bar-col {
		display: flex;
		flex-direction: column;
		align-items: center;
		justify-content: flex-end;
		height: 100%;
	}

	.bar-wins {
		margin-bottom: 4px;
		font-size: 12px;
		font-weight: 700;
		color: var(--color-ink-2);
	}

	.bar {
		width: 100%;
		border-radius: 4px 4px 0 0;
		box-sizing: border-box;
	}

	.bar-labels {
		min-width: 760px;
		display: grid;
		gap: 6px;
		margin-top: 6px;
	}

	.bar-labels span {
		text-align: center;
		font-size: 12px;
		color: var(--color-muted);
	}

	.split {
		margin-top: 20px;
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: flex-start;
	}

	.split.first {
		margin-top: 20px;
	}

	.split-left,
	.split-right {
		flex: 1 1 340px;
		min-width: 0;
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	/* The boards set these bases on a content-box card, so its 24px of side
	   padding and 1px border sit outside them: 50px more on the wrapper. */
	.split-left {
		flex-basis: 390px;
	}

	.split-titles {
		flex: 1.5 1 570px;
		min-width: 0;
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
		font-weight: 600;
		text-align: right;
	}

	.share-row {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 140px 52px;
		gap: 12px;
		align-items: center;
		padding: 8px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	.share-name {
		font-weight: 600;
		min-width: 0;
	}

	.share-bar {
		position: relative;
		display: block;
		height: 8px;
		border-radius: 4px;
		background: var(--color-pennant-tint);
	}

	.share-fill {
		display: block;
		height: 8px;
		border-radius: 4px;
	}

	.share-tick {
		position: absolute;
		left: 50%;
		top: -3px;
		width: 1px;
		height: 14px;
		background: var(--color-ink);
	}

	/* 52px as drawn; a list with a tied record ("14-15-1") widens every row
	   alike, so the bars stay in one column. */
	.share-row.with-ties {
		grid-template-columns: minmax(0, 1fr) 140px 64px;
	}

	.share-record {
		white-space: nowrap;
		text-align: right;
		font-weight: 600;
	}

	/* ---------- Season history (S6.6) ---------- */
	.season-year {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 17px;
	}

	.outcome {
		display: flex;
		align-items: center;
		gap: 6px;
	}

	/* ---------- Head to head (S6.7) ---------- */
	.opp {
		display: flex;
		align-items: center;
		gap: 10px;
		font-weight: 600;
	}

	.left-note {
		font-size: 12px;
		font-weight: 500;
		color: var(--color-muted);
	}

	.left-note.block {
		display: block;
	}

	.pct {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	.pct-bar {
		position: relative;
		flex: 1;
		height: 8px;
		border-radius: 4px;
		background: var(--color-wash);
	}

	.pct-fill {
		position: absolute;
		top: 0;
		bottom: 0;
		border-radius: 4px;
	}

	.pct-center {
		position: absolute;
		top: -3px;
		bottom: -3px;
		left: 50%;
		width: 1.5px;
		background: var(--color-ink-2);
	}

	.pct-text {
		width: 44px;
		text-align: right;
		font-weight: 600;
	}

	.rivalry {
		font-size: 14px;
		font-weight: 600;
		color: var(--color-field);
		text-decoration: none;
	}

	.rivalry:hover {
		color: var(--color-clubhouse);
	}

	.footnote {
		margin: 12px 0 6px;
		font-size: 13px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	/* ---------- Achievements (S6.8) ---------- */
	.summary {
		display: block;
		font-size: 14px;
		color: var(--color-ink-2);
	}

	.summary b {
		color: var(--color-pennant-dark);
	}

	.final {
		display: grid;
		grid-template-columns: 64px 26px minmax(0, 1fr) auto 96px;
		gap: 0 12px;
		align-items: center;
		padding: 11px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	.final.won {
		background: var(--color-pennant-wash);
	}

	.final-year {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 19px;
	}

	.final-flag {
		display: flex;
	}

	.final-opp {
		display: flex;
		align-items: center;
		gap: 10px;
		min-width: 0;
	}

	.verb {
		flex: none;
		color: var(--color-muted);
	}

	.name {
		font-weight: 600;
	}

	.final-score {
		font-weight: 600;
	}

	.final-tag {
		justify-self: end;
	}

	.ach-ranks {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 10px;
	}

	.ach-rank {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 12px;
		border-radius: 10px;
	}

	.medal {
		flex: none;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 44px;
		height: 44px;
		border-radius: 50%;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 19px;
	}

	.ach-rank-label {
		font-size: 14px;
		font-weight: 700;
	}

	.badge-row {
		display: flex;
		justify-content: space-between;
		align-items: center;
		gap: 12px;
		padding: 10px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	.marks {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
		gap: 18px 16px;
	}

	.mark-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 32px;
		line-height: 1;
	}

	.mark-label {
		margin-top: 5px;
		font-size: 14px;
		font-weight: 600;
	}

	.mark-sub {
		margin-top: 2px;
		font-size: 13px;
		color: var(--color-muted);
	}

	/* ---------- Phones (S6.9) ---------- */
	@media (max-width: 639.98px) {
		.profile {
			margin-top: 0;
		}

		.back {
			min-height: 44px;
		}

		.state,
		.first,
		.next,
		.split,
		.split.first {
			margin-top: 14px;
		}

		.split,
		.split-left,
		.split-right {
			gap: 14px;
		}

		.split-left,
		.split-right,
		.split-titles {
			flex: 1 1 100%;
		}

		.sk-header {
			margin-top: 8px;
		}

		.header,
		.header.compact {
			display: block;
			margin-top: 8px;
			padding: 18px;
			/* The board's line box: the photo sits on a baseline at normal
			   line height, 4px of descent under it. */
			line-height: normal;
			text-align: center;
		}

		.kicker,
		.compact .kicker {
			margin-top: 10px;
			font-size: 12px;
		}

		h1,
		.compact h1 {
			margin: 4px 0 0;
			font-size: 38px;
			letter-spacing: -0.01em;
			overflow-wrap: anywhere;
		}

		.tenure {
			margin-top: 8px;
			font-size: 15px;
		}

		.head-chips {
			justify-content: center;
		}

		.title-years {
			margin-top: 14px;
			justify-content: center;
		}

		.head-tiles {
			margin-top: 16px;
			gap: 10px;
		}

		.head-tile {
			padding: 12px 14px;
		}

		.head-tile-value {
			font-size: 24px;
		}

		.head-tile-label {
			font-size: 12px;
		}

		.head-stats {
			justify-content: center;
			margin-top: 16px;
			padding-top: 14px;
			border-top: 1px solid var(--color-clubhouse-2);
			text-align: center;
		}

		.tabs {
			margin-top: 14px;
		}

		.bars,
		.bar-labels {
			min-width: 612px;
			gap: 4px;
		}

		.bars {
			height: 180px;
		}

		.share-row {
			grid-template-columns: minmax(0, 1fr) 100px 46px;
			min-height: 44px;
			padding: 0;
			box-sizing: content-box;
		}

		.share-row.with-ties {
			grid-template-columns: minmax(0, 1fr) 100px 60px;
		}

		.share-name .name-link {
			display: flex;
			align-items: center;
			min-height: 44px;
		}

		.season-year {
			font-size: 16px;
		}

		.opp-phone {
			display: flex;
			align-items: center;
			gap: 8px;
			min-height: 52px;
			padding-right: 8px;
			font-weight: 600;
			color: inherit;
			text-decoration: none;
		}

		.opp-text {
			min-width: 0;
		}

		.pct-text {
			width: 40px;
		}

		.rivalry {
			display: flex;
			align-items: center;
			justify-content: flex-end;
			min-height: 44px;
		}

		.final {
			grid-template-columns: 40px 14px minmax(0, 1fr) 80px;
			gap: 0 10px;
			padding: 10px 0;
			font-size: 14px;
		}

		.final-year {
			font-size: 17px;
		}

		.final-body {
			display: block;
			min-width: 0;
		}

		.final-body .final-opp {
			gap: 8px;
		}

		.final-body .final-score {
			display: block;
			margin-top: 4px;
		}

		.truncate {
			min-width: 0;
			overflow: hidden;
			text-overflow: ellipsis;
			white-space: nowrap;
		}

		.name-link.tap {
			display: inline-block;
			padding: 12px 0;
			margin: -12px 0;
			line-height: 20px;
			font-weight: 600;
		}

		.ach-rank {
			display: block;
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

		.badge-row {
			min-height: 44px;
			padding: 0;
			box-sizing: content-box;
		}

		.marks {
			grid-template-columns: repeat(2, minmax(0, 1fr));
			gap: 16px 12px;
		}

		.mark-value {
			font-size: 24px;
		}
	}
</style>

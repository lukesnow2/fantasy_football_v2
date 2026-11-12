<script lang="ts">
	import { tick } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import { page } from '$app/state';
	import { afterNavigate, replaceState } from '$app/navigation';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import ClientOnlyD3Overview from '$lib/components/ClientOnlyD3Overview.svelte';
	import ManagerPicker from '$lib/components/history/ManagerPicker.svelte';
	import {
		Avatar,
		Button,
		Card,
		DataTable,
		EmptyValue,
		Pennant,
		StatStrip,
		StatTile,
		StatTip,
		TabNav,
		Tag,
		type Column
	} from '$lib/components/ui';
	import { profileHref } from '$lib/home';
	import { rivalryHref } from '$lib/utils/managers';
	import {
		HISTORY_TABS,
		TABLE_ROWS,
		computeEras,
		defaultPair,
		faceRecord,
		filterRivalries,
		findRivalry,
		historyIntro,
		historyKicker,
		historyTab,
		luckier,
		oneDecimal,
		plural,
		recordBook,
		recordChips,
		recordFootnote,
		resolveManager,
		rivalryFromRow,
		rivalryManagers,
		seriesLeader,
		seriesShare,
		spanYears,
		tableLeader,
		winShare,
		type H2HRow,
		type HistoryTab,
		type Rivalry
	} from '$lib/history-display';

	const phone = new MediaQuery('(max-width: 639.98px)');

	/* ---------- Data, fetched per tab and kept (G4) ---------- */

	let overviewData: any = $state(null);
	let h2hData: any = $state(null);
	let recordBookData: any = $state(null);
	let loadingOverview = $state(false);
	let loadingH2H = $state(false);
	let loadingRecords = $state(false);
	let overviewFailed = $state(false);
	let h2hFailed = $state(false);
	let recordsFailed = $state(false);

	async function loadOverviewData() {
		if (overviewData || loadingOverview) return;
		loadingOverview = true;
		overviewFailed = false;
		try {
			const response = await fetch('/api/overview?metric=all');
			if (response.ok) overviewData = await response.json();
			else overviewFailed = true;
		} catch {
			overviewFailed = true;
		} finally {
			loadingOverview = false;
		}
	}

	async function loadH2HData() {
		if (h2hData || loadingH2H) return;
		loadingH2H = true;
		h2hFailed = false;
		try {
			const response = await fetch('/api/head-to-head?analysis=all');
			if (response.ok) {
				h2hData = await response.json();
				setPairFromUrl();
			} else h2hFailed = true;
		} catch {
			h2hFailed = true;
		} finally {
			loadingH2H = false;
		}
	}

	async function loadRecordBookData() {
		if (recordBookData || loadingRecords) return;
		loadingRecords = true;
		recordsFailed = false;
		try {
			const response = await fetch('/api/record-book');
			if (response.ok) recordBookData = await response.json();
			else recordsFailed = true;
		} catch {
			recordsFailed = true;
		} finally {
			loadingRecords = false;
		}
	}

	/* ---------- S10.1 Header and tabs in the URL ---------- */

	// The face-off's pair, which the head to head tab's link carries.
	let pairA = $state<string | null>(null);
	let pairB = $state<string | null>(null);
	/** A reader changed a name, so the table filters to the pair (D10). */
	let picked = $state(false);
	/** The URL named both managers, so the tab's link keeps them. */
	let fromUrl = $state(false);

	let tab = $state<HistoryTab>(historyTab(page.url.searchParams));

	// The header reads the overview on every tab, so it loads first.
	function openTab(id: HistoryTab) {
		loadOverviewData();
		if (id === 'head-to-head') loadH2HData();
		else if (id === 'records') loadRecordBookData();
	}

	// The first load and any real navigation (a profile's rivalry link) land
	// on what the URL names; SvelteKit runs this after the first mount too.
	afterNavigate(() => {
		tab = historyTab(page.url.searchParams);
		openTab(tab);
		// The URL's pair replaces any earlier pick, so the table stops filtering (D10).
		picked = false;
		if (h2hData) setPairFromUrl();
	});

	function hrefFor(id: HistoryTab): string {
		if (id === 'head-to-head' && (picked || fromUrl) && pairA && pairB) return rivalryHref(pairA, pairB);
		return HISTORY_TABS.find((t) => t.id === id)!.href;
	}

	const tabItems = $derived(HISTORY_TABS.map((t) => ({ id: t.id, label: t.label, href: hrefFor(t.id) })));

	function selectTab(id: string) {
		tab = id as HistoryTab;
		openTab(tab);
		// Shallow: no reload and no history entry, as the profile's tabs do.
		replaceState(hrefFor(tab), {});
	}

	const meta = $derived(overviewData?.meta ?? null);
	const liveSeason = $derived<number | null>(overviewData?.data?.seasonInProgress?.seasonYear ?? null);

	/* ---------- S10.2 Overview: three eras ---------- */

	let selectedEra = $state('all');
	const eras = $derived(computeEras(overviewData?.data));

	/* ---------- S10.5 to S10.7 Head to head ---------- */

	const rows = $derived<H2HRow[]>(h2hData?.headToHead ?? []);
	const managers = $derived(rivalryManagers(rows));
	const overview = $derived(h2hData?.analytics?.overview ?? null);
	const records = $derived(h2hData?.analytics?.records ?? null);

	let openPicker = $state<'a' | 'b' | null>(null);
	let showAll = $state(false);
	let faceoff: HTMLElement | null = $state(null);

	function setPairFromUrl() {
		const params = page.url.searchParams;
		const names = rivalryManagers(h2hData?.headToHead ?? []);
		const a = resolveManager(params.get('a'), names);
		const b = resolveManager(params.get('b'), names);
		const pair = defaultPair(h2hData?.headToHead ?? [], a, b);
		if (pair) [pairA, pairB] = pair;
		fromUrl = !!(a && b && a !== b);
	}

	function setPair(a: string, b: string) {
		pairA = a;
		pairB = b;
		picked = true;
		showAll = false;
		replaceState(rivalryHref(a, b), {});
	}

	function pick(side: 'a' | 'b', name: string) {
		if (side === 'a') setPair(name, pairB ?? '');
		else setPair(pairA ?? '', name);
	}

	async function openRivalry(event: MouseEvent, r: Rivalry) {
		if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
		event.preventDefault();
		setPair(r.a, r.b);
		await tick();
		const still = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
		faceoff?.scrollIntoView({ behavior: still ? 'auto' : 'smooth', block: 'start' });
	}

	const rivalry = $derived(pairA && pairB ? findRivalry(rows, pairA, pairB) : null);
	const leader = $derived(rivalry ? seriesLeader(rivalry) : null);
	const share = $derived(rivalry ? seriesShare(rivalry.aWins, rivalry.bWins, rivalry.games) : null);
	const optionsA = $derived(managers.filter((m) => m !== pairB));
	const optionsB = $derived(managers.filter((m) => m !== pairA));

	function sideLine(side: 'a' | 'b'): { text: string; lead: boolean } | null {
		if (!rivalry) return null;
		const me = side === 'a' ? pairA : pairB;
		if (!leader) return side === 'a' ? { text: 'Series tied', lead: false } : { text: rivalry.span, lead: false };
		return leader === me ? { text: 'Leads the series', lead: true } : { text: rivalry.span, lead: false };
	}

	const longest = $derived(rows.slice(0, 6).map((r) => rivalryFromRow(r)));
	const filtered = $derived(filterRivalries(rows, picked, pairA, pairB));
	const tableRows = $derived(showAll ? filtered : filtered.slice(0, TABLE_ROWS));
	const titleGames = $derived(
		(h2hData?.analytics?.playoffImpact ?? []).filter((b: any) => Number(b.championshipMatchups) > 0).slice(0, 8)
	);

	const columns = $derived<Column[]>([
		{ key: 'match', label: 'Matchup', width: phone.current ? '168px' : undefined },
		{ key: 'games', label: 'Games', numeric: true, width: phone.current ? '44px' : '50px', fixedHeader: true },
		{ key: 'record', label: 'Record', numeric: true, width: phone.current ? '64px' : '70px' },
		{ key: 'leader', label: 'Leader', width: '130px' },
		{ key: 'avg', label: 'Avg total', numeric: true, width: '64px', wrapHeader: true },
		{ key: 'best', label: 'Best win', numeric: true, width: '64px', wrapHeader: true },
		{ key: 'expected', label: 'Expected', numeric: true, width: '84px', wrapHeader: true },
		{ key: 'luckier', label: 'Luckier', numeric: true, width: '130px' }
	]);

	/* ---------- S10.8 Record book ---------- */

	let chip = $state('all');
	const bookRecords = $derived(recordBookData?.records ?? []);
	const chips = $derived(recordChips(bookRecords));
	const book = $derived(recordBook(bookRecords, chip, liveSeason));
</script>

<svelte:head>
	<title>League History · The League</title>
</svelte:head>

{#snippet name(n: string)}<a class="name-link" href={profileHref(n)}>{n}</a>{/snippet}

{#snippet names(list: string[])}{#each list as n, i (n)}{#if i > 0}{i === list.length - 1 ? ' and ' : ', '}{/if}{@render name(
			n
		)}{/each}{/snippet}

{#snippet failure(title: string, retry: () => void)}
	<div class="first">
		<Card {title}>
			<p class="muted-line">Try again in a minute.</p>
			<Button variant="secondary" onclick={retry}>Try again</Button>
		</Card>
	</div>
{/snippet}

{#snippet loadingCard()}
	<div class="first">
		<Card title="Loading league history" headGap={0} />
	</div>
{/snippet}

<PageHeader
	kicker={historyKicker(meta?.firstSeason, meta?.lastSeason)}
	title="League History"
	introWidth={640}
	pending={!meta && !overviewFailed}
>
	{historyIntro(meta?.totalSeasons)}
</PageHeader>

<div class="tabs">
	<TabNav label="History sections" tabs={tabItems} current={tab} onselect={selectTab} />
</div>

{#if tab === 'overview'}
	{#if overviewFailed}
		{@render failure("League history didn't load", () => loadOverviewData())}
	{:else if !overviewData}
		{@render loadingCard()}
	{:else}
		<ClientOnlyD3Overview data={overviewData} bind:selectedEra />

		{#if eras.length}
			<section class="section" aria-labelledby="eras-title">
				<h2 id="eras-title" class="section-title">Three eras</h2>
				<div class="eras">
					{#each eras as e (e.name)}
						<Card>
							<div class="era-head">
								<h3>{e.name}</h3>
								<span class="era-span">{e.span}</span>
							</div>
							{#if e.titles}
								<div class="era-titles">
									<Pennant width={14} height={20} />
									<span><b>{@render names(e.leaders)} ({e.leaderTitles})</b> won the most titles</span>
								</div>
							{/if}
							{#each e.rows as r (r.label)}
								<div class="era-stat"><span>{r.label}</span><span class="era-value">{r.value}</span></div>
							{/each}
						</Card>
					{/each}
				</div>
			</section>
		{/if}
	{/if}
{:else if tab === 'head-to-head'}
	{#if h2hFailed}
		{@render failure("League history didn't load", () => loadH2HData())}
	{:else if !h2hData}
		{@render loadingCard()}
	{:else}
		{#if overview}
			<div class="first">
				<StatStrip label="Rivalries in numbers" twoUpOnPhones>
					<StatTile dense value={String(overview.totalRivalries)} label="rivalries" />
					<StatTile dense value={oneDecimal(overview.avgMatchupsPerRivalry)} label="games per rivalry, on average" />
					<StatTile dense value={String(overview.longestRivalrySeasons)} label="seasons, the longest rivalry" />
					<StatTile dense value={String(overview.championshipRivalries)} label="pairs who met in a title game" />
				</StatStrip>
			</div>
		{/if}

		{#if pairA && pairB}
			{@const lineA = sideLine('a')}
			{@const lineB = sideLine('b')}
			<div class="block">
				<Card tone="clubhouse" padding="24px 28px" phonePadding="18px" label="Face-off" bind:element={faceoff}>
					<div class="faceoff">
						<div class="side side-a">
							<Avatar
								name={pairA}
								size={88}
								phoneSize={64}
								ring={4}
								ringColor={rivalry && leader === pairA ? 'var(--color-pennant-light)' : 'var(--color-field-2)'}
								alt=""
							/>
							<div class="side-text">
								<ManagerPicker
									name={pairA}
									options={optionsA}
									open={openPicker === 'a'}
									ontoggle={(o) => (openPicker = o ? 'a' : null)}
									onpick={(n) => pick('a', n)}
								/>
								{#if lineA}<div class="side-line" class:lead={lineA.lead}>{lineA.text}</div>{/if}
							</div>
						</div>
						<div class="center">
							{#if rivalry}
								<div class="record">{faceRecord(rivalry)}</div>
								<div class="record-sub">{rivalry.games} games over {rivalry.seasons} seasons</div>
							{:else}
								<p class="few">{pairA} and {pairB} have played fewer than five games.</p>
							{/if}
						</div>
						<div class="side side-b">
							<Avatar
								name={pairB}
								size={88}
								phoneSize={64}
								ring={4}
								ringColor={rivalry && leader === pairB ? 'var(--color-pennant-light)' : 'var(--color-field-2)'}
								alt=""
							/>
							<div class="side-text">
								<ManagerPicker
									name={pairB}
									options={optionsB}
									open={openPicker === 'b'}
									align="end"
									ontoggle={(o) => (openPicker = o ? 'b' : null)}
									onpick={(n) => pick('b', n)}
								/>
								{#if lineB}<div class="side-line" class:lead={lineB.lead}>{lineB.text}</div>{/if}
							</div>
						</div>
					</div>
					<p class="hint">Tap either name to swap in someone else.</p>
					{#if rivalry && share}
						<div class="share" role="img" aria-label="{pairA} won {rivalry.aWins} of {rivalry.games}, {pairB} won {rivalry.bWins}">
							<span class="share-a" style:width="{share.a}%"></span><span class="share-b" style:width="{share.b}%"></span>
						</div>
						<div class="face-tiles">
							<Card tone="raised" padding="12px 14px" phonePadding="12px">
								<div class="tile-value">{oneDecimal(rivalry.avgTotal)}</div>
								<div class="tile-label">combined points per game</div>
							</Card>
							<Card tone="raised" padding="12px 14px" phonePadding="12px">
								<div class="tile-value">{oneDecimal(rivalry.bestWin)}</div>
								<div class="tile-label">biggest winning margin</div>
							</Card>
							<Card tone="raised" padding="12px 14px" phonePadding="12px">
								<div class="tile-value">{rivalry.titleGames}</div>
								<div class="tile-label">title games</div>
							</Card>
							<Card tone="raised" padding="12px 14px" phonePadding="12px">
								<div class="tile-value">{rivalry.playoffGames}</div>
								<div class="tile-label">playoff games</div>
							</Card>
							<div class="wide-tile">
								<Card tone="raised" padding="12px 14px" phonePadding="12px">
									<div class="tile-value">
										{#if rivalry.keyWinner && rivalry.keySeason}{@render name(rivalry.keyWinner)}, {rivalry.keySeason}{:else}<EmptyValue
											/>{/if}
									</div>
									<div class="tile-label">the one that mattered most</div>
								</Card>
							</div>
						</div>
					{/if}
				</Card>
			</div>
		{/if}

		{#if longest.length}
			<section class="block section-gap" aria-labelledby="longest-title">
				<h2 id="longest-title" class="section-title">Longest rivalries</h2>
				<div class="rivals">
					{#each longest as r (r.a + r.b)}
						{@const w = winShare(r.aWins, r.bWins)}
						<a class="rival" href={rivalryHref(r.a, r.b)} onclick={(e) => openRivalry(e, r)}>
							<Card padding="18px 20px" phonePadding="14px 16px">
								<div class="rival-top">
									<Avatar name={r.a} size={40} phoneSize={32} alt="" />
									<span class="rival-name">{r.a}</span>
									<span class="rival-record">{r.aWins}-{r.bWins}</span>
									<span class="rival-name end">{r.b}</span>
									<Avatar name={r.b} size={40} phoneSize={32} alt="" />
								</div>
								<div class="rival-bar" aria-hidden="true">
									<span class="bar-a" style:width="{w.a}%"></span><span class="bar-b" style:width="{w.b}%"></span>
								</div>
								<div class="rival-meta">
									<span>{r.games} games · {r.seasons} seasons</span>
									{#if r.titleGames > 0}
										<span class="titles"><Pennant width={9} height={13} stroke={1.8} />{plural(r.titleGames, 'title game')}</span>
									{/if}
									<span>{plural(r.playoffGames, 'playoff game')}</span>
								</div>
							</Card>
						</a>
					{/each}
				</div>
			</section>
		{/if}

		{#if records}
			<div class="block">
				<StatStrip label="Head to head records" twoUpOnPhones>
					<StatTile
						layout="record"
						avatar={records.highestScoreManager}
						label="Highest score"
						value={oneDecimal(records.highestScoreValue)}
						holder={records.highestScoreManager ?? undefined}
						holderHref={records.highestScoreManager ? profileHref(records.highestScoreManager) : undefined}
					/>
					<StatTile
						layout="record"
						avatar={records.biggestWinManager}
						label="Biggest blowout"
						value={oneDecimal(records.biggestWinValue)}
						holder={records.biggestWinManager ?? undefined}
						holderHref={records.biggestWinManager ? profileHref(records.biggestWinManager) : undefined}
					/>
					<StatTile
						layout="record"
						avatar={records.longestStreakManager}
						label="Hot streak"
						value={String(records.longestStreakValue)}
						holder={records.longestStreakManager ?? undefined}
						holderHref={records.longestStreakManager ? profileHref(records.longestStreakManager) : undefined}
					/>
					<StatTile
						layout="record"
						avatar={records.worstStreakManager}
						label="Cold streak"
						value={String(Math.abs(Number(records.worstStreakValue)))}
						holder={records.worstStreakManager ?? undefined}
						holderHref={records.worstStreakManager ? profileHref(records.worstStreakManager) : undefined}
					/>
				</StatStrip>
			</div>
		{/if}

		<div class="block tables">
			<div class="every">
				<Card
					title="Every rivalry"
					padding="22px 24px 12px"
					phonePadding="16px 16px 8px"
					headGap={10}
					phoneHeadStack
				>
					{#snippet action()}<span class="note">Sorted by games played. The names you pick above filter this table too.</span>{/snippet}
					<DataTable
						{columns}
						rows={tableRows}
						label="Every rivalry"
						rowPadding={10}
						columnGap={12}
						minWidth={phone.current ? '832px' : '866px'}
						fontSize={phone.current ? 14 : 15}
						stickyFirst={phone.current}
						headerAlign="top"
						rowKey={(r) => `${r.managerAName}|${r.managerBName}`}
					>
						{#snippet header(column)}
							{#if column.key === 'expected'}
								<StatTip metricId="pythagorean_wins" label="Expected" phoneTap="inline-small" />
							{:else if column.key === 'luckier'}
								<StatTip metricId="luck_factor" label="Luckier" phoneTap="inline-small" />
							{:else}{column.label}{/if}
						{/snippet}
						{#snippet cell(row, column)}
							{#if column.key === 'match'}
								<span class="match"
									>{@render name(row.managerAName)} vs {@render name(row.managerBName)}</span
								>
								<span class="match-sub"
									>{row.seasonsPlayedTogether} seasons · {spanYears(row.firstMatchupDate, row.lastMatchupDate)}</span
								>
							{:else if column.key === 'games'}
								{row.totalMatchups}
							{:else if column.key === 'record'}
								<span class="strong">{row.seriesRecord}</span>
							{:else if column.key === 'leader'}
								{@const lead = tableLeader(row)}
								{#if lead}
									<a class="leader" href={profileHref(lead)}><Avatar name={lead} size={22} alt="" />{lead}</a>
								{:else}
									<span class="tied">Tied</span>
								{/if}
							{:else if column.key === 'avg'}
								<span class="soft">{oneDecimal(row.avgTotalPointsPerGame)}</span>
							{:else if column.key === 'best'}
								<span class="soft">{oneDecimal(row.mostLopsidedGame)}</span>
							{:else if column.key === 'expected'}
								<span class="soft"
									>{oneDecimal(row.managerAPythagoreanWins)}-{oneDecimal(row.managerBPythagoreanWins)}</span
								>
							{:else if column.key === 'luckier'}
								{@const l = luckier(row)}
								<span class="luckier"
									>{#if l}<span class="strong-600">{@render name(l.name)}</span>
										<span class="luck">{l.luck}</span>{:else}<EmptyValue text="Even" />{/if}</span
								>
							{/if}
						{/snippet}
					</DataTable>
					{#if filtered.length > TABLE_ROWS}
						<Button variant="link" size="tap" onclick={() => (showAll = !showAll)}>
							{showAll ? `Show the top ${TABLE_ROWS}` : `Show all ${filtered.length} →`}
						</Button>
					{/if}
				</Card>
			</div>

			{#if titleGames.length}
				<div class="title-games">
					<Card title="Title-game rivalries" tone="pennant" titleSize={26} headGap={0}>
						<p class="title-games-sub">Pairs who've met with a championship on the line.</p>
						{#each titleGames as b (b.managerAName + b.managerBName)}
							<div class="tg-row">
								<div class="tg-top">
									<b>{@render name(b.managerAName)} vs {@render name(b.managerBName)}</b>
									<span class="tg-count">{plural(Number(b.championshipMatchups), 'title game')}</span>
								</div>
								{#if b.mostImportantGameWinner}
									<div class="tg-key">
										Biggest one: {b.mostImportantGameWinner}, {b.mostImportantGameSeason} championship
									</div>
								{/if}
							</div>
						{/each}
					</Card>
				</div>
			{/if}
		</div>
	{/if}
{:else if tab === 'records'}
	{#if recordsFailed}
		{@render failure("League history didn't load", () => loadRecordBookData())}
	{:else if !recordBookData}
		{@render loadingCard()}
	{:else}
		<div class="first chips">
			<span class="count">{plural(bookRecords.length, 'record')}</span>
			{#each chips as c (c.id)}
				<Button variant="filter" aria-pressed={chip === c.id} onclick={() => (chip = c.id)}
					>{c.label} <span class="chip-count">{c.count}</span></Button
				>
			{/each}
		</div>

		{#if book.champion.length}
			<div class="block">
				<Card
					title="Championship records"
					tone="clubhouse"
					titleSize={26}
					padding="24px 28px"
					phonePadding="18px"
					headGap={16}
					phoneHeadGap={14}
				>
					{#snippet icon()}<Pennant
							staff="var(--color-white)"
							fill="var(--color-pennant-light)"
							width={14}
							height={20}
						/>{/snippet}
					<div class="champ-grid">
						{#each book.champion as r (r.key)}
							<Card tone="raised" padding="18px 20px" phonePadding="14px 16px">
								<div class="champ">
									{#if r.face}
										<Avatar name={r.face} size={52} phoneSize={44} ring={3} ringColor="var(--color-pennant-light)" alt="" />
									{/if}
									<div class="champ-text">
										<div class="champ-label">{r.label}</div>
										<div class="champ-value"><span class="champ-number">{r.value}</span><span class="champ-unit">{r.unit}</span></div>
										<div class="champ-holder">{#each r.names as n (n)}{@render name(n)}{/each}</div>
									</div>
								</div>
							</Card>
						{/each}
					</div>
				</Card>
			</div>
		{/if}

		{#each book.groups as g (g.id)}
			<div class="block">
				<Card title={g.name} titleSize={26} headRule>
					{#snippet action()}<span class="group-count">{g.count}</span>{/snippet}
					<div class="book-grid">
						{#each g.rows as r (r.key)}
							<div class="book-row">
								<Avatar name={r.face ?? 'Hidden'} size={44} phoneSize={40} initialTone="field" initialSize={16} alt="" />
								<div class="book-text">
									<div class="book-label">{r.label}</div>
									<div class="book-value">
										<span class="book-number" style:color="var(--color-{r.tone})">{r.value}</span>
										{#if r.unit}<span class="book-unit">{r.unit}</span>{/if}
									</div>
									<div class="book-who">
										<b
											>{#each r.who.split(' vs. ') as part, i (i)}{#if i > 0}{' vs. '}{/if}{#if r.names.includes(part)}{@render name(
														part
													)}{:else}{part}{/if}{/each}</b
										><span class="book-when">{r.when}</span>
									</div>
								</div>
								{#if r.live}<Tag variant="live" size="note">{r.live}</Tag>{/if}
							</div>
						{/each}
					</div>
				</Card>
			</div>
		{/each}

		<p class="footnote">{recordFootnote(liveSeason, recordBookData.lastUpdated)}</p>
	{/if}
{/if}

<style>
	.tabs {
		margin-top: 24px;
	}

	.first {
		margin-top: 20px;
	}

	.block {
		margin-top: 20px;
	}

	.muted-line {
		margin: 0 0 14px;
		font-size: 15px;
		color: var(--color-muted);
	}

	.name-link {
		color: inherit;
	}

	.name-link:hover {
		color: var(--color-field);
	}

	/* Section titles outside a card ("Three eras", "Longest rivalries"). */
	.section {
		margin-top: 20px;
	}

	.section-title {
		margin: 0 0 12px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 28px;
	}

	/* ---------- Three eras ---------- */

	.eras {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
		gap: 16px;
	}

	.era-head {
		display: flex;
		justify-content: space-between;
		align-items: baseline;
		gap: 12px;
	}

	.era-head h3 {
		margin: 0;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 24px;
	}

	.era-span {
		font-size: 14px;
		font-weight: 600;
		color: var(--color-field);
	}

	.era-titles {
		margin-top: 14px;
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 12px 14px;
		border-radius: 10px;
		background: var(--color-pennant-tint);
		font-size: 15px;
	}

	.era-stat {
		display: flex;
		justify-content: space-between;
		gap: 12px;
		padding: 9px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
		color: var(--color-ink-2);
	}

	.era-value {
		font-weight: 600;
		color: var(--color-ink);
	}

	/* ---------- The face-off ---------- */

	.faceoff {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
		gap: 20px;
		align-items: center;
	}

	.side {
		display: flex;
		align-items: center;
		gap: 16px;
	}

	.side-b {
		flex-direction: row-reverse;
		text-align: right;
	}

	.side-line {
		margin-top: 4px;
		font-size: 14px;
		color: var(--color-on-clubhouse-2);
	}

	.side-line.lead {
		font-weight: 700;
		color: var(--color-pennant-light);
	}

	.center {
		text-align: center;
	}

	.record {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 64px;
		line-height: 1;
	}

	.record-sub {
		margin-top: 6px;
		font-size: 14px;
		color: var(--color-on-clubhouse);
	}

	.few {
		margin: 0;
		max-width: 320px;
		font-size: 16px;
		color: var(--color-on-clubhouse);
	}

	.hint {
		margin: 10px 0 0;
		text-align: center;
		font-size: 13px;
		color: var(--color-on-clubhouse-2);
	}

	.share {
		margin-top: 18px;
		height: 12px;
		border-radius: 6px;
		display: flex;
		overflow: hidden;
		background: var(--color-field-2);
	}

	.share-a {
		background: var(--color-pennant-light);
	}

	.share-b {
		background: var(--color-on-clubhouse-2);
	}

	.face-tiles {
		margin-top: 20px;
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
		gap: 12px;
	}

	.wide-tile {
		display: contents;
	}

	.tile-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 24px;
	}

	.tile-value :global(.empty) {
		color: var(--color-on-clubhouse-2);
	}

	.tile-label {
		margin-top: 2px;
		font-size: 13px;
		color: var(--color-on-clubhouse-2);
	}

	/* ---------- Longest rivalries ---------- */

	.rivals {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
		gap: 16px;
	}

	.rival {
		display: block;
		color: var(--color-ink);
	}

	.rival:hover {
		color: var(--color-ink);
	}

	.rival:hover :global(.card) {
		border-color: var(--color-field-line);
	}

	.rival:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
		border-radius: var(--radius-card);
	}

	.rival-top {
		display: flex;
		align-items: center;
		gap: 10px;
	}

	.rival-name {
		flex: 1;
		min-width: 0;
		font-weight: 700;
		font-size: 16px;
	}

	.rival-name.end {
		text-align: right;
	}

	.rival-record {
		flex: none;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 24px;
	}

	.rival-bar {
		margin-top: 12px;
		height: 8px;
		border-radius: 4px;
		display: flex;
		overflow: hidden;
	}

	.bar-a {
		background: var(--color-field);
	}

	.bar-b {
		background: var(--color-line-strong);
	}

	.rival-meta {
		margin-top: 10px;
		display: flex;
		flex-wrap: wrap;
		gap: 6px 14px;
		font-size: 13px;
		color: var(--color-ink-2);
	}

	.titles {
		display: inline-flex;
		align-items: center;
		gap: 5px;
		font-weight: 700;
		color: var(--color-pennant-dark);
	}

	/* ---------- Every rivalry and title games ---------- */

	.tables {
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: flex-start;
	}

	/* The board's bases are content-box: 640px and 320px inside each card's
	   24px sides and 1px border. */
	.every {
		flex: 999 1 690px;
		min-width: 0;
	}

	.title-games {
		flex: 1 1 370px;
		min-width: 0;
	}

	.note {
		font-size: 13px;
		color: var(--color-muted);
	}

	/* The board's minmax(190px, 1fr): at least 190px, and the rest of the row.
	   Its columns and gaps come to 866px, past the 860px minimum it sets, so
	   the table's minimum is 866px. */
	.match {
		display: block;
		min-width: 190px;
		font-weight: 600;
	}

	.match-sub {
		display: block;
		font-size: 12px;
		color: var(--color-muted);
	}

	.strong {
		font-weight: 700;
	}

	.strong-600 {
		font-weight: 600;
	}

	.leader {
		display: flex;
		align-items: center;
		gap: 8px;
		font-weight: 600;
	}

	.tied {
		font-weight: 600;
		color: var(--color-muted);
	}

	.soft {
		color: var(--color-ink-2);
	}

	.luckier {
		font-size: 14px;
	}

	.luck {
		font-weight: 700;
		color: var(--color-field);
	}

	.title-games-sub {
		margin: 4px 0 6px;
		font-size: 14px;
		color: var(--color-ink-brown);
	}

	.tg-row {
		padding: 11px 0;
		border-bottom: 1px solid var(--color-pennant-line);
	}

	.tg-top {
		display: flex;
		justify-content: space-between;
		gap: 10px;
		font-size: 15px;
	}

	.tg-count {
		flex: none;
		font-weight: 600;
		color: var(--color-pennant-dark);
	}

	.tg-key {
		margin-top: 3px;
		font-size: 13px;
		color: var(--color-ink-2);
	}

	/* ---------- Record book ---------- */

	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		align-items: center;
	}

	.count {
		margin-right: 6px;
		font-size: 14px;
		color: var(--color-muted);
	}

	.chip-count {
		opacity: 0.7;
	}

	.champ-grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
		gap: 14px;
	}

	.champ {
		display: flex;
		align-items: center;
		gap: 14px;
	}

	.champ-text {
		min-width: 0;
	}

	.champ-label {
		font-size: 13px;
		color: var(--color-on-clubhouse-2);
	}

	.champ-value {
		margin-top: 2px;
		display: flex;
		align-items: baseline;
		gap: 8px;
	}

	.champ-number {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 34px;
		line-height: 1;
	}

	.champ-unit {
		font-size: 14px;
		color: var(--color-on-clubhouse);
	}

	.champ-holder {
		margin-top: 2px;
		font-size: 14px;
		font-weight: 700;
		color: var(--color-pennant-light);
	}

	.champ-holder .name-link:hover {
		color: var(--color-white);
	}

	.group-count {
		flex: none;
		font-size: 14px;
		color: var(--color-muted);
	}

	.book-grid {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(330px, 1fr));
		gap: 0 28px;
	}

	.book-row {
		display: flex;
		gap: 14px;
		align-items: center;
		padding: 14px 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.book-text {
		flex: 1;
		min-width: 0;
	}

	.book-label {
		font-size: 13px;
		font-weight: 600;
		color: var(--color-muted);
	}

	.book-value {
		margin-top: 2px;
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 4px 8px;
	}

	.book-number {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 26px;
		line-height: 1.1;
	}

	.book-unit {
		font-size: 14px;
		color: var(--color-ink-2);
	}

	.book-who {
		margin-top: 2px;
		font-size: 14px;
	}

	.book-when {
		color: var(--color-muted);
	}

	.footnote {
		margin: 16px 0 0;
		font-size: 13px;
		color: var(--color-muted);
	}

	/* ---------- Phones (S10.10) ---------- */

	@media (max-width: 639.98px) {
		.tabs {
			margin-top: 20px;
		}

		.first {
			margin-top: 16px;
		}

		.block {
			margin-top: 16px;
		}

		.section,
		.section-gap {
			margin-top: 24px;
		}

		.section-title {
			margin-bottom: 10px;
			font-size: 22px;
		}

		.eras {
			grid-template-columns: minmax(0, 1fr);
			gap: 12px;
		}

		.era-head h3 {
			font-size: 18px;
		}

		.era-titles {
			margin-top: 12px;
		}

		.era-stat {
			font-size: 14px;
		}

		.faceoff {
			display: flex;
			flex-direction: column;
			align-items: stretch;
			gap: 16px;
			text-align: center;
		}

		.side,
		.side-b {
			flex-direction: column;
			gap: 0;
			text-align: center;
		}

		.side-text {
			width: 100%;
			margin-top: 8px;
		}

		.record {
			font-size: 56px;
		}

		.few {
			max-width: none;
		}

		.hint {
			margin-top: 12px;
		}

		.share {
			margin-top: 16px;
		}

		.face-tiles {
			margin-top: 16px;
			grid-template-columns: repeat(2, minmax(0, 1fr));
			gap: 10px;
			text-align: center;
		}

		.wide-tile {
			display: block;
			grid-column: 1 / -1;
		}

		.tile-value {
			font-size: 22px;
		}

		.rivals {
			grid-template-columns: minmax(0, 1fr);
			gap: 12px;
		}

		.rival-top {
			gap: 8px;
		}

		.rival-name {
			font-size: 15px;
			line-height: 1.25;
		}

		.rival-record {
			font-size: 22px;
		}

		.tables {
			display: block;
		}

		.title-games {
			margin-top: 16px;
		}

		.note {
			line-height: 1.45;
		}

		/* The sticky matchup keeps 10px clear of its rule, as drawn. */
		.match,
		.match-sub {
			min-width: 0;
			padding-right: 10px;
		}

		.match-sub {
			white-space: nowrap;
		}

		.champ-grid {
			grid-template-columns: minmax(0, 1fr);
			gap: 12px;
		}

		.champ-number {
			font-size: 28px;
		}

		.book-grid {
			grid-template-columns: minmax(0, 1fr);
		}

		.book-row {
			gap: 12px;
			padding: 12px 0;
		}

		.book-number {
			font-size: 22px;
		}

		.footnote {
			line-height: 1.5;
		}
	}
</style>

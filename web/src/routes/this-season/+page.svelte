<script lang="ts">
	import { onMount } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import PlayoffOdds from '$lib/components/PlayoffOdds.svelte';
	import ChatPreview from '$lib/components/chat/ChatPreview.svelte';
	import {
		Avatar,
		Button,
		Card,
		DataTable,
		EmptyValue,
		Movement,
		OddsBar,
		Pennant,
		Segmented,
		StatStrip,
		StatTile,
		StatTip,
		Tag,
		type Column
	} from '$lib/components/ui';
	import { YAHOO_LEAGUE_URL } from '$lib/config/league';
	import { profileHref } from '$lib/home';
	import { startHistory, type StartHistoryRow } from '$lib/start-history';
	import {
		championLine,
		finishGroups,
		finishTag,
		formatPoints,
		formatRecord,
		historyTile,
		moveLine,
		pageKicker,
		playoffLineLabel,
		recordOrder,
		seasonState,
		seasonStatus,
		standingsRows,
		statTiles,
		thousands,
		updatedAgo,
		type OddsResponse,
		type OverviewStatus,
		type StandingRow,
		type StandingsResponse,
		type Transaction
	} from '$lib/this-season';
	import { buildWeekNotes } from '$lib/week-notes';

	interface PowerRow {
		managerName: string;
		managerHasProfile: boolean;
		powerRank: number;
		rankChange: number | null;
	}

	interface Overview extends OverviewStatus {
		currentWeek: number;
	}

	// Everything loads client-side, as before: overview, standings, odds,
	// power rankings and transactions together, then the start history after
	// the leader's games played. The body draws once it all settles, so the
	// live layout never draws and then swaps for the final one.
	let status = $state<'loading' | 'ready' | 'error'>('loading');
	let overview = $state<Overview | null>(null);
	let standings = $state<StandingsResponse | null>(null);
	let odds = $state<OddsResponse | null>(null);
	let power = $state<{ week: number | null; rankings: PowerRow[] } | null>(null);
	let moves = $state<Transaction[]>([]);
	let history = $state<StartHistoryRow[] | null>(null);
	let now = $state(new Date());

	// The body's phone layout (below 640px) differs in markup, not just in
	// spacing, and the body only ever draws in the browser.
	const phoneQuery = new MediaQuery('max-width: 639.98px', false);
	const phone = $derived(phoneQuery.current);

	async function getJson<T>(url: string): Promise<T> {
		const res = await fetch(url);
		if (!res.ok) throw new Error(`${url}: ${res.status}`);
		return res.json();
	}

	async function load() {
		status = 'loading';
		const [ov, st, od, pw, tx] = await Promise.allSettled([
			getJson<{ overview: Overview }>('/api/leagues/overview'),
			getJson<StandingsResponse>('/api/standings'),
			getJson<OddsResponse>('/api/playoff-odds'),
			getJson<{ week: number | null; rankings: PowerRow[] }>('/api/power-rankings'),
			getJson<{ transactions: Transaction[] }>('/api/transactions?limit=8')
		]);
		for (const r of [ov, st, od, pw, tx]) {
			if (r.status === 'rejected') console.error('This Season:', r.reason);
		}
		if (st.status === 'rejected') {
			status = 'error';
			return;
		}
		standings = st.value;
		overview = ov.status === 'fulfilled' ? ov.value.overview : null;
		odds = od.status === 'fulfilled' ? od.value : null;
		power = pw.status === 'fulfilled' ? pw.value : null;
		moves = tx.status === 'fulfilled' ? (tx.value.transactions ?? []) : [];
		history = null;

		// Past records are matched after the same number of games the leader
		// has played, which is the odds' week when every week is loaded in full.
		const first = standings.standings[0];
		const played = first ? first.wins + first.losses + first.ties : 0;
		if (
			seasonState(standings) === 'live' &&
			odds?.available &&
			odds.phase === 'regular' &&
			played >= 1
		) {
			try {
				const body = await getJson<{ rows: StartHistoryRow[] }>(
					`/api/standings/start-history?week=${played}`
				);
				history = body.rows;
			} catch (err) {
				console.error('This Season:', err);
			}
		}
		now = new Date();
		status = 'ready';
	}

	onMount(load);

	/* ---------- Derived ---------- */

	const mode = $derived(seasonState(standings));
	const season = $derived(standings?.season ?? new Date().getFullYear());
	const rows = $derived(standings?.standings ?? []);
	const playoffTeams = $derived(standings?.playoffTeams ?? 6);
	const regularSeasonWeeks = $derived(standings?.regularSeasonWeeks ?? 14);
	const currentWeek = $derived(overview?.currentWeek ?? 0);
	const oddsReady = $derived(odds?.available === true ? odds : null);
	const phase = $derived(
		oddsReady?.phase ?? (overview?.playoffStatus === 'Playoffs' ? 'postseason' : 'regular')
	);
	const throughWeek = $derived(oddsReady?.throughWeek ?? 0);

	const kicker = $derived(
		pageKicker(mode, season, currentWeek, regularSeasonWeeks, seasonStatus(overview))
	);

	const leader = $derived(rows[0] ?? null);
	const tiles = $derived(
		statTiles({
			currentWeek,
			regularSeasonWeeks,
			playoffStartWeek: overview?.format?.playoffStartWeek ?? regularSeasonWeeks + 1,
			phase,
			throughWeek,
			standings: rows,
			history: history && leader ? historyTile(startHistory(history, leader), leader) : null
		})
	);

	const tableRows = $derived(standingsRows(rows, oddsReady?.teams, playoffTeams));
	const hasLastWeek = $derived(oddsReady?.teams.some((t) => t.previousOdds != null) ?? false);
	const columns = $derived<Column[]>([
		{ key: 'rank', label: '#', width: '32px' },
		{ key: 'manager', label: 'Manager' },
		{ key: 'record', label: 'W-L', numeric: true, width: '56px' },
		{ key: 'pointsFor', label: 'PF', numeric: true, width: '72px' },
		{ key: 'pointsAgainst', label: 'PA', numeric: true, width: '72px' },
		{ key: 'diff', label: 'Diff', numeric: true, width: '72px' },
		{ key: 'odds', label: 'Playoff odds', width: '260px' },
		{
			key: 'week',
			label: hasLastWeek ? 'Wk Δ' : '',
			numeric: true,
			width: '60px',
			ariaLabel: hasLastWeek ? 'Change in playoff odds since last week' : undefined
		}
	]);
	// "Updated 2 hours ago · odds from 10,000 sims"; either half alone leads.
	const standingsNote = $derived.by(() => {
		const updated = standings?.dataUpdatedAt ? updatedAgo(standings.dataUpdatedAt, now) : null;
		const sims = oddsReady ? `${thousands(oddsReady.sims)} sims` : null;
		if (updated && sims) return `${updated} · odds from ${sims}`;
		return updated ?? (sims ? `Odds from ${sims}` : '');
	});

	const powerRows = $derived(
		[...(power?.rankings ?? [])].sort((a, b) => a.powerRank - b.powerRank)
	);
	const powerByManager = $derived(new Map(powerRows.map((p) => [p.managerName, p])));
	const oddsByManager = $derived(new Map((oddsReady?.teams ?? []).map((t) => [t.managerName, t])));

	const showNotes = $derived(mode === 'live' && phase === 'regular' && throughWeek >= 1);
	const notes = $derived(
		showNotes
			? buildWeekNotes({
					week: throughWeek,
					history,
					managers: rows.map((r) => ({
						name: r.managerName,
						wins: r.wins,
						losses: r.losses,
						ties: r.ties,
						pointsFor: r.pointsFor,
						pointsAgainst: r.pointsAgainst,
						playoffOdds: oddsByManager.get(r.managerName)?.playoffOdds ?? null,
						previousOdds: oddsByManager.get(r.managerName)?.previousOdds ?? null,
						powerRank: powerByManager.get(r.managerName)?.powerRank ?? null,
						rankChange: powerByManager.get(r.managerName)?.rankChange ?? null
					}))
				})
			: []
	);

	const moveRows = $derived(
		moves.filter((m): m is Transaction & { managerName: string } => !!m.managerName).slice(0, 8)
	);

	/* ---------- After the final ---------- */

	let view = $state<'record' | 'playoff'>('record');
	const VIEWS = [
		{ value: 'record' as const, label: 'Record' },
		{ value: 'playoff' as const, label: 'Playoff finish' }
	];

	const seeded = $derived(recordOrder(rows));
	const seedOf = (r: StandingRow) => {
		const seed = seeded.findIndex((s) => s.managerName === r.managerName) + 1;
		return { ...r, seed };
	};
	const champion = $derived(rows.find((r) => r.playoffTier === 'Champion') ?? null);
	const runnerUp = $derived(rows.find((r) => r.playoffTier === 'Runner-up') ?? null);
	const champLine = $derived(
		champion ? championLine(seedOf(champion), runnerUp ? seedOf(runnerUp) : null) : ''
	);
	const groups = $derived(finishGroups(rows));
	const GROUP_TONE: Record<string, string> = {
		Championship: 'var(--color-pennant-dark)',
		Semifinalists: 'var(--color-field)',
		Quarterfinalists: 'var(--color-field)',
		'Consolation bracket': 'var(--color-ink-2)'
	};

	const record = (r: { wins: number; losses: number; ties: number }) =>
		formatRecord(r.wins, r.losses, r.ties);
</script>

<svelte:head>
	<title>This Season · The League</title>
</svelte:head>

{#snippet yahoo()}
	<Button variant="secondary" href={YAHOO_LEAGUE_URL} target="_blank" rel="noopener noreferrer"
		>Open in Yahoo ↗</Button
	>
{/snippet}

{#if status !== 'ready'}
	<PageHeader title="This Season" pending={status === 'loading'} />
{:else if mode === 'final'}
	<PageHeader kicker={kicker.desktop} phoneKicker={kicker.phone} title="This Season">
		Final standings and how the bracket played out. The page switches back to live standings when
		the next season starts.
	</PageHeader>
{:else}
	<PageHeader
		kicker={kicker.desktop}
		phoneKicker={kicker.phone}
		title="This Season"
		hideActionOnPhone
		action={yahoo}
	>
		Live standings, matchups and the playoff picture as the season plays out.
	</PageHeader>
{/if}

{#if status === 'loading'}
	<div class="skeleton" aria-busy="true" aria-label="Loading this season">
		<div class="sk-card sk-strip">
			{#each Array(4) as _, i (i)}<span class="sk-tile"
					><span class="sk-bar short"></span><span class="sk-bar tall"></span></span
				>{/each}
		</div>
		<div class="sk-card">
			{#each Array(10) as _, i (i)}<span class="sk-bar"></span>{/each}
		</div>
	</div>
{:else if status === 'error'}
	<div class="first">
		<Card title="This season didn't load">
			<p class="muted-line">The standings didn't come back from the server.</p>
			<Button variant="secondary" class="retry" onclick={load}>Try again</Button>
		</Card>
	</div>
{:else if mode === 'final'}
	<!-- S4.8 The champion -->
	{#if champion}
		<div class="first">
			<Card tone="clubhouse" padding="26px 28px" phonePadding="18px 16px">
				<div class="champion">
					<Avatar
						name={champion.managerName}
						size={phone ? 72 : 104}
						ring={4}
						ringColor="var(--color-pennant-light)"
					/>
					<div class="champ-text">
						<div class="champ-kicker">
							<Pennant
								staff="var(--color-white)"
								fill="var(--color-pennant-light)"
								width={14}
								height={20}
							/>{season} champion
						</div>
						<div class="champ-name">{champion.managerName}</div>
						<div class="champ-line">{champLine}</div>
					</div>
				</div>
			</Card>
		</div>
	{/if}

	<div class="row">
		<div class="final-standings">
			<Card
				title="Final standings"
				headAlign="center"
				headSpacing="10px 16px"
				headGap={0}
				padding="22px 24px 12px"
				phonePadding="16px 16px 6px"
				phoneHeadStack
				phoneHeadStackGap={10}
			>
				{#snippet action()}
					<Segmented label="Order standings by" size="toggle" options={VIEWS} bind:value={view} />
				{/snippet}
				<p class="view-note">
					{view === 'record'
						? 'Regular season record, with points for as the tiebreaker.'
						: 'Where everyone finished once the bracket played out.'}
				</p>

				{#if view === 'record'}
					<div class="final-table record-view" role="table" aria-label="Final standings by record">
						<div class="final-row head" role="row">
							<span role="columnheader">#</span>
							<span role="columnheader" class="manager-head">Manager</span>
							<span role="columnheader" class="finish-head">Finish</span>
							<span role="columnheader" class="num">W-L</span>
							<span role="columnheader" class="num">PF</span>
							<span role="columnheader" class="num pa">PA</span>
						</div>
						{#each seeded as r, i (r.managerName)}
							{@const tag = finishTag(r.playoffTier)}
							{#if i === playoffTeams && seeded.length > playoffTeams}
								<div class="final-line" role="presentation">
									<span class="rule"></span>Playoff line<span class="rule"></span>
								</div>
							{/if}
							<div class="final-row" role="row">
								<span role="cell" class="final-rank" class:below={i >= playoffTeams}>{i + 1}</span>
								<span role="cell" class="manager"
									><Avatar name={r.managerName} size={phone ? 28 : 30} /><span class="name"
										>{r.managerName}</span
									></span
								>
								<span role="cell" class="finish"
									>{#if tag}<Tag variant={tag.variant} size="row">{tag.text}</Tag>{/if}</span
								>
								<span role="cell" class="num strong">{record(r)}</span>
								<span role="cell" class="num">{formatPoints(r.pointsFor)}</span>
								<span role="cell" class="num pa muted">{formatPoints(r.pointsAgainst)}</span>
							</div>
						{/each}
					</div>
				{:else}
					<div class="final-table playoff-view">
						{#each groups as group (group.name)}
							<h3 class="group" style:color={GROUP_TONE[group.name]}>{group.name}</h3>
							<ul>
								{#each group.rows as r (r.managerName)}
									{@const tag = finishTag(r.playoffTier)}
									<li class="final-row">
										<span class="final-rank">{r.rank}</span>
										<span class="manager"
											><Avatar name={r.managerName} size={phone ? 28 : 30} /><span class="name"
												>{r.managerName}</span
											></span
										>
										<span class="finish"
											>{#if tag}<Tag variant={tag.variant} size="row">{tag.text}</Tag>{/if}</span
										>
										<span class="num strong">{record(r)}</span>
										<span class="num pf-note">{formatPoints(r.pointsFor)} PF</span>
									</li>
								{/each}
							</ul>
						{/each}
					</div>
				{/if}
			</Card>
		</div>
		<div class="field">
			<PlayoffOdds data={odds} />
		</div>
	</div>
{:else}
	<!-- S4.2 The stat strip -->
	<div class="strip">
		<StatStrip label="This season at a glance" twoUpOnPhones>
			{#each tiles as tile (tile.label)}
				{#if phone}
					<StatTile label={tile.phone} value={tile.value} compact />
				{:else}
					<StatTile label={tile.label} value={tile.value} sub={tile.sub} />
				{/if}
			{/each}
		</StatStrip>
	</div>

	<!-- S4.3 Standings -->
	<div class="stack">
		<Card title="Standings" padding="22px 24px 12px" phoneHeadGap={0}>
			{#snippet action()}
				{#if standingsNote && !phone}<span class="meta">{standingsNote}</span>{/if}
			{/snippet}
			{#if rows.length === 0}
				<p class="muted-line">No games yet</p>
			{:else if phone}
				<ul class="phone-standings">
					{#each tableRows as r (r.managerName)}
						<li>
							<span class="p-rank">{r.rank}</span>
							<a class="p-manager" href={profileHref(r.managerName)}
								><Avatar name={r.managerName} size={26} /><span class="name">{r.managerName}</span
								></a
							>
							<span class="strong">{r.record}</span>
							<span class="p-odds">{r.oddsText ?? ''}</span>
						</li>
					{/each}
				</ul>
			{:else}
				<DataTable
					{columns}
					rows={tableRows}
					label="Standings"
					rowPadding={12}
					columnGap={14}
					minWidth="700px"
					rowKey={(r) => r.managerName}
					divider={(_, i) =>
						i === playoffTeams && tableRows.length > playoffTeams
							? playoffLineLabel(playoffTeams, oddsReady?.byes)
							: null}
				>
					{#snippet header(column)}
						{#if column.key === 'odds'}
							<StatTip metricId="playoff_probability" label="Playoff odds" />
						{:else if column.key === 'week'}
							<span aria-hidden="true">{column.label}</span>
						{:else}
							{column.label}
						{/if}
					{/snippet}
					{#snippet cell(r, column)}
						{#if column.key === 'rank'}
							<span class="rank" class:below={!r.aboveLine}>{r.rank}</span>
						{:else if column.key === 'manager'}
							<a class="manager-link" href={profileHref(r.managerName)}
								><Avatar name={r.managerName} size={28} />{r.managerName}</a
							>
						{:else if column.key === 'record'}
							<span class="strong">{r.record}</span>
						{:else if column.key === 'pointsAgainst'}
							<span class="muted">{r.pointsAgainst}</span>
						{:else if column.key === 'diff'}
							<span class:negative={r.diffNegative}>{r.diff}</span>
						{:else if column.key === 'odds'}
							{#if r.oddsPercent === null}
								<EmptyValue />
							{:else}
								<OddsBar
									percent={r.oddsPercent}
									tone={r.aboveLine ? 'field' : 'sage'}
									text={r.oddsText ?? undefined}
									numberWidth={38}
								/>
							{/if}
						{:else if column.key === 'week'}
							{#if r.weekChange !== null}<span class="small-move"
									><Movement change={r.weekChange} /></span
								>{/if}
						{:else}
							{r[column.key]}
						{/if}
					{/snippet}
				</DataTable>
			{/if}
		</Card>
	</div>

	<div class="row">
		<div class="column left">
			<!-- S4.4 Playoff Odds -->
			<PlayoffOdds data={odds} />

			<!-- S4.5 Recent moves -->
			<Card
				title="Recent moves"
				headGap={6}
				phonePadding="6px 16px 16px"
				phoneHeadGap={0}
				phoneHeadAlign="center"
			>
				{#snippet action()}
					<!-- D5: Yahoo's league slug has no /transactions page (it 404s), so
					     this opens the league, where the transactions list is a tab. -->
					<a
						class="field-link card-link"
						href={YAHOO_LEAGUE_URL}
						target="_blank"
						rel="noopener noreferrer">All transactions →</a
					>
				{/snippet}
				{#if moveRows.length === 0}
					<p class="muted-line">No moves yet this season.</p>
				{:else}
					<ul class="moves">
						{#each moveRows as tx, i (i)}
							{@const line = moveLine(tx)}
							<li>
								<Avatar name={tx.managerName} size={26} />
								<span class="move-text"
									><a class="move-name" href={profileHref(tx.managerName)}>{tx.managerName}</a>
									<span class="verb">{line.verb}</span>
									{line.player}</span
								>
								<Tag variant={line.tone} size="row">{line.tag}</Tag>
							</li>
						{/each}
					</ul>
				{/if}
			</Card>
		</div>

		<div class="column right">
			<!-- S4.6 Notes from week N -->
			{#if showNotes && notes.length > 0}
				<Card tone="pennant" title="Notes from week {throughWeek}" headGap={6} phoneHeadGap={2}>
					{#each notes as note (note.id)}
						<p class="note">{note.text}</p>
					{/each}
				</Card>
			{/if}

			<!-- S4.7 Power Rankings -->
			<Card title="Power Rankings" headGap={6} phonePadding="16px 16px 6px" phoneHeadGap={2}>
				{#snippet action()}
					{#if power?.week}<span class="meta wk">Wk {power.week}</span>{/if}
				{/snippet}
				{#if powerRows.length === 0}
					<p class="muted-line">Power rankings aren't available for this season yet.</p>
				{:else}
					<ul class="power">
						{#each powerRows as p (p.managerName)}
							<li>
								<span class="power-rank">{p.powerRank}</span>
								{#if p.managerHasProfile}
									<a class="power-name" href={profileHref(p.managerName)}>{p.managerName}</a>
								{:else}
									<span class="power-name">{p.managerName}</span>
								{/if}
								{#if p.rankChange !== null}<span class="small-move"
										><Movement change={p.rankChange} /></span
									>{/if}
							</li>
						{/each}
					</ul>
				{/if}
				<div class="full-link">
					<Button variant="link" size="tap" href="/power-rankings">Full rankings →</Button>
				</div>
			</Card>

			<!-- S4.7 League chat -->
			<ChatPreview />
		</div>
	</div>
{/if}

<style>
	/* ---------- Layout (S4.1) ---------- */
	.strip,
	.first {
		margin-top: 28px;
	}

	.stack,
	.row {
		margin-top: 20px;
	}

	.row {
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: flex-start;
	}

	.column {
		display: flex;
		flex-direction: column;
		gap: 20px;
		min-width: 0;
	}

	.left {
		flex: 1.6 1 520px;
	}

	.right {
		flex: 1 1 320px;
	}

	/* The board's bases (600px and 320px) are content-box on cards with 24px
	   of side padding and a 1px border; these wrappers are border-box, so they
	   take the cards' outer widths. */
	.final-standings {
		flex: 999 1 650px;
		min-width: 0;
	}

	.field {
		flex: 1 1 370px;
		min-width: 0;
	}

	ul {
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.meta {
		font-size: 13px;
		color: var(--color-muted);
	}

	.muted-line {
		margin: 0;
		font-size: 15px;
		color: var(--color-muted);
	}

	.strong {
		font-weight: 600;
	}

	.muted {
		color: var(--color-muted);
	}

	.negative {
		color: var(--color-pennant);
	}

	.small-move {
		font-size: 13px;
	}

	.first :global(.retry) {
		margin-top: 14px;
	}

	/* ---------- Loading ---------- */
	.skeleton {
		margin-top: 28px;
		display: grid;
		gap: 20px;
	}

	.sk-card {
		display: grid;
		gap: 25px;
		padding: 22px 24px;
		background: var(--color-card);
		border: 1px solid var(--color-line);
		border-radius: var(--radius-card);
	}

	.sk-strip {
		grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
		gap: 20px 44px;
	}

	.sk-tile {
		display: grid;
		gap: 10px;
	}

	.sk-bar.short {
		width: 60%;
		height: 14px;
	}

	.sk-bar.tall {
		height: 38px;
	}

	.sk-bar {
		display: block;
		height: 28px;
		border-radius: 6px;
		background: var(--color-line-soft);
	}

	/* ---------- Standings (S4.3) ---------- */
	.rank {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 19px;
		color: var(--color-ink);
	}

	.rank.below {
		color: var(--color-muted);
	}

	.manager-link {
		display: flex;
		align-items: center;
		gap: 10px;
		font-weight: 600;
	}

	/* ---------- Recent moves (S4.5) ---------- */
	.card-link {
		font-size: 14px;
		font-weight: 600;
	}

	.moves li {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 9px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	.move-text {
		flex: 1;
		min-width: 0;
	}

	.move-name {
		font-weight: 700;
	}

	.verb {
		color: var(--color-ink-2);
	}

	/* ---------- Notes (S4.6) ---------- */
	.note {
		margin: 0;
		padding: 12px 0;
		border-bottom: 1px solid var(--color-pennant-line);
		font-size: 16px;
		line-height: 1.45;
	}

	/* ---------- Power Rankings (S4.7) ---------- */
	.power li {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 8px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	.power-rank {
		width: 22px;
		flex: none;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 18px;
		color: var(--color-ink-2);
	}

	.power-name {
		flex: 1;
		font-weight: 600;
	}

	.full-link {
		margin-top: 4px;
	}

	/* ---------- The champion (S4.8) ---------- */
	.champion {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 20px 28px;
	}

	.champ-text {
		flex: 1 1 300px;
		min-width: 0;
	}

	.champ-kicker {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-pennant-light);
	}

	.champ-name {
		margin-top: 6px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 48px;
		line-height: 1;
	}

	.champ-line {
		margin-top: 8px;
		font-size: 16px;
		color: var(--color-on-clubhouse);
	}

	/* ---------- Final standings (S4.9) ---------- */
	.view-note {
		margin: 6px 0 0;
		font-size: 13px;
		color: var(--color-muted);
	}

	.final-row {
		display: grid;
		gap: 0 14px;
		align-items: center;
		padding: 10px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	.record-view .final-row {
		grid-template-columns: 32px minmax(0, 1fr) auto 56px 76px 76px;
	}

	.playoff-view .final-row {
		grid-template-columns: 32px minmax(0, 1fr) auto 64px 80px;
	}

	.final-row.head {
		margin-top: 10px;
		padding: 8px 0;
		border-bottom: 2px solid var(--color-ink);
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-ink-2);
	}

	.num {
		text-align: right;
	}

	.final-rank {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 19px;
		color: var(--color-ink);
	}

	.final-rank.below {
		color: var(--color-muted);
	}

	.manager {
		display: flex;
		align-items: center;
		gap: 10px;
		min-width: 0;
		font-weight: 600;
	}

	.pf-note {
		color: var(--color-ink-2);
	}

	.final-line {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 6px 0;
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-field);
	}

	.final-line .rule {
		flex: 1;
		border-top: 2px solid var(--color-field);
	}

	.group {
		margin: 0;
		padding: 16px 0 4px;
		font-family: var(--font-sans);
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
	}

	/* ---------- Phones (S4.10) ---------- */
	@media (max-width: 639.98px) {
		.strip,
		.first,
		.stack,
		.row {
			margin-top: 14px;
		}

		.row,
		.column {
			gap: 14px;
		}

		.skeleton {
			margin-top: 14px;
			gap: 14px;
		}

		.sk-card {
			padding: 16px;
		}

		.sk-strip {
			grid-template-columns: repeat(2, minmax(0, 1fr));
			gap: 16px;
		}

		.sk-bar.tall {
			height: 24px;
		}

		.phone-standings li {
			display: flex;
			align-items: center;
			gap: 10px;
			border-bottom: 1px solid var(--color-line-soft);
			font-size: 14px;
		}

		.p-rank {
			width: 16px;
			flex: none;
			font-family: var(--font-slab);
			font-weight: 700;
		}

		.p-manager {
			flex: 1;
			min-width: 0;
			min-height: 44px;
			display: flex;
			align-items: center;
			gap: 10px;
			font-weight: 600;
		}

		.p-odds {
			width: 40px;
			text-align: right;
			font-weight: 700;
			color: var(--color-field);
		}

		.card-link {
			display: inline-flex;
			align-items: center;
			min-height: 44px;
		}

		.moves li {
			gap: 10px;
			font-size: 14px;
			line-height: 1.4;
		}

		.note {
			font-size: 15px;
		}

		.power li {
			gap: 10px;
			padding: 0;
			font-size: 14px;
		}

		.power-rank {
			width: 20px;
			font-size: 16px;
		}

		.power-name {
			min-width: 0;
			min-height: 44px;
			display: flex;
			align-items: center;
		}

		.full-link {
			margin-top: 2px;
		}

		.champion {
			flex-direction: column;
			align-items: flex-start;
			gap: 14px;
		}

		.champ-text {
			flex: none;
		}

		.champ-kicker {
			font-size: 12px;
		}

		.champ-name {
			font-size: 36px;
		}

		.champ-line {
			font-size: 15px;
			line-height: 1.5;
		}

		.view-note {
			margin-top: 8px;
			line-height: 1.45;
		}

		.final-row {
			gap: 0 10px;
			font-size: 14px;
		}

		.record-view .final-row {
			grid-template-columns: 24px 28px minmax(0, 1fr) 36px 60px;
		}

		.playoff-view .final-row {
			grid-template-columns: 24px 28px minmax(0, 1fr) 36px;
		}

		/* The photo and the name take their own columns, and the Finish tag
		   drops to a second line under the name. */
		.final-row .manager {
			display: contents;
		}

		.final-row .name {
			min-width: 0;
			font-weight: 600;
		}

		.final-row .finish {
			grid-row: 2;
			grid-column: 3 / -1;
			justify-self: start;
			margin-top: 4px;
		}

		.final-rank {
			font-size: 18px;
		}

		.final-row.head .manager-head {
			grid-column: 2 / 4;
		}

		.final-row.head .finish-head,
		.pa,
		.pf-note {
			display: none;
		}
	}
</style>

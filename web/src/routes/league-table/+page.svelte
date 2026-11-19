<script lang="ts">
	/**
	 * The League Table (WP14): every manager on every career number the site
	 * tracks, one sortable table, eight column groups. All the rules (columns,
	 * sorting, ranks, leaders, formats, the URL) are in $lib/league-table; this
	 * page loads the three APIs, keeps the state in the URL and draws the rows.
	 * A span of seasons (`?from=2010&to=2020`) loads every manager's seasons
	 * once, from /api/managers/seasons, and adds the span up in the browser.
	 */
	import { onMount } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import { page } from '$app/state';
	import { afterNavigate, replaceState } from '$app/navigation';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import {
		Avatar,
		Button,
		Card,
		DataTable,
		Segmented,
		Select,
		StatTip,
		Tag,
		type Column
	} from '$lib/components/ui';
	import { profileHref, type DraftSeasonScore } from '$lib/utils/managers';
	import {
		COLUMNS,
		GROUPS,
		buildRangeRows,
		buildRows,
		caption,
		clickSort,
		filterRows,
		groupById,
		groupColumns,
		leadColumns,
		leaders,
		parseState,
		phoneColumns,
		pickSeason,
		rangeLabel,
		rankRows,
		seasonRange,
		seasonSpan,
		signTone,
		sortLine,
		sortRows,
		stateQuery,
		switchGroup,
		tableMinWidth,
		type Display,
		type GroupId,
		type LeagueColumn,
		type LeagueRow,
		type SeasonsPayload,
		type TableState,
		type Who
	} from '$lib/league-table';

	let { data } = $props();

	const phone = new MediaQuery('max-width: 639.98px');

	let rows = $state<LeagueRow[]>([]);
	let loading = $state(true);
	let failed = $state(false);
	let view = $state<TableState>(parseState(page.url.searchParams));
	/** The draft's season scores and spread, for grading a span of seasons. */
	let draftSeasons = $state<{ bySeason: DraftSeasonScore[]; spread: number | null }>({
		bySeason: [],
		spread: null
	});
	/** Every manager's seasons, loaded the first time a span is picked. */
	let seasons = $state<SeasonsPayload | null>(null);
	let seasonsLoading = $state(false);
	let seasonsFailed = $state(false);

	async function loadTable() {
		loading = true;
		failed = false;
		try {
			const [performance, draft, hallOfFame] = await Promise.all([
				fetch('/api/managers/performance').then((r) => {
					if (!r.ok) throw new Error(`performance ${r.status}`);
					return r.json();
				}),
				// The draft and Hall of Fame numbers are a column or two each: if
				// either fails, those cells read blank rather than the page failing.
				fetch('/api/draft?analysis=all')
					.then((r) => (r.ok ? r.json() : null))
					.catch(() => null),
				fetch('/api/hall-of-fame')
					.then((r) => (r.ok ? r.json() : null))
					.catch(() => null)
			]);
			const grades = draft?.data?.draftGrades ?? draft?.draftGrades;
			draftSeasons = {
				bySeason: grades?.bySeason ?? [],
				spread: typeof grades?.spread === 'number' ? grades.spread : null
			};
			rows = buildRows(
				performance?.data?.performance ?? [],
				{ byManager: grades?.byManager ?? [], bySeason: grades?.bySeason ?? [] },
				// null, not [], when it didn't load: its columns then read "None",
				// not "Under 3 seasons".
				Array.isArray(hallOfFame?.hallOfFame) ? hallOfFame.hallOfFame : null
			);
		} catch (error) {
			console.error('Error loading the league table:', error);
			failed = true;
		} finally {
			loading = false;
		}
	}

	async function loadSeasons() {
		seasonsLoading = true;
		seasonsFailed = false;
		try {
			const response = await fetch('/api/managers/seasons');
			if (!response.ok) throw new Error(`seasons ${response.status}`);
			seasons = (await response.json()).data as SeasonsPayload;
		} catch (error) {
			console.error('Error loading the seasons:', error);
			seasonsFailed = true;
		} finally {
			seasonsLoading = false;
		}
	}

	onMount(loadTable);

	// Back and Forward, or a link to another view of this page.
	afterNavigate(({ to }) => {
		if (to?.url) view = parseState(to.url.searchParams);
	});

	/** A new view, written to the URL in place (no history entry). */
	function show(next: TableState) {
		view = next;
		replaceState(`${page.url.pathname}${stateQuery(next)}`, page.state);
	}

	const span = $derived(seasonSpan(rows));
	const range = $derived(span ? seasonRange(view, span) : null);
	/** A span narrower than all time is picked. */
	const spanned = $derived(range != null && !range.all);

	// A span needs every manager's seasons: load them once, the first time.
	$effect(() => {
		if (spanned && !seasons && !seasonsLoading && !seasonsFailed) loadSeasons();
	});

	// The span's two ends as numbers: a derived number that comes out the same
	// stops there, so a sort click or a group switch doesn't rebuild the rows.
	const spanFrom = $derived(spanned && range ? range.from : null);
	const spanTo = $derived(spanned && range ? range.to : null);
	/** The career rows, or the picked span's. */
	const viewRows = $derived(
		spanFrom != null && spanTo != null
			? seasons
				? buildRangeRows(rows, seasons, draftSeasons, { from: spanFrom, to: spanTo })
				: []
			: rows
	);

	const group = $derived(groupById(view.group));
	const sortColumn = $derived(COLUMNS[view.sort]);
	const shown = $derived(filterRows(viewRows, view.who));
	const ranked = $derived(rankRows(sortRows(shown, sortColumn, view.dir), sortColumn));
	const columns = $derived(phone.current ? phoneColumns(group, view.sort) : groupColumns(group));
	const leaderSets = $derived(new Map(columns.map((c) => [c.key, leaders(shown, c)])));

	/** Every season the league has played, newest first, for the two pickers. */
	const seasonOptions = $derived(
		span
			? Array.from({ length: span.latest - span.first + 1 }, (_, i) => {
					const year = String(span.latest - i);
					return { value: year, label: year };
				})
			: []
	);

	function onSeason(end: 'from' | 'to', event: Event) {
		if (!span) return;
		const year = Number((event.currentTarget as HTMLSelectElement).value);
		show(pickSeason(view, end, year, span));
	}

	const lead = $derived(leadColumns(phone.current));
	const tableColumns = $derived<Column[]>([
		{ key: 'rank', label: '#', width: `${lead.rank}px` },
		{ key: 'manager', label: 'Manager', width: `${lead.manager}px`, gap: lead.gap },
		...columns.map((c) => ({
			key: c.key,
			label: c.label,
			numeric: true,
			width: `${c.width}px`,
			wrapHeader: true
		}))
	]);

	const tableRows = $derived(
		ranked.map((r) => ({ id: r.row.name, rank: r.rank, tied: r.tied, row: r.row }))
	);

	const groupOptions = GROUPS.map((g) => ({ value: g.id, label: g.label }));
	const whoOptions: { value: Who; label: string }[] = [
		{ value: 'all', label: 'All managers' },
		{ value: 'active', label: 'Active only' }
	];

	const isMine = (name: string) => data.userManagerName != null && name === data.userManagerName;

	function valueClass(column: LeagueColumn, row: LeagueRow, display: Display): string {
		const sorted = column.key === view.sort;
		if (display.blank) return 'blank';
		if (leaderSets.get(column.key)?.has(row.name)) return 'leader';
		const weight = sorted ? 'w700' : '';
		if (display.tone === 'signed') {
			const tone = signTone(display);
			// Zero reads as a plain value: ink 700 when sorted, ink-2 otherwise.
			if (tone === 'zero') return sorted ? 'sorted' : 'plain';
			return `${tone} signed ${weight}`;
		}
		if (display.tone !== 'plain') return `tier tier-${display.tone}`;
		return sorted ? 'sorted' : 'plain';
	}

	const footnote =
		'Career totals count every game, the season in progress too. Points per season, title games, playoff trips, swing, best and worst season and the Hall of Fame index count finished seasons only. Ties share a rank, and a blank value sorts last.';
	const spanNote =
		'With a span of seasons picked, every number counts only those seasons, and a manager who played none of them drops off the table. The Hall of Fame index is a whole-career number, so it reads All time only.';
</script>

<svelte:head>
	<title>League Table · The League</title>
</svelte:head>

<PageHeader
	kicker={span && range
		? range.all
			? `Rankings · all time · ${span.first} to ${span.latest}`
			: `Rankings · ${rangeLabel(range)}`
		: undefined}
	phoneKicker={range ? `Rankings · ${rangeLabel(range)}` : 'Rankings · all time'}
	title="League Table"
	introWidth={640}
	pending={loading && rows.length === 0}
>
	Every manager on every career number the site tracks. Pick a group of columns, then sort by any of
	them.
</PageHeader>

<div class="controls">
	{#if phone.current}
		<Select
			label="Columns"
			stacked
			options={groupOptions}
			value={view.group}
			onchange={(event) =>
				show(switchGroup(view, (event.currentTarget as HTMLSelectElement).value as GroupId))}
		/>
		<div class="who">
			<Segmented
				label="Managers"
				options={whoOptions}
				value={view.who}
				phoneColumns={2}
				onchange={(who) => show({ ...view, who })}
			/>
		</div>
		{#if range}
			<div class="seasons" role="group" aria-label="Seasons">
				<Select
					label="From"
					stacked
					options={seasonOptions}
					value={String(range.from)}
					onchange={(event) => onSeason('from', event)}
				/>
				<Select
					label="To"
					stacked
					options={seasonOptions}
					value={String(range.to)}
					onchange={(event) => onSeason('to', event)}
				/>
			</div>
		{/if}
	{:else}
		<Segmented
			label="Columns"
			options={groupOptions}
			value={view.group}
			wrap
			onchange={(id) => show(switchGroup(view, id))}
		/>
		<div class="filters">
			<Segmented
				label="Managers"
				options={whoOptions}
				value={view.who}
				onchange={(who) => show({ ...view, who })}
			/>
			{#if range}
				<div class="seasons" role="group" aria-label="Seasons">
					<Select
						label="Seasons"
						aria-label="First season"
						options={seasonOptions}
						value={String(range.from)}
						onchange={(event) => onSeason('from', event)}
					/>
					<Select
						label="to"
						aria-label="Last season"
						options={seasonOptions}
						value={String(range.to)}
						onchange={(event) => onSeason('to', event)}
					/>
				</div>
			{/if}
		</div>
	{/if}
</div>

{#snippet header(column: Column)}
	{@const c = COLUMNS[column.key]}
	{#if !c}
		{column.label}
	{:else if c.better == null}
		{c.label}
	{:else}
		{@const on = view.sort === c.key}
		<span class="head" class:on>
			<button type="button" class="sort" onclick={() => show(clickSort(view, c.key))}
				>{c.label}</button
			>{#if c.metricId}<span class="i-gap">{' '}</span><StatTip
					metricId={c.metricId}
					label={c.label}
					iconOnly
					phoneTap="inline-small"
					headerIcon
				/>{/if}{#if on}<span class="arrow" aria-hidden="true"
					>{' '}{view.dir === 'desc' ? '▼' : '▲'}</span
				>{/if}
		</span>
	{/if}
{/snippet}

{#snippet cell(r: Record<string, unknown>, column: Column)}
	{@const entry = r as unknown as (typeof tableRows)[number]}
	{#if column.key === 'rank'}
		{#if entry.rank != null}<span class="rank"
				>{#if entry.tied}<span class="t">T</span>{/if}{entry.rank}</span
			>{/if}
	{:else if column.key === 'manager'}
		<a class="who-link" href={profileHref(entry.row.name)}>
			<Avatar
				name={entry.row.name}
				size={phone.current ? 24 : 30}
				initialSize={phone.current ? 10 : 12}
				alt=""
			/>
			<span class="who-text">
				<span class="who-name"
					>{entry.row.name}{#if isMine(entry.row.name)}<Tag variant="playoffs" size="name">You</Tag
						>{/if}{#if entry.row.alum}<Tag variant="pending" size="name">League alum</Tag
						>{/if}</span
				>
				{#if !phone.current && entry.row.firstSeason != null && entry.row.lastSeason != null}
					<span class="years"
						>{entry.row.firstSeason === entry.row.lastSeason
							? entry.row.firstSeason
							: `${entry.row.firstSeason} to ${entry.row.lastSeason}`}</span
					>
				{/if}
			</span>
		</a>
	{:else}
		{@const c = COLUMNS[column.key]}
		{@const display = c.display(entry.row)}
		<span class="value {valueClass(c, entry.row, display)}">{display.text}</span>
	{/if}
{/snippet}

{#if loading && rows.length === 0}
	<div class="skeleton" role="status" aria-busy="true" aria-label="Loading the league table"></div>
{:else if failed}
	<div class="state">
		<Card title="The league table didn't load">
			<p class="muted-line">Check your connection, then try again.</p>
			<Button variant="secondary" onclick={loadTable}>Try again</Button>
		</Card>
	</div>
{:else if spanned && seasonsFailed}
	<div class="state">
		<Card title="Those seasons didn't load">
			<p class="muted-line">Check your connection, then try again.</p>
			<Button variant="secondary" onclick={loadSeasons}>Try again</Button>
		</Card>
	</div>
{:else if spanned && !seasons}
	<div class="skeleton" role="status" aria-busy="true" aria-label="Loading those seasons"></div>
{:else if rows.length === 0}
	<div class="state">
		<Card title="No managers yet">
			<p class="muted-line">Managers show up here once a season is loaded.</p>
		</Card>
	</div>
{:else}
	<div class="table-card">
		<Card
			title={group.label}
			padding="22px 24px 10px"
			phonePadding="16px 0 6px"
			headGap={4}
			phoneHeadGap={8}
			headSpacing="6px 16px"
			phoneHeadStack
			phoneHeadInset={16}
		>
			{#snippet action()}<span class="sort-line"
					>{sortLine(shown.length, sortColumn, view.dir)}</span
				>{/snippet}
			{#if !phone.current}<p class="description">{group.description}</p>{/if}
			<div class="scroller">
				<!-- A scrolling region on phones, focusable so the keyboard can scroll it. -->
				<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
				<div
					class="region"
					role={phone.current ? 'region' : undefined}
					tabindex={phone.current ? 0 : undefined}
					aria-label={phone.current ? `${group.label} table, scrolls sideways` : undefined}
				>
					<DataTable
						columns={tableColumns}
						rows={tableRows}
						caption={caption(group, sortColumn)}
						rowKey={(r) => r.id}
						rowPadding={phone.current ? 10 : 11}
						columnGap={12}
						fontSize={phone.current ? 14 : 15}
						minWidth="{tableMinWidth(columns, phone.current)}px"
						headerAlign="bottom"
						stickyLead="phone"
						leadWidth={leadColumns(true).rank}
						scroll={!phone.current}
						bleed={phone.current ? undefined : 8}
						mine={(r) => isMine(r.row.name)}
						ariaSort={(column) => {
							const c = COLUMNS[column.key];
							if (!c || c.better == null) return undefined;
							if (c.key !== view.sort) return 'none';
							return view.dir === 'desc' ? 'descending' : 'ascending';
						}}
						{header}
						{cell}
					/>
				</div>
				{#if phone.current}<span class="mask" aria-hidden="true"></span><span
						class="fade"
						aria-hidden="true"
					></span>{/if}
			</div>
		</Card>
	</div>
	{#if phone.current}
		<p class="note first">
			Swipe the table sideways for more columns. Tap a column name to sort by it, and tap again to
			flip the order.
		</p>
		<p class="note">{footnote}</p>
		{#if spanned}<p class="note">{spanNote}</p>{/if}
	{:else}
		<p class="note">{footnote}</p>
		{#if spanned}<p class="note">{spanNote}</p>{/if}
	{/if}
{/if}

<style>
	.controls {
		margin-top: 28px;
		display: flex;
		flex-wrap: wrap;
		gap: 12px 20px;
		justify-content: space-between;
		align-items: center;
	}

	.table-card {
		margin-top: 16px;
	}

	.filters {
		display: flex;
		flex-wrap: wrap;
		gap: 12px 20px;
		align-items: center;
	}

	.seasons {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.sort-line {
		font-size: 13px;
		color: var(--color-muted);
	}

	.description {
		margin: 0;
		font-size: 14px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	.scroller {
		position: relative;
		margin-top: 12px;
	}

	.note {
		margin: 14px 0 0;
		max-width: 820px;
		font-size: 13px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	/* Header: the label is a sort button, the ⓘ its own button, the arrow
	   after both. */
	.head {
		display: inline-block;
		text-align: right;
		line-height: 1.25;
	}

	.sort {
		all: unset;
		position: relative;
		cursor: pointer;
	}

	.sort:hover {
		text-decoration: underline;
	}

	.sort:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
		border-radius: 2px;
	}

	.head.on {
		color: var(--color-ink);
	}

	.arrow {
		color: var(--color-field);
	}

	/* The space before the ⓘ, which the board sets in the ⓘ's 600. */
	.i-gap {
		font-weight: 600;
	}

	.rank {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 20px;
	}

	.t {
		font-family: var(--font-sans);
		font-size: 12px;
		font-weight: 700;
		color: var(--color-muted);
	}

	.who-link {
		position: relative;
		display: flex;
		align-items: center;
		gap: 10px;
		min-width: 0;
		font-weight: 600;
		color: var(--color-ink);
		text-decoration: none;
	}

	.who-link:hover {
		color: var(--color-field);
	}

	.who-text {
		min-width: 0;
	}

	.who-name {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 4px 6px;
	}

	.years {
		display: block;
		margin-top: 1px;
		font-size: 12px;
		font-weight: 400;
		color: var(--color-muted);
	}

	.value {
		white-space: nowrap;
	}

	.plain {
		color: var(--color-ink-2);
	}

	.sorted {
		color: var(--color-ink);
		font-weight: 700;
	}

	.blank {
		color: var(--color-muted);
		font-weight: 400;
	}

	.leader {
		padding: 2px 7px;
		margin-right: -7px;
		border-radius: var(--radius-tag);
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
		font-weight: 700;
	}

	.signed {
		font-weight: 600;
	}

	.signed.w700 {
		font-weight: 700;
	}

	.up {
		color: var(--color-field);
	}

	.down {
		color: var(--color-pennant-dark);
	}

	.tier {
		font-weight: 700;
	}

	.tier-pennant-dark {
		color: var(--color-pennant-dark);
	}

	.tier-field {
		color: var(--color-field);
	}

	.tier-ink-2 {
		color: var(--color-ink-2);
	}

	.tier-muted {
		color: var(--color-muted);
	}

	.state {
		margin-top: 28px;
	}

	.muted-line {
		margin: 0 0 12px;
		color: var(--color-muted);
	}

	.skeleton {
		margin-top: 16px;
		height: 520px;
		border-radius: var(--radius-card);
		background: var(--color-line-soft);
	}

	@media (max-width: 639.98px) {
		.controls {
			margin-top: 14px;
			display: block;
		}

		.who {
			margin-top: 10px;
		}

		.seasons {
			margin-top: 10px;
			display: grid;
			grid-template-columns: 1fr 1fr;
			gap: 10px;
		}

		.table-card {
			margin-top: 14px;
		}

		.sort-line {
			line-height: 1.45;
		}

		.scroller {
			margin-top: 0;
		}

		.region {
			overflow-x: auto;
			padding: 0 16px;
		}

		/* Rows scrolled sideways pass under the sticky columns; this hides
		   them in the 16px gutter to the left of #. */
		.mask {
			position: absolute;
			top: 0;
			left: 0;
			bottom: 0;
			width: 16px;
			z-index: 2;
			background: var(--color-card);
			pointer-events: none;
		}

		.fade {
			position: absolute;
			top: 0;
			right: 0;
			bottom: 0;
			width: 28px;
			background: linear-gradient(90deg, transparent, var(--color-card));
			pointer-events: none;
		}

		.rank {
			font-size: 18px;
		}

		.who-link {
			gap: 8px;
		}

		/* 44px targets without moving the rows (Phones rule). */
		.who-link::after,
		.sort::after {
			content: '';
			position: absolute;
			left: -4px;
			right: -4px;
			top: 50%;
			height: 44px;
			transform: translateY(-50%);
		}

		.note {
			margin-top: 8px;
		}

		.note.first {
			margin-top: 12px;
		}
	}
</style>

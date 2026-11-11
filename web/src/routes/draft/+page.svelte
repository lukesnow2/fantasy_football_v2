<script lang="ts">
	import { onMount, tick } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import DraftBoard from '$lib/components/DraftBoard.svelte';
	import DraftAnalysis from '$lib/components/DraftAnalysis.svelte';
	import DraftValueBar from '$lib/components/DraftValueBar.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import {
		Avatar,
		Button,
		Card,
		DataTable,
		EmptyValue,
		GradeChip,
		PositionChip,
		Select,
		StatTip,
		type Column
	} from '$lib/components/ui';
	import { isHit, pickGrade, pickVerdict } from '$lib/draft-value';
	import { consistencyDetail, hasLowestBustRate, pointsOf, signed } from '$lib/draft-display';
	import { formatCount, profileHref } from '$lib/home';

	let draftData = $state<any>(null);
	let specificSeasonDraft = $state<any>(null);
	// Starts true: the server renders this page before onMount fetches the data,
	// and 'No draft data available' showed until the scripts ran.
	let loadingDrafts = $state(true);
	let loadingSeason = $state(false);
	let seasonFailed = $state(false);
	// Only the latest season request may land: a slower earlier one would put
	// one season's board under another season's header.
	let seasonRequest = 0;
	let failed = $state(false);
	let selectedSeason = $state('all');
	let selectedPick = $state<number | null>(null);
	let selectedCard = $state<HTMLElement | null>(null);

	onMount(() => {
		loadDraftData();
	});

	// What each pick was expected to score: from the API, which judges it against
	// players at the same position taken around the same round in this league's
	// history ($lib/draft-value). It used to be computed here as the average of
	// every pick at the same overall slot, position-blind, so QBs were always
	// steals and late kickers that were cut always busts.
	const expectedOf = (pick: any): number => Number(pick.expectedPoints ?? 0);

	// Calculate comprehensive manager analytics. The grade is the API's career
	// draft grade (draftGrades.byManager, $lib/draft-value): points over
	// expectation per season, graded on the evidence. It used to be raw points
	// per pick (A at 120+), which rewarded taking QBs early. Rows sort by draft
	// value per season, highest first, as the board draws them.
	function getManagerAnalytics(drafts: any[], careerGrades: any[]) {
		if (!drafts?.length) return [];
		const career = new Map(careerGrades.map((g: any) => [g.managerName, g]));

		const managerStats: any = {};

		drafts.forEach((pick) => {
			const manager = pick.managerName;
			const points = parseFloat(pick.seasonPoints || 0);
			const pickNum = pick.overallPick;

			if (!managerStats[manager]) {
				managerStats[manager] = {
					name: manager,
					picks: [],
					totalPoints: 0,
					totalPicks: 0
				};
			}

			managerStats[manager].picks.push({ points, pickNum, pick });
			managerStats[manager].totalPoints += points;
			managerStats[manager].totalPicks++;
		});

		return Object.values(managerStats)
			.map((manager: any) => {
				const avgPick =
					manager.picks.reduce((sum: number, p: any) => sum + p.pickNum, 0) / manager.picks.length;

				// Steals/busts/hits judged against what the pick's position and round were
				// expected to score (expectedOf); busts only among picks expected to beat
				// an average pick, and never a kicker or defense (pickVerdict).
				const verdictOf = (p: any) =>
					pickVerdict(p.points, expectedOf(p.pick), p.pick.highExpectation, p.pick.primaryPosition);
				const steals = manager.picks.filter((p: any) => verdictOf(p) === 'steal').length;
				const busts = manager.picks.filter((p: any) => verdictOf(p) === 'bust').length;
				const hits = manager.picks.filter((p: any) => isHit(p.points, expectedOf(p.pick))).length;
				const hitRate = Math.round((hits / manager.totalPicks) * 100);

				const c: any = career.get(manager.name);
				return {
					name: manager.name as string,
					totalPicks: manager.totalPicks as number,
					avgPick: Math.round(avgPick),
					steals,
					busts,
					hitRate,
					grade: (c?.grade ?? null) as string | null,
					draftValue: (c?.averageScore ?? null) as number | null
				};
			})
			.sort((a, b) => (b.draftValue ?? -Infinity) - (a.draftValue ?? -Infinity));
	}

	// Find best early round drafter (rounds 1-3)
	function getBestEarlyRoundDrafter(drafts: any[]) {
		if (!drafts?.length) return null;

		const earlyPicks = drafts.filter((pick) => pick.roundNumber <= 3);
		const managerStats: any = {};

		earlyPicks.forEach((pick) => {
			const manager = pick.managerName;
			const points = parseFloat(pick.seasonPoints || 0);

			if (!managerStats[manager]) {
				managerStats[manager] = { name: manager, totalPoints: 0, picks: 0 };
			}

			managerStats[manager].totalPoints += points;
			managerStats[manager].picks++;
		});

		const results = Object.values(managerStats)
			.filter((m: any) => m.picks >= 5) // Need at least 5 early picks
			.map((m: any) => ({
				...m,
				avgPoints: (m.totalPoints / m.picks).toFixed(1)
			}))
			.sort((a: any, b: any) => parseFloat(b.avgPoints) - parseFloat(a.avgPoints));

		return results[0] || null;
	}

	// Find best value hunter: the most steals, and the picks they came from.
	function getBestValueHunter(drafts: any[]) {
		if (!drafts?.length) return null;

		const managerStats: any = {};

		drafts.forEach((pick) => {
			const manager = pick.managerName;
			const points = parseFloat(pick.seasonPoints || 0);
			const expected = expectedOf(pick);

			if (!managerStats[manager]) {
				managerStats[manager] = { name: manager, steals: 0, picks: 0 };
			}

			if (pickVerdict(points, expected, pick.highExpectation, pick.primaryPosition) === 'steal')
				managerStats[manager].steals++;
			managerStats[manager].picks++;
		});

		const results = Object.values(managerStats).sort((a: any, b: any) => b.steals - a.steals);

		return results[0] || null;
	}

	// Find most consistent drafter
	function getMostConsistent(drafts: any[]) {
		if (!drafts?.length) return null;

		const managerStats: any = {};

		drafts.forEach((pick) => {
			const manager = pick.managerName;
			const points = parseFloat(pick.seasonPoints || 0);
			const expected = expectedOf(pick);

			if (!managerStats[manager]) {
				managerStats[manager] = { name: manager, hits: 0, total: 0 };
			}

			if (isHit(points, expected)) managerStats[manager].hits++;
			managerStats[manager].total++;
		});

		const results = Object.values(managerStats)
			.filter((m: any) => m.total >= 10) // Need at least 10 picks
			.map((m: any) => ({
				...m,
				consistency: Math.round((m.hits / m.total) * 100)
			}))
			.sort((a: any, b: any) => b.consistency - a.consistency);

		return results[0] || null;
	}

	// Load draft data
	async function loadDraftData() {
		if (draftData) return; // Already loaded

		loadingDrafts = true;
		failed = false;
		try {
			const response = await fetch('/api/draft?analysis=all');
			if (response.ok) {
				draftData = await response.json();
			} else {
				failed = true;
				console.error('Failed to fetch draft data:', response.status, response.statusText);
			}
		} catch (err) {
			failed = true;
			console.error('Error fetching draft data:', err);
		} finally {
			loadingDrafts = false;
		}
	}

	// Load specific season draft data
	async function loadSpecificSeasonDraft(season: string) {
		const request = ++seasonRequest;
		selectedPick = null;
		seasonFailed = false;
		specificSeasonDraft = null;
		if (season === 'all') {
			loadingSeason = false;
			return;
		}

		loadingSeason = true;
		try {
			const response = await fetch(`/api/draft?season=${season}&analysis=all`);
			if (request !== seasonRequest) return;
			if (response.ok) {
				const body = await response.json();
				if (request !== seasonRequest) return;
				specificSeasonDraft = body;
			} else {
				seasonFailed = true;
				console.error('Failed to fetch season draft data:', response.status, response.statusText);
			}
		} catch (err) {
			if (request !== seasonRequest) return;
			seasonFailed = true;
			console.error('Error fetching season draft data:', err);
		} finally {
			if (request === seasonRequest) loadingSeason = false;
		}
	}

	// The phone layout (below 640px) moves the links and the table's columns,
	// which is markup, not spacing. The data loads in the browser, so the body
	// never renders on the server.
	const phoneQuery = new MediaQuery('max-width: 639.98px', false);
	const phone = $derived(phoneQuery.current);
	const reducedMotion = new MediaQuery('prefers-reduced-motion: reduce', false);

	// ---- S8.4 header and callouts ------------------------------------------
	const data = $derived(draftData?.data);
	const drafts = $derived<any[]>(data?.drafts ?? []);
	const seasons = $derived<number[]>(data?.availableSeasons ?? []);
	const firstSeason = $derived(seasons.length ? Math.min(...seasons) : null);
	const seasonOptions = $derived([
		{ value: 'all', label: 'All seasons' },
		...seasons.map((s) => ({ value: String(s), label: String(s) }))
	]);

	const managerAnalytics = $derived(
		drafts.length ? getManagerAnalytics(drafts, data?.draftGrades?.byManager ?? []) : []
	);
	const bestEarlyRoundDrafter = $derived<any>(drafts.length ? getBestEarlyRoundDrafter(drafts) : null);
	const bestValueHunter = $derived<any>(drafts.length ? getBestValueHunter(drafts) : null);
	const mostConsistent = $derived<any>(drafts.length ? getMostConsistent(drafts) : null);
	const draftValue = $derived(data?.draftValue ?? null);

	// "Lowest bust rate" only when it's true, among the managers the
	// consistency callout considers (10 picks or more).
	const lowestBustRate = $derived(
		mostConsistent
			? hasLowestBustRate(
					mostConsistent.name,
					managerAnalytics
						.filter((m) => m.totalPicks >= 10)
						.map((m) => ({ name: m.name, busts: m.busts, picks: m.totalPicks }))
				)
			: false
	);

	const callouts = $derived(
		[
			bestEarlyRoundDrafter && {
				label: 'Best early-round drafter',
				name: bestEarlyRoundDrafter.name,
				detail: `${bestEarlyRoundDrafter.avgPoints} points per pick in rounds 1 to 3`
			},
			bestValueHunter && {
				label: 'Most steals',
				name: bestValueHunter.name,
				detail: `${bestValueHunter.steals} steals across ${bestValueHunter.picks} picks`
			},
			mostConsistent && {
				label: 'Most consistent',
				name: mostConsistent.name,
				detail: consistencyDetail(mostConsistent.consistency, lowestBustRate)
			}
		].filter(Boolean) as { label: string; name: string; detail: string }[]
	);

	// ---- S8.5 Career draft grades -------------------------------------------
	// One list for both widths. The phone table is 786px wide: the Manager
	// column's 16px inset sits inside its 172px and Avg pick's inside its 86px.
	// Desktop's Grade and Hit rate headers wrap their ⓘ as the board draws it.
	const GRADE_COLUMNS: (Omit<Column, 'width'> & { desktop?: string; phone: string; wrap?: boolean })[] = [
		{ key: 'name', label: 'Manager', phone: '156px' },
		{ key: 'grade', label: 'Grade', desktop: '60px', phone: '64px', wrap: true },
		{ key: 'value', label: 'Draft value per season', desktop: '240px', phone: '200px' },
		{ key: 'steals', label: 'Steals', numeric: true, desktop: '64px', phone: '56px' },
		{ key: 'busts', label: 'Busts', numeric: true, desktop: '64px', phone: '56px' },
		{ key: 'hit', label: 'Hit rate', numeric: true, desktop: '72px', phone: '80px', wrap: true },
		{ key: 'avg', label: 'Avg pick', numeric: true, desktop: '72px', phone: '70px' }
	];
	const columns = $derived<Column[]>(
		GRADE_COLUMNS.map(({ desktop, phone: phoneWidth, wrap, ...c }) => {
			const width = phone ? phoneWidth : desktop;
			const wraps = !phone && wrap;
			return { ...c, width, wrapHeader: wraps, fixedHeader: width != null && !wraps };
		})
	);
	const HEADER_TIPS: Record<string, string> = {
		grade: 'draft_grade',
		value: 'draft_value_per_season',
		hit: 'draft_hit_rate'
	};

	// ---- S8.7 a season's draft ----------------------------------------------
	const season = $derived(specificSeasonDraft?.data);
	const board = $derived<any[]>(season?.draftBoard ?? []);
	const numTeams = $derived(
		board[0]?.numTeams || board.filter((p: any) => p.roundNumber === 1).length || 10
	);
	const rounds = $derived(board.length ? Math.max(...board.map((p: any) => p.roundNumber)) : 0);
	const seasonPicks = $derived<any[]>(
		(season?.bestWorstPicks ?? []).filter((p: any) => String(p.seasonYear) === selectedSeason)
	);
	// The season's top steal until a reader picks another.
	const defaultPick = $derived(
		seasonPicks.find((p: any) => p.pickType === 'best')?.overallPick ?? board[0]?.overallPick ?? null
	);
	const shownPick = $derived(
		board.find((p: any) => p.overallPick === (selectedPick ?? defaultPick)) ?? null
	);
	const shownGrade = $derived(
		shownPick
			? pickGrade(
					pointsOf(shownPick),
					shownPick.expectedPoints,
					shownPick.highExpectation,
					shownPick.primaryPosition
				)
			: null
	);

	async function choosePick(overallPick: number) {
		selectedPick = overallPick;
		// Below 640px the card can sit 2,000px above the tapped cell.
		if (phone) {
			await tick();
			selectedCard?.scrollIntoView({
				block: 'start',
				behavior: reducedMotion.current ? 'auto' : 'smooth'
			});
		}
	}

	const kicker = $derived(
		selectedSeason === 'all'
			? data
				? `Activity · ${seasons.length} drafts · ${formatCount(data.meta?.totalDrafts ?? 0)} picks`
				: undefined
			: `Activity · Draft Central · ${selectedSeason}`
	);
	const pending = $derived(
		(loadingDrafts && !draftData) || (selectedSeason !== 'all' && loadingSeason)
	);
</script>

<svelte:head>
	<title>Draft Central · The League</title>
</svelte:head>

<PageHeader
	{kicker}
	title={selectedSeason === 'all' ? 'Draft Central' : `The ${selectedSeason} draft`}
	introWidth={660}
	phoneActionGap={14}
	phoneActionFull
	{pending}
>
	{#if selectedSeason === 'all'}
		{#if firstSeason != null}
			Every draft since {firstSeason}, graded on what the picks scored against what their round and
			position usually score in this league.
		{/if}
	{:else if board.length}
		{numTeams} teams, {rounds} rounds, {board.length} picks. Each pick is graded on what it scored against
		what that round and position usually score in this league.
	{/if}
	{#snippet action()}
		{#if data}
			<Select
				label="Season"
				options={seasonOptions}
				bind:value={selectedSeason}
				onchange={() => loadSpecificSeasonDraft(selectedSeason)}
			/>
		{/if}
	{/snippet}
</PageHeader>

<!-- A manager's avatar and name, linked to their profile. -->
{#snippet managerLink(name: string, size: number, initialSize: number, cls: string)}
	<a class={cls} href={profileHref(name)}
		><Avatar {name} {size} alt="" initialTone="field" {initialSize} />{name}</a
	>
{/snippet}

{#if loadingDrafts}
	<div class="first">
		<Card title="Loading drafts" headGap={0} />
	</div>
{:else if failed || !data}
	<div class="first">
		<Card title="Drafts didn't load">
			<p class="muted-line">Try again in a minute.</p>
			<Button
				variant="secondary"
				onclick={() => {
					draftData = null;
					loadDraftData();
				}}>Try again</Button
			>
		</Card>
	</div>
{:else if selectedSeason === 'all'}
	<!-- S8.4 Three callouts -->
	<div class="callouts">
		{#each callouts as c (c.label)}
			<Card padding="0" phonePadding="0" class="callout-card">
				<svelte:element
					this={phone ? 'a' : 'div'}
					class="callout"
					href={phone ? profileHref(c.name) : undefined}
				>
					<Avatar name={c.name} size={phone ? 48 : 56} alt="" initialTone="field" />
					<span class="callout-text">
						<span class="callout-label">{c.label}</span>
						<span class="callout-name"
							>{#if phone}{c.name}{:else}<a href={profileHref(c.name)}>{c.name}</a>{/if}</span
						>
						<span class="callout-detail">{c.detail}</span>
					</span>
				</svelte:element>
			</Card>
		{/each}
	</div>

	<!-- S8.5 Career draft grades -->
	<div class="gap grades">
	<Card
		title="Career draft grades"
		padding="22px 24px 12px"
		phonePadding="16px 16px 8px"
		headSpacing="8px 16px"
		phoneHeadStack
	>
		{#snippet action()}<span class="head-note"
				>A and F take a long record well clear of average. C means too close to call.</span
			>{/snippet}
		<DataTable
			label="Career draft grades"
			{columns}
			rows={managerAnalytics}
			rowKey={(m) => m.name}
			rowPadding={phone ? 2 : 10}
			columnGap={phone ? 12 : 14}
			fontSize={phone ? 14 : 15}
			minWidth={phone ? '786px' : '860px'}
			stickyFirst={phone}
			stickyRule={false}
			edgeInset={phone ? 16 : undefined}
			headerAlign={phone ? 'bottom' : 'top'}
		>
			{#snippet header(column)}
				{#if HEADER_TIPS[column.key]}<StatTip
						metricId={HEADER_TIPS[column.key]}
						label={column.label}
						phoneTap="inline-small"
					/>{:else}{column.label}{/if}
			{/snippet}
			{#snippet cell(m, column)}
				{#if column.key === 'name'}
					{@render managerLink(m.name, phone ? 26 : 30, phone ? 12 : 13, 'grade-manager')}
				{:else if column.key === 'grade'}
					<GradeChip grade={m.grade} size={phone ? 30 : 34} fontSize={phone ? 18 : 20} />
				{:else if column.key === 'value'}
					{#if m.draftValue == null}<EmptyValue />{:else}<DraftValueBar value={m.draftValue} />{/if}
				{:else if column.key === 'steals'}
					{m.steals}
				{:else if column.key === 'busts'}
					{m.busts}
				{:else if column.key === 'hit'}
					{m.hitRate}%
				{:else if column.key === 'avg'}
					<span class="muted">#{m.avgPick}</span>
				{/if}
			{/snippet}
		</DataTable>
		<p class="footnote">
			Draft value is how many average picks' worth of points a draft beat its slots by. Career grades
			weigh the whole record, so getting off a C takes years of drafting that way.
		</p>
	</Card>
	</div>

	<!-- S8.6 Best value picks, biggest busts, best drafters -->
	{#if draftValue}
		<div class="lists">
			{#each [{ title: 'Best value picks', sub: 'Points over what that round and position usually score.', rows: draftValue.bestValue.slice(0, 8), tone: 'field' }, { title: 'Biggest busts', sub: 'Early picks, rounds 1 to 8, that came up furthest short.', rows: draftValue.biggestBusts.slice(0, 8), tone: 'pennant' }] as list (list.title)}
				<Card title={list.title} headGap={0}>
					<p class="list-sub">{list.sub}</p>
					{#each list.rows as p, i (i)}
						<div class="value-row">
							<span class="value-main">
								<span class="value-player">{p.playerName}</span>
								<span class="value-meta">
									<PositionChip position={p.position} tone="neutral" />
									<span class="value-when">{p.seasonYear} · Rd {p.roundNumber}</span>
								</span>
							</span>
							<span class="value-points" style:color="var(--color-{list.tone})"
								>{signed(Number(p.valueOverExpected), 1)}</span
							>
						</div>
					{/each}
				</Card>
			{/each}
		</div>

		{#if draftValue.byManager?.length}
			<div class="gap">
			<Card title="Best drafters" headSpacing="8px 16px" phoneHeadStack>
				{#snippet action()}<span class="head-note"
						>Average points over expectation per pick, career</span
					>{/snippet}
				<div class="drafters">
					{#each draftValue.byManager.slice(0, 5) as m, i (m.managerName)}
						<Card tone="panel" padding="0" phonePadding="0">
							<svelte:element
								this={phone ? 'a' : 'div'}
								class="drafter"
								href={phone ? profileHref(m.managerName) : undefined}
							>
								<span class="drafter-rank">{i + 1}</span>
								<Avatar name={m.managerName} size={36} alt="" initialTone="field" />
								<span class="drafter-text">
									<span class="drafter-name"
										>{#if phone}{m.managerName}{:else}<a href={profileHref(m.managerName)}
												>{m.managerName}</a
											>{/if}</span
									>
									<span class="drafter-picks">{m.picks} picks</span>
								</span>
								<span class="drafter-value" class:negative={m.avgValue < 0}
									>{signed(Number(m.avgValue), 1)}</span
								>
							</svelte:element>
						</Card>
					{/each}
				</div>
			</Card>
			</div>
		{/if}
	{/if}
{:else if loadingSeason}
	<div class="first">
		<Card title="Loading the {selectedSeason} draft" headGap={0} />
	</div>
{:else if seasonFailed}
	<div class="first">
		<Card title="The {selectedSeason} draft didn't load">
			<p class="muted-line">Try again in a minute.</p>
			<Button variant="secondary" onclick={() => loadSpecificSeasonDraft(selectedSeason)}
				>Try again</Button
			>
		</Card>
	</div>
{:else if !board.length}
	<div class="first">
		<Card title="No draft for {selectedSeason}">
			<p class="muted-line last">Yahoo has no draft results for this season.</p>
		</Card>
	</div>
{:else}
	{#if season?.seasonInProgress}
		<div class="first">
			<Card title="Season in progress">
				<p class="muted-line last">
					This season is still being played: grades and tags are judged on points so far.
				</p>
			</Card>
		</div>
	{/if}

	<!-- S8.7 Selected pick, best and worst -->
	<div class="pick-row" class:after-note={season?.seasonInProgress}>
		{#if shownPick}
				<Card
					label="Selected pick"
					padding="20px 22px"
					class="selected-card"
					bind:element={selectedCard}
				>
					<div class="selected">
						<div class="selected-who">
							<div class="selected-top">
								<PositionChip position={shownPick.primaryPosition} />
								<span class="selected-slot"
									>Pick {shownPick.overallPick} · Round {shownPick.roundNumber}</span
								>
							</div>
							<div class="selected-player">{shownPick.playerName}</div>
							{@render managerLink(shownPick.managerName, 26, 11, 'selected-manager')}
						</div>
						<div class="selected-stats">
							<div>
								<div class="stat-value">{pointsOf(shownPick).toFixed(1)}</div>
								<div class="stat-label">Points</div>
							</div>
							<div>
								<div class="stat-value muted">
									{#if shownPick.expectedPoints == null}<EmptyValue />{:else}{Number(
											shownPick.expectedPoints
										).toFixed(1)}{/if}
								</div>
								<div class="stat-label">Expected</div>
							</div>
							<div>
								<div
									class="stat-value"
									class:up={Number(shownPick.valueOverExpected) >= 0}
									class:down={Number(shownPick.valueOverExpected) < 0}
								>
									{#if shownPick.valueOverExpected == null}<EmptyValue />{:else}{signed(
											Number(shownPick.valueOverExpected),
											1
										)}{/if}
								</div>
								<div class="stat-label">vs expected</div>
							</div>
							<div>
								<div class="stat-chip"><GradeChip grade={shownGrade} size={40} /></div>
								<div class="stat-label chip-label">Grade</div>
							</div>
						</div>
					</div>
				</Card>
		{/if}
		<DraftAnalysis picks={seasonPicks} onpick={choosePick} />
	</div>

	<!-- S8.8 The draft board -->
	<div class="gap">
		<DraftBoard
			picks={board}
			season={selectedSeason}
			grades={season?.draftGrades?.bySeason ?? []}
			selected={shownPick?.overallPick ?? null}
			onpick={choosePick}
		/>
	</div>
{/if}

<style>
	.first {
		margin-top: 28px;
	}

	.muted-line {
		margin: 0 0 16px;
		color: var(--color-muted);
	}

	.muted-line.last {
		margin-bottom: 0;
	}

	.muted {
		color: var(--color-muted);
	}

	.gap {
		margin-top: 20px;
	}

	.head-note {
		font-size: 13px;
		color: var(--color-muted);
	}

	/* ---- S8.4 callouts ---- */
	.callouts {
		margin-top: 28px;
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
		gap: 16px;
	}

	.callout {
		display: flex;
		gap: 14px;
		align-items: center;
		padding: 18px 20px;
	}

	.callout-text {
		display: flex;
		flex-direction: column;
		min-width: 0;
	}

	.callout-label {
		font-size: 13px;
		font-weight: 600;
		color: var(--color-muted);
	}

	.callout-name {
		margin-top: 2px;
		font-weight: 700;
		font-size: 17px;
	}

	.callout-detail {
		margin-top: 2px;
		font-size: 14px;
		color: var(--color-ink-2);
	}

	/* ---- S8.5 grades ---- */
	:global(.grade-manager) {
		display: flex;
		align-items: center;
		gap: 10px;
		font-weight: 600;
	}

	.footnote {
		margin: 14px 0 6px;
		max-width: 860px;
		font-size: 13px;
		line-height: 1.5;
		color: var(--color-muted);
	}

	/* ---- S8.6 lists ---- */
	.lists {
		margin-top: 20px;
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
		gap: 20px;
	}

	.list-sub {
		margin: 4px 0 8px;
		font-size: 14px;
		color: var(--color-muted);
	}

	.value-row {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 44px 92px 72px;
		gap: 10px;
		align-items: center;
		padding: 9px 0;
		border-bottom: 1px solid var(--color-line-soft);
		font-size: 15px;
	}

	/* Grid cells on desktop; the name over its chip and year on phones. */
	.value-main,
	.value-meta {
		display: contents;
	}

	.value-player {
		font-weight: 600;
	}

	.value-when {
		font-size: 13px;
		color: var(--color-muted);
	}

	.value-points {
		text-align: right;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 18px;
	}

	.drafters {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
		gap: 12px;
	}

	.drafter {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 12px 14px;
	}

	.drafter-rank {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 20px;
		color: var(--color-muted);
	}

	.drafter-text {
		display: flex;
		flex-direction: column;
		flex: 1;
		min-width: 0;
	}

	.drafter-name {
		font-weight: 700;
		font-size: 15px;
	}

	.drafter-picks {
		font-size: 12px;
		color: var(--color-muted);
	}

	.drafter-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 22px;
		color: var(--color-field);
	}

	.drafter-value.negative {
		color: var(--color-pennant);
	}

	/* ---- S8.7 a season's draft ---- */
	.pick-row {
		margin-top: 24px;
		display: flex;
		flex-wrap: wrap;
		gap: 16px;
		align-items: stretch;
	}

	.pick-row.after-note {
		margin-top: 20px;
	}

	/* The board is content-box: the basis is the card's content width, with
	   its padding outside it, as on the Trade Center. */
	.pick-row :global(.selected-card) {
		box-sizing: content-box;
		flex: 2 1 460px;
		display: flex;
		align-items: center;
	}

	.selected {
		flex: 1;
		min-width: 0;
		display: flex;
		flex-wrap: wrap;
		gap: 16px 24px;
		align-items: center;
	}

	.selected-who {
		flex: 1 1 220px;
		min-width: 0;
	}

	.selected-top {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.selected-slot {
		font-size: 13px;
		font-weight: 600;
		color: var(--color-muted);
	}

	.selected-player {
		margin-top: 6px;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 32px;
		line-height: 1.05;
	}

	:global(.selected-manager) {
		margin-top: 6px;
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 15px;
		font-weight: 600;
	}

	.selected-stats {
		flex: 1 1 260px;
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr));
		gap: 10px;
		text-align: center;
	}

	.stat-value {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 26px;
	}

	.stat-value.muted {
		color: var(--color-muted);
	}

	.stat-value.up {
		color: var(--color-field);
	}

	.stat-value.down {
		color: var(--color-pennant);
	}

	.stat-chip {
		display: flex;
		justify-content: center;
	}

	.stat-label {
		font-size: 12px;
		color: var(--color-muted);
	}

	.chip-label {
		margin-top: 2px;
	}

	/* ---- S8.10 phones ---- */
	@media (max-width: 639.98px) {
		.first,
		.callouts {
			margin-top: 14px;
		}

		.gap {
			margin-top: 14px;
		}

		.callouts {
			display: flex;
			flex-direction: column;
			gap: 10px;
		}

		.callout {
			gap: 12px;
			padding: 16px;
		}

		/* The table runs flush to the card's sides and scrolls inside it. */
		.grades :global(.scroll) {
			margin: 12px -16px 0;
		}

		.grades :global(.head) {
			margin-bottom: 0;
		}

		:global(.grade-manager) {
			min-height: 44px;
			gap: 8px;
		}

		.footnote {
			margin: 12px 0 6px;
			max-width: none;
		}

		.lists {
			display: flex;
			flex-direction: column;
			gap: 14px;
			margin-top: 14px;
		}

		.list-sub {
			margin: 4px 0 4px;
		}

		.value-row {
			grid-template-columns: minmax(0, 1fr) auto;
		}

		.value-main {
			display: block;
			min-width: 0;
		}

		.value-player {
			display: block;
		}

		.value-meta {
			display: flex;
			align-items: center;
			gap: 8px;
			margin-top: 3px;
		}

		.drafters {
			display: flex;
			flex-direction: column;
			gap: 8px;
		}

		.drafter {
			padding: 10px 14px;
		}

		.pick-row,
		.pick-row.after-note {
			flex-direction: column;
			flex-wrap: nowrap;
			gap: 14px;
			margin-top: 14px;
		}

		.pick-row :global(.selected-card) {
			box-sizing: border-box;
			flex: none;
			display: block;
		}

		.selected {
			display: block;
		}

		.selected-player {
			font-size: 28px;
		}

		:global(.selected-manager) {
			display: inline-flex;
			margin-top: 2px;
			min-height: 44px;
		}

		.selected-stats {
			margin-top: 8px;
		}

		.stat-value {
			font-size: 24px;
		}
	}
</style>

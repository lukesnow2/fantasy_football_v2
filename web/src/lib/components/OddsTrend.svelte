<script lang="ts">
	import { formatOdds } from '$lib/home';
	import {
		chartWeeks,
		decidedWeek,
		dodge,
		oddsAxis,
		type OddsHistory,
		type OddsMetric
	} from '$lib/odds-history';
	import { RANK_LINE_COLORS } from '$lib/utils/power-rankings';
	import Avatar from './ui/Avatar.svelte';

	/**
	 * Every team's odds week by week, one line each, on the Playoff Odds page.
	 * Each line ends in its manager's picture. Picking or hovering a team in the
	 * legend or on the chart (or passing `highlight`) brings its line forward,
	 * as Rank through the season does.
	 *
	 * Odds crowd together in ways ranks can't, so three rules keep it legible:
	 *  - A line stops the week its odds were settled for good (decidedWeek), so
	 *    a season's end isn't ten lines piled at 0% and 100%, and where a line
	 *    stops is when that team's fate was sealed.
	 *  - Pictures never stack. Lines still open at the last
	 *    week get theirs in a column right of the plot, spread apart (dodge)
	 *    with a thin leader back to the line; lines that stopped earlier get
	 *    theirs at the stop, spread the same way.
	 *  - Title odds before the bracket use a shorter axis (oddsAxis), from 0.
	 *
	 * Colors follow the team, not its place, so switching between playoffs,
	 * bye and title doesn't repaint anyone.
	 */

	interface Props {
		history: Extract<OddsHistory, { available: true }>;
		metric: OddsMetric;
		/** The team to bring forward until a reader picks one. */
		highlight?: number | null;
	}

	let { history, metric, highlight = null }: Props = $props();

	let picked = $state<number | null | undefined>(undefined);
	let hovered = $state<number | null>(null);
	// A pick only counts while its team is on the chart: switching seasons on
	// the page keeps this component, and a team from another season would
	// fade every line.
	const onChart = $derived(new Set(history.teams.map((t) => t.teamKey)));
	const focus = $derived.by(() => {
		if (hovered != null && onChart.has(hovered)) return hovered;
		if (picked === null || (picked !== undefined && onChart.has(picked))) return picked;
		return highlight;
	});

	// Room on the right for the column of pictures.
	const W = 960;
	const H = 460;
	const PAD = { top: 22, right: 70, bottom: 34, left: 52 };
	/** The chart's drawn width in px, to keep the pictures a picture apart. */
	let drawn = $state(0);

	const weeks = $derived(chartWeeks(history, metric));
	const lastWeek = $derived(weeks[weeks.length - 1] ?? 0);
	const axis = $derived(oddsAxis(history, metric));
	const x = $derived.by(() => {
		const first = weeks[0] ?? 1;
		const span = Math.max(1, lastWeek - first);
		return (w: number) => PAD.left + ((w - first) / span) * (W - PAD.left - PAD.right);
	});
	const y = $derived(
		(p: number) => PAD.top + (1 - Math.min(p, axis.max) / axis.max) * (H - PAD.top - PAD.bottom)
	);

	// One color per team for good: by team, in the order the teams come.
	const colorOf = $derived(
		new Map(
			[...history.teams]
				.sort((a, b) => a.teamKey - b.teamKey)
				.map((t, i) => [t.teamKey, RANK_LINE_COLORS[i % RANK_LINE_COLORS.length]])
		)
	);

	const series = $derived.by(() => {
		const inWeeks = new Set(weeks);
		return history.teams
			.map((t) => {
				const decided = decidedWeek(history, t, metric);
				const pts = t.points.filter(
					(p) => inWeeks.has(p.week) && (decided == null || p.week <= decided)
				);
				const end = pts[pts.length - 1];
				return {
					teamKey: t.teamKey,
					name: t.managerName ?? 'Unknown manager',
					color: colorOf.get(t.teamKey) ?? RANK_LINE_COLORS[0],
					last: end?.[metric] ?? 0,
					endWeek: end?.week ?? 0,
					stopped: end != null && end.week < lastWeek,
					end: end ? { x: x(end.week), y: y(end[metric]) } : { x: 0, y: 0 },
					path: pts
						.map(
							(p, i) => `${i === 0 ? 'M' : 'L'}${x(p.week).toFixed(1)},${y(p[metric]).toFixed(1)}`
						)
						.join(' ')
				};
			})
			.filter((s) => s.path !== '')
			.sort((a, b) => b.last - a.last || b.endWeek - a.endWeek);
	});

	// The pictures: grouped by the week their line stops, each
	// group spread apart up and down so no two overlap.
	const ends = $derived.by(() => {
		const gap = (30 * W) / (drawn || W);
		const lo = PAD.top;
		const hi = H - PAD.bottom;
		const railX = W - PAD.right + 34;
		const groups = new Map<number, typeof series>();
		for (const s of series) {
			const group = groups.get(s.endWeek) ?? [];
			group.push(s);
			groups.set(s.endWeek, group);
		}
		const out: {
			teamKey: number;
			name: string;
			color: string;
			from: { x: number; y: number };
			x: number;
			y: number;
			rail: boolean;
		}[] = [];
		for (const [week, group] of groups) {
			const rail = week === lastWeek;
			// A group too big for the plot at that spacing squeezes in rather
			// than running off the bottom (narrow screens, ten close lines).
			const fit = group.length > 1 ? Math.min(gap, (hi - lo) / (group.length - 1)) : gap;
			const spread = dodge(
				group.map((s) => s.end.y),
				fit,
				lo,
				hi
			);
			group.forEach((s, i) =>
				out.push({
					teamKey: s.teamKey,
					name: s.name,
					color: s.color,
					from: s.end,
					x: rail ? railX : s.end.x,
					y: spread[i],
					rail
				})
			);
		}
		return out;
	});
	// A leader whenever the picture isn't sitting on its line's end.
	const leaders = $derived(ends.filter((e) => e.rail || Math.abs(e.y - e.from.y) > 1));
	// Axis text in viewBox units that come out near 12.5px whatever the width.
	const axisFont = $derived((12.5 * W) / (drawn || W));
	const pctX = (v: number) => `${(v / W) * 100}%`;
	const pctY = (v: number) => `${(v / H) * 100}%`;

	// Lines drawn back to front, so the focused one sits on top.
	const drawOrder = $derived(
		[...series].sort((a, b) => Number(a.teamKey === focus) - Number(b.teamKey === focus))
	);

	// Every week fits up to about 16; past that, every other one.
	const ticks = $derived(weeks.filter((_, i) => weeks.length <= 16 || i % 2 === 0));
	const playoffStart = $derived(
		metric === 'titleOdds' && history.playoffWeeks.length > 0
			? (x(history.regularSeasonWeeks) + x(history.playoffWeeks[0])) / 2
			: null
	);
	const pct = (p: number) => `${Math.round(p * 100)}%`;
	const zoomNote = $derived(
		axis.max < 1
			? `The scale stops at ${pct(axis.max)}, since no team's title odds have gone past it.`
			: null
	);

	// A line that stopped says when: in or out, and the week it was settled.
	const legendValue = (s: { last: number; stopped: boolean; endWeek: number }) =>
		s.stopped && (s.last === 0 || s.last === 1)
			? `${s.last === 1 ? 'In' : 'Out'}, wk ${s.endWeek}`
			: formatOdds(s.last, s.last === 0 || s.last === 1);

	const LABELS: Record<OddsMetric, string> = {
		playoffOdds: 'Playoff odds',
		byeOdds: 'Bye odds',
		titleOdds: 'Title odds'
	};
	const label = $derived(
		series.length === 0
			? `${LABELS[metric]} by week: none yet.`
			: `${LABELS[metric]} by week, weeks ${weeks[0]} to ${lastWeek}, on a scale of 0 to ${pct(axis.max)}. ${series
					.map(
						(s) =>
							`${s.name} ${s.stopped ? `settled at ${pct(s.last)} in week ${s.endWeek}` : formatOdds(s.last, s.last === 0 || s.last === 1)}`
					)
					.join(', ')}.`
	);
</script>

<div class="trend">
	<div class="chart-scroll">
		<div class="chart" role="img" aria-label={label} bind:clientWidth={drawn}>
			<svg viewBox="0 0 {W} {H}" aria-hidden="true">
				{#each axis.ticks as p (p)}
					<line
						x1={PAD.left}
						x2={W - PAD.right}
						y1={y(p)}
						y2={y(p)}
						style:stroke="var(--color-line-soft)"
						stroke-width="1"
					/>
					<text
						style:font-size="{axisFont}px"
						class="axis"
						x={PAD.left - 8}
						y={y(p) + 5}
						text-anchor="end">{pct(p)}</text
					>
				{/each}
				{#if playoffStart != null}
					<line
						x1={playoffStart}
						x2={playoffStart}
						y1={PAD.top}
						y2={H - PAD.bottom}
						style:stroke="var(--color-field)"
						stroke-width="1.5"
						stroke-dasharray="4 4"
					/>
					<!-- Left of the line: the bracket weeks are the chart's last few. -->
					<text
						style:font-size="{axisFont}px"
						class="axis playoffs"
						x={playoffStart - 6}
						y={PAD.top + 12}
						text-anchor="end">Playoffs</text
					>
				{/if}
				{#each ticks as w (w)}
					<text style:font-size="{axisFont}px" class="axis" x={x(w)} y={H - 6} text-anchor="middle"
						>{w}</text
					>
				{/each}
				{#each drawOrder as s (s.teamKey)}
					<path
						d={s.path}
						fill="none"
						style:stroke={s.color}
						stroke-width={focus === s.teamKey ? 4 : 2.25}
						stroke-opacity={focus != null && focus !== s.teamKey ? 0.2 : 0.9}
						stroke-linejoin="round"
						stroke-linecap="round"
					/>
				{/each}
				{#each leaders as e (e.teamKey)}
					<line
						x1={e.from.x}
						y1={e.from.y}
						x2={e.x}
						y2={e.y}
						style:stroke={e.color}
						stroke-width="1.25"
						stroke-opacity={focus != null && focus !== e.teamKey ? 0.15 : 0.55}
					/>
				{/each}
			</svg>
			{#each ends as e (e.teamKey)}
				<span
					class="end"
					role="presentation"
					title={e.name}
					style:left={pctX(e.x)}
					style:top={pctY(e.y)}
					style:opacity={focus != null && focus !== e.teamKey ? 0.35 : 1}
					style:z-index={focus === e.teamKey ? 2 : 1}
					onmouseenter={() => (hovered = e.teamKey)}
					onmouseleave={() => (hovered = null)}
				>
					<Avatar name={e.name} size={26} ring={2.5} ringColor={e.color} />
				</span>
			{/each}
		</div>
		<p class="axis-note">Week</p>
		{#if zoomNote}<p class="zoom-note">{zoomNote}</p>{/if}
	</div>
	<div class="legend" role="group" aria-label="Pick a team to bring its line forward">
		{#each series as s (s.teamKey)}
			<button
				type="button"
				class:on={focus === s.teamKey}
				aria-pressed={focus === s.teamKey}
				onclick={() => (picked = focus === s.teamKey ? null : s.teamKey)}
				onmouseenter={() => (hovered = s.teamKey)}
				onmouseleave={() => (hovered = null)}
			>
				<span class="dot" style:background={s.color}></span>
				<span class="name">{s.name}</span>
				<span class="value">{legendValue(s)}</span>
			</button>
		{/each}
	</div>
</div>

<style>
	/* Legend beside the chart, which scrolls sideways on phones. */
	.trend {
		display: flex;
		flex-wrap: wrap;
		gap: 20px;
		align-items: flex-start;
		min-width: 0;
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

	.end {
		position: absolute;
		display: inline-flex;
		transform: translate(-50%, -50%);
	}

	.axis {
		font-family: var(--font-sans);
		font-weight: 600;
		fill: var(--color-muted);
	}

	.axis.playoffs {
		fill: var(--color-field);
	}

	.zoom-note {
		margin: 6px 0 0;
		font-size: 12px;
		line-height: 1.4;
		color: var(--color-muted);
	}

	.axis-note {
		margin: -2px 0 0;
		text-align: center;
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-muted);
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
		gap: 8px;
		min-height: 34px;
		padding: 0 8px;
		border: 0;
		border-radius: var(--radius-control);
		background: transparent;
		cursor: pointer;
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

	.name {
		flex: 1;
		min-width: 0;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.value {
		color: var(--color-muted);
	}

	@media (max-width: 639.98px) {
		.legend button {
			min-height: 40px;
		}
	}
</style>

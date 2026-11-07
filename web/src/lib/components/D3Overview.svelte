<script lang="ts">
	// League trends over time, drawn with D3.
	//
	// Each measure answers one question and offers only the chart forms that
	// suit its data: a level over time is a line; a count per season can also be
	// bars; shares of a whole are stacked; a rank is a dot; who-won-when is a
	// timeline. No measure gets a form just because the toggle exists.
	//
	// Design rules (from the dataviz guidance this site follows):
	//  - One y-scale per plot. Measures on different scales get panels that share
	//    the season axis and one crosshair, never a second y-axis.
	//  - Reference lines carry meaning where there is one (a league of coin flips).
	//  - The season still being played is marked partial and kept out of
	//    call-outs, insights and anything that needs a finished season.
	//  - Hover snaps to the nearest season (or mark) and lists every series;
	//    arrow keys do the same; a table view carries every value.
	//  - Motion has a job: lines draw in, bars and stacks grow, titles appear in
	//    the order they were won, and switching measure or view cross-fades.
	import { onDestroy, onMount, tick } from 'svelte';
	import { browser } from '$app/environment';
	import { getInitials, getManagerProfilePicture } from '$lib/utils/managerUtils';

	export let data: any = null;

	let d3: any = null;
	let container: HTMLDivElement;
	let tooltipEl: HTMLDivElement;
	let selectedMetric = 'scoring';
	let selectedView: ViewId = 'line';
	let showTable = false;

	type ViewId = 'line' | 'bar' | 'dots' | 'grid' | 'race' | 'stackedBar' | 'stackedArea';

	interface SeriesDef {
		key: string;
		label: string;
		unit: string;
		decimals: number;
		color: string;
		/** Percent of 1 (0.27 shows as 27%). */
		percent?: boolean;
		/** In bar view, drawn as a line over the bars (a running average). */
		overlay?: boolean;
	}

	interface MetricDef {
		id: string;
		name: string;
		question: string;
		description: string;
		kind: 'series' | 'champion' | 'titles' | 'stacked';
		views: ViewId[];
		layout?: 'shared' | 'panels';
		series: SeriesDef[];
		bands?: { lo: string; hi: string; label: string; opacity: number }[];
		refLine?: { value: number; label: string };
		axisLabel?: string;
		source: 'scoring' | 'trends';
	}

	// Categorical slots, dark steps, validated against this page's navy surface
	// (six adjacent for the stack: all checks pass; green is under 3:1 contrast,
	// so the table view is the relief).
	const BLUE = '#3987e5';
	const ORANGE = '#d95926';
	const AQUA = '#199e70';
	const YELLOW = '#c98500';
	const MAGENTA = '#d55181';
	const GREEN = '#008300';

	const POSITIONS: [string, string, string][] = [
		['QB', 'Quarterbacks', BLUE],
		['RB', 'Running backs', ORANGE],
		['WR', 'Wide receivers', AQUA],
		['TE', 'Tight ends', YELLOW],
		['K', 'Kickers', MAGENTA],
		['DEF', 'Defenses', GREEN]
	];

	const metrics: MetricDef[] = [
		{
			id: 'scoring',
			name: 'Scoring Evolution',
			question: 'How much do teams score, and how far apart are good and bad weeks?',
			description:
				'Average team score per game, inside the range of real weekly scores: the darker band is the middle half of all scores that season, the lighter one the middle 80%.',
			kind: 'series',
			views: ['line'],
			layout: 'shared',
			series: [{ key: 'avgWeeklyScore', label: 'Average team score', unit: ' pts', decimals: 1, color: BLUE }],
			bands: [
				{ lo: 'p10Score', hi: 'p90Score', label: 'Middle 80% of weekly scores', opacity: 0.12 },
				{ lo: 'p25Score', hi: 'p75Score', label: 'Middle 50%', opacity: 0.2 }
			],
			axisLabel: 'Points per game',
			source: 'scoring'
		},
		{
			id: 'balance',
			name: 'Skill vs Luck',
			question: 'Do records spread out more than coin flips would?',
			description:
				"The Noll-Scully ratio: how far apart the season's records were, against how far apart they'd be if every game were a coin flip (1.0). Above 1, skill is showing through; the hover gives the share of the spread that's skill (Tango's method). One season is a small sample, so the five-season average carries the trend.",
			kind: 'series',
			views: ['line', 'bar'],
			layout: 'shared',
			series: [
				{ key: 'nollScully', label: 'Noll-Scully ratio', unit: '', decimals: 2, color: BLUE },
				{ key: 'nollScullyRolling', label: 'Five-season average', unit: '', decimals: 2, color: ORANGE, overlay: true }
			],
			refLine: { value: 1, label: 'A league of coin flips' },
			axisLabel: 'Spread of records vs chance',
			source: 'trends'
		},
		{
			id: 'champion',
			name: 'Does the Best Team Win?',
			question: 'Where did each champion rank in regular-season scoring?',
			description:
				"Each season's champion, placed at their rank in points per game over the regular season (1 = the top scorer). The shaded band is the top three.",
			kind: 'champion',
			views: ['dots'],
			series: [],
			source: 'trends'
		},
		{
			id: 'titles',
			name: 'Championships',
			question: 'Who has won, and who keeps coming close?',
			description:
				'Every final, by manager and season. The grid shows titles and finals lost against the seasons each manager played; the race shows how the title count built up.',
			kind: 'titles',
			views: ['grid', 'race'],
			series: [],
			source: 'trends'
		},
		{
			id: 'positions',
			name: 'Where the Points Come From',
			question: "Which positions score a starting lineup's points?",
			description:
				"Share of every starting lineup's regular-season points, by position. The 2008 shift (defenses halve, receivers jump) lines up with a change in the league's settings, and it explains part of that year's jump in scoring.",
			kind: 'stacked',
			views: ['stackedBar', 'stackedArea'],
			series: POSITIONS.map(([k, label, color]) => ({
				key: `pos_${k}`,
				label,
				unit: '',
				decimals: 1,
				color,
				percent: true
			})),
			source: 'trends'
		},
		{
			id: 'activity',
			name: 'League Activity',
			question: 'How busy is the waiver wire, and how often do managers trade?',
			description:
				'Adds, drops and add/drops per team (per team, so the 8-team seasons compare fairly), and trades across the league.',
			kind: 'series',
			views: ['bar', 'line'],
			layout: 'panels',
			series: [
				{ key: 'movesPerTeam', label: 'Moves per team', unit: '', decimals: 1, color: BLUE },
				{ key: 'trades', label: 'Trades', unit: '', decimals: 0, color: ORANGE }
			],
			source: 'trends'
		}
	];

	const VIEW_NAMES: Record<ViewId, string> = {
		line: 'Line',
		bar: 'Bars',
		dots: 'Dots',
		grid: 'Grid',
		race: 'Title race',
		stackedBar: 'Stacked bars',
		stackedArea: 'Stacked area'
	};

	$: currentMetric = metrics.find((m) => m.id === selectedMetric) ?? metrics[0];
	// A measure only offers its own views; switching measure picks its first.
	$: if (!currentMetric.views.includes(selectedView)) selectedView = currentMetric.views[0];
	$: live = data?.seasonInProgress ?? null;
	$: trends = data?.trends ?? null;
	$: headline = trends?.headline ?? null;

	// ---- data ----------------------------------------------------------------

	const num = (v: unknown): number | null => {
		if (v === null || v === undefined || v === '') return null;
		const n = typeof v === 'number' ? v : Number(v);
		return Number.isFinite(n) ? n : null;
	};
	const snake = (k: string) => k.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
	const read = (row: any, key: string) => num(row?.[key] ?? row?.[snake(key)]);
	const yearOf = (row: any) => num(row?.seasonYear ?? row?.season_year);

	function rowsFor(metric: MetricDef): any[] {
		if (!data) return [];
		if (metric.source === 'scoring') {
			return (data.scoringPatterns ?? data.scoring_patterns ?? [])
				.map((r: any) => ({ ...r, seasonYear: yearOf(r) }))
				.filter((r: any) => r.seasonYear != null)
				.sort((a: any, b: any) => a.seasonYear - b.seasonYear);
		}
		return (trends?.seasons ?? []).map((s: any) => ({
			...s,
			...Object.fromEntries(POSITIONS.map(([k]) => [`pos_${k}`, s.positionShare?.[k] ?? null]))
		}));
	}

	$: rows = data ? rowsFor(currentMetric) : [];
	$: isLive = (year: number) => live != null && year === live.seasonYear;

	function fmt(s: SeriesDef, v: number | null): string {
		if (v == null) return '—';
		if (s.percent) return `${(v * 100).toFixed(s.decimals)}%`;
		return v.toLocaleString(undefined, { minimumFractionDigits: s.decimals, maximumFractionDigits: s.decimals }) + s.unit;
	}

	const pct = (v: number | null | undefined, digits = 0) => (v == null ? '—' : `${(v * 100).toFixed(digits)}%`);

	// Insights, finished seasons only.
	$: insights = (() => {
		const m = currentMetric;
		const done = rows.filter((r) => !isLive(r.seasonYear));
		if (m.kind === 'champion') {
			const h = headline;
			if (!h) return [];
			const worst = done
				.filter((r) => r.champion?.pointsRank != null)
				.reduce((a: any, r: any) => (a == null || r.champion.pointsRank > a.champion.pointsRank ? r : a), null);
			return [
				`A top-three scorer won ${h.championTopThree.count} of ${h.championTopThree.seasons} titles; the season's top scorer won ${h.topScorerTitles}.`,
				worst ? `Longest shot: ${worst.champion.manager} in ${worst.seasonYear}, the #${worst.champion.pointsRank} scorer.` : '',
				'Records are mostly luck year to year, but titles still mostly go to teams that score.'
			].filter(Boolean);
		}
		if (m.kind === 'titles') {
			const t = trends?.titles ?? [];
			if (!t.length) return [];
			const total = t.reduce((a: number, x: any) => a + x.years.length, 0);
			const lead = t[0];
			const finals = (m: any) => m.years.length + m.finals.length;
			const mostFinals = t.reduce((a: any, m: any) => (finals(m) > finals(a) ? m : a));
			const champs = t.filter((m: any) => m.years.length);
			const never = t.filter((m: any) => !m.years.length && m.finals.length).sort((a: any, b: any) => b.finals.length - a.finals.length);
			const recent = done.slice(-10).map((r) => r.champion?.manager).filter(Boolean);
			return [
				`${lead.manager} has ${lead.years.length} of ${total} titles (${lead.years[0]}–${lead.years[lead.years.length - 1]}) and is ${lead.years.length}–${lead.finals.length} in finals.`,
				mostFinals.manager !== lead.manager
					? `Most finals: ${mostFinals.manager}, ${finals(mostFinals)} (${mostFinals.years.length} won).`
					: '',
				never.length
					? `Reached a final, never won one: ${never.map((m: any) => `${m.manager} (0–${m.finals.length})`).join(', ')}.`
					: '',
				`${champs.length} different champions in ${total} seasons; ${new Set(recent).size} in the last ${recent.length}.`
			].filter(Boolean);
		}
		if (m.kind === 'stacked') {
			const first = done.find((r) => r.positionShare);
			const last = [...done].reverse().find((r) => r.positionShare);
			if (!first || !last) return [];
			return POSITIONS.slice(0, 3).map(
				([k, label]) => `${label}: ${pct(first.positionShare[k])} in ${first.seasonYear} → ${pct(last.positionShare[k])} in ${last.seasonYear}`
			);
		}
		const s = m.series[0];
		const pts = done
			.map((r) => ({ year: r.seasonYear as number, val: read(r, s.key) }))
			.filter((p): p is { year: number; val: number } => p.val != null);
		if (pts.length < 2) return [];
		const first = pts[0];
		const last = pts[pts.length - 1];
		const peak = pts.reduce((a, p) => (p.val > a.val ? p : a));
		const low = pts.reduce((a, p) => (p.val < a.val ? p : a));
		const avg = pts.reduce((a, p) => a + p.val, 0) / pts.length;
		const out = [
			`${s.label}: ${fmt(s, first.val)} in ${first.year} → ${fmt(s, last.val)} in ${last.year}`,
			`Peak ${fmt(s, peak.val)} (${peak.year}) · low ${fmt(s, low.val)} (${low.year}) · ${pts.length}-season average ${fmt(s, avg)}`
		];
		if (m.id === 'balance' && headline) {
			const avg5 = (list: typeof pts) => list.reduce((a, p) => a + p.val, 0) / list.length;
			if (pts.length >= 10) {
				const early = pts.slice(0, 5);
				const late = pts.slice(-5);
				out.push(
					`Five-season average: ${avg5(early).toFixed(2)} in ${early[0].year}–${early[4].year} → ${avg5(late).toFixed(2)} in ${late[0].year}–${late[4].year}. Records spread less than they used to; recent seasons sit near what coin flips would give.`
				);
			}
			const below = pts.filter((p) => p.val < 1.05).length;
			out.push(`${below} of ${pts.length} seasons were within 5% of a coin-flip league; on average ${pct(headline.skillShare)} of the spread in records was skill.`);
		}
		if (live) out.push(`${live.seasonYear} is still being played and isn't included.`);
		return out;
	})();

	// ---- rendering -----------------------------------------------------------

	const TEXT_PRIMARY = '#f1f5f9';
	const TEXT_SECONDARY = '#cbd5e1';
	const TEXT_MUTED = '#94a3b8';
	const GRID = '#334155';
	const SURFACE = '#1c2746';
	const PANEL_GAP = 48;

	let hoverIndex: number | null = null;
	let resizeObserver: ResizeObserver | null = null;
	let reduceMotion = false;

	const dur = (ms: number) => (reduceMotion ? 0 : ms);

	// Below this width the championship grid turns on its side.
	const isVerticalGrid = (width: number) => width < 520;

	function marginFor(m: MetricDef, view: ViewId, width: number) {
		if (m.kind === 'titles' && view === 'grid') {
			if (isVerticalGrid(width)) return { top: 84, right: 6, bottom: 30, left: 40 };
			return width < 760
				? { top: 38, right: 28, bottom: 30, left: 112 }
				: { top: 38, right: 150, bottom: 30, left: 168 };
		}
		if (m.kind === 'titles') return width < 520 ? { top: 30, right: 62, bottom: 34, left: 28 } : { top: 30, right: 104, bottom: 34, left: 40 };
		return { top: m.kind === 'stacked' ? 30 : 16, right: 92, bottom: 34, left: 56 };
	}

	function render(animate: boolean) {
		if (!d3 || !container) return;
		const metric = currentMetric;
		const view = selectedView;
		const width = container.clientWidth;
		if (width <= 0) return;
		if (!rows.length && metric.kind !== 'titles') return;

		const margin = marginFor(metric, view, width);
		const plotW = Math.max(120, width - margin.left - margin.right);
		let plotH: number;
		let panels = 1;
		if (metric.kind === 'titles' && view === 'grid') {
			const seasons = (trends?.seasons ?? []).filter((s: any) => s.champion).length;
			plotH = isVerticalGrid(width) ? Math.max(1, seasons) * 24 : Math.max(1, trends?.titles?.length ?? 1) * 34;
		}
		else if (metric.kind === 'series' && metric.layout === 'panels') {
			panels = metric.series.length;
			plotH = panels * 190 + (panels - 1) * PANEL_GAP;
		} else plotH = 360;
		const height = margin.top + plotH + margin.bottom;

		const root = d3.select(container);
		let svg = root.select('svg.viz');
		if (svg.empty()) svg = root.append('svg').attr('class', 'viz').attr('role', 'img');
		svg
			.attr('aria-label', `${metric.name}: ${metric.question}`)
			.attr('width', width)
			.transition()
			.duration(dur(animate ? 350 : 0))
			.attr('viewBox', `0 0 ${width} ${height}`)
			.attr('height', height);

		svg
			.selectAll('g.frame')
			.attr('class', 'frame-old')
			.style('pointer-events', 'none')
			.transition()
			.duration(dur(animate ? 220 : 0))
			.style('opacity', 0)
			.remove();
		const frame = svg.append('g').attr('class', 'frame').attr('transform', `translate(${margin.left},${margin.top})`);
		ensureDefs(svg, metric);
		hideTooltip();
		hoverIndex = null;

		const ctx = { frame, metric, view, plotW, plotH, margin, animate };
		if (metric.kind === 'series') drawSeries(ctx);
		else if (metric.kind === 'champion') drawChampion(ctx);
		else if (metric.kind === 'titles') (view === 'race' ? drawTitleRace : drawTitleGrid)(ctx);
		else if (metric.kind === 'stacked') drawStacked(ctx);
	}

	interface Ctx {
		frame: any;
		metric: MetricDef;
		view: ViewId;
		plotW: number;
		plotH: number;
		margin: { top: number; right: number; bottom: number; left: number };
		animate: boolean;
	}

	function ensureDefs(svg: any, metric: MetricDef) {
		let defs = svg.select('defs');
		if (defs.empty()) defs = svg.append('defs');
		if (defs.select('#avatar-clip').empty()) {
			defs.append('clipPath').attr('id', 'avatar-clip').attr('clipPathUnits', 'objectBoundingBox')
				.append('circle').attr('cx', 0.5).attr('cy', 0.5).attr('r', 0.5);
		}
		for (const s of metric.series) {
			const id = `grad-${s.key}`;
			if (!defs.select(`#${id}`).empty()) continue;
			const g = defs.append('linearGradient').attr('id', id).attr('x1', 0).attr('x2', 0).attr('y1', 0).attr('y2', 1);
			g.append('stop').attr('offset', '0%').attr('stop-color', s.color).attr('stop-opacity', 0.22);
			g.append('stop').attr('offset', '100%').attr('stop-color', s.color).attr('stop-opacity', 0.02);
		}
	}

	function styleAxis(sel: any) {
		sel.select('.domain').remove();
		sel.selectAll('.tick line').attr('stroke', GRID);
		sel.selectAll('.tick text').attr('fill', TEXT_MUTED).style('font-size', '11px').style('font-variant-numeric', 'tabular-nums');
	}

	function seasonAxis(frame: any, x: any, y: number, plotW: number, count: number, animate: boolean) {
		const a = frame
			.append('g')
			.attr('transform', `translate(0,${y})`)
			.call(d3.axisBottom(x).ticks(Math.min(count, Math.floor(plotW / 64))).tickFormat(d3.format('d')).tickSize(0).tickPadding(10))
			.call(styleAxis);
		if (animate) a.style('opacity', 0).transition().duration(dur(400)).style('opacity', 1);
	}

	function title(p: any, text: string, y = -4) {
		p.append('text').attr('x', 0).attr('y', y).attr('fill', TEXT_SECONDARY).style('font-size', '12px').style('font-weight', '600').text(text);
	}

	/**
	 * A season crosshair over [0, bottom] that snaps to the nearest of `list`
	 * (the rows this chart actually plots) and calls back with it.
	 */
	function crosshair(
		ctx: Ctx,
		x: any,
		bottom: number,
		step: number,
		list: any[],
		dotsFor: (r: any) => { cy: number; color: string }[],
		tip: (r: any) => void
	) {
		const { frame, plotW, margin } = ctx;
		const cross = frame.append('g').style('display', 'none').style('pointer-events', 'none');
		cross.append('line').attr('y1', -6).attr('y2', bottom).attr('stroke', TEXT_MUTED).attr('stroke-opacity', 0.6);
		const dots = cross.append('g');
		const show = (i: number) => {
			hoverIndex = i;
			const r = list[i];
			cross.style('display', null).attr('transform', `translate(${x(r.seasonYear)},0)`);
			dots.selectAll('*').remove();
			for (const d of dotsFor(r)) {
				dots.append('circle').attr('cy', d.cy).attr('r', 5).attr('fill', d.color).attr('stroke', SURFACE).attr('stroke-width', 2);
			}
			tip(r);
			positionTooltip(x(r.seasonYear) + margin.left, plotW + margin.left + margin.right);
		};
		const hide = () => {
			hoverIndex = null;
			cross.style('display', 'none');
			hideTooltip();
		};
		frame
			.append('rect')
			.attr('x', -step / 2)
			.attr('y', -margin.top)
			.attr('width', plotW + step)
			.attr('height', bottom + margin.top)
			.attr('fill', 'transparent')
			.on('pointermove', (event: PointerEvent) => {
				const [mx] = d3.pointer(event);
				const year = x.invert(mx);
				let best = 0;
				list.forEach((r, i) => {
					if (Math.abs(r.seasonYear - year) < Math.abs(list[best].seasonYear - year)) best = i;
				});
				show(best);
			})
			.on('pointerleave', hide);
		navLength = list.length;
		keyboardShow = show;
		keyboardHide = hide;
	}

	// ---- series: a level over time (line / bars), shared axis or panels -------

	function drawSeries(ctx: Ctx) {
		const { frame, metric, view, plotW, animate } = ctx;
		// Seasons with something to plot (the ratio has none for a season in progress).
		const plotted = rows.filter((r) => metric.series.some((s) => read(r, s.key) != null));
		const years = plotted.map((r) => r.seasonYear as number);
		const roughStep = years.length > 1 ? plotW / (years.length - 1) : plotW;
		const barW = Math.min(24, Math.max(4, roughStep * 0.55));
		const inset = view === 'bar' ? barW / 2 + 6 : 0;
		const x = d3.scaleLinear().domain(d3.extent(years)).range([inset, plotW - inset]);
		const step = years.length > 1 ? (plotW - 2 * inset) / (years.length - 1) : plotW;
		const ease = d3.easeCubicOut;
		const panelH = metric.layout === 'panels' ? 190 : ctx.plotH;
		const groups = metric.layout === 'panels' ? metric.series.map((s) => [s]) : [metric.series];
		const yScales: any[] = [];

		groups.forEach((inPanel, pi) => {
			const top = pi * (panelH + PANEL_GAP);
			const p = frame.append('g').attr('transform', `translate(0,${top})`);
			const vals: number[] = [];
			for (const r of rows) {
				for (const s of inPanel) {
					const v = read(r, s.key);
					if (v != null) vals.push(v);
				}
				if (pi === 0) for (const b of metric.bands ?? []) {
					for (const k of [b.lo, b.hi]) {
						const v = read(r, k);
						if (v != null) vals.push(v);
					}
				}
			}
			if (metric.refLine) vals.push(metric.refLine.value);
			const base = metric.refLine?.value ?? 0;
			const fromZero = metric.layout === 'panels' || (view === 'bar' && !metric.refLine);
			const [lo, hi] = d3.extent(vals);
			const span = hi - lo || 1;
			const y = d3
				.scaleLinear()
				.domain([fromZero ? Math.min(0, lo) : lo - span * 0.08, hi + span * 0.06])
				.nice(5)
				.range([panelH, 0]);
			yScales.push(y);

			const yAxis = p
				.append('g')
				.call(d3.axisLeft(y).ticks(panelH < 220 ? 4 : 6).tickSize(-plotW).tickFormat(d3.format(metric.id === 'balance' ? '.1f' : '~s')))
				.call(styleAxis);
			yAxis.selectAll('.tick line').attr('stroke-opacity', 0.6);
			if (animate) yAxis.style('opacity', 0).transition().duration(dur(400)).style('opacity', 1);
			const t = metric.layout === 'panels' ? inPanel[0].label : metric.axisLabel;
			if (t) title(p, t);

			// Reference line: what the measure reads in a league of coin flips.
			if (metric.refLine) {
				const ry = y(metric.refLine.value);
				p.append('line').attr('x1', 0).attr('x2', plotW).attr('y1', ry).attr('y2', ry).attr('stroke', TEXT_MUTED).attr('stroke-width', 1).attr('stroke-opacity', 0.8);
				// Left end: the early seasons ran well above chance, so the label sits clear.
				p.append('text').attr('x', 4).attr('y', ry + 14).attr('fill', TEXT_MUTED).style('font-size', '11px').text(metric.refLine.label);
			}

			if (pi === 0 && metric.bands) {
				for (const b of metric.bands) {
					const band = d3.area().defined((r: any) => read(r, b.lo) != null && read(r, b.hi) != null)
						.x((r: any) => x(r.seasonYear)).y0((r: any) => y(read(r, b.lo))).y1((r: any) => y(read(r, b.hi))).curve(d3.curveMonotoneX);
					const collapsed = d3.area().defined((r: any) => read(r, b.lo) != null)
						.x((r: any) => x(r.seasonYear)).y0((r: any) => y(read(r, metric.series[0].key))).y1((r: any) => y(read(r, metric.series[0].key))).curve(d3.curveMonotoneX);
					const path = p.append('path').datum(rows).attr('fill', metric.series[0].color).attr('fill-opacity', b.opacity);
					if (animate) path.attr('d', collapsed).transition().delay(dur(350)).duration(dur(700)).ease(ease).attr('d', band);
					else path.attr('d', band);
				}
			}

			for (const s of inPanel) {
				const pts = rows.filter((r) => read(r, s.key) != null);
				if (view === 'bar' && !s.overlay) {
					const off = inPanel.filter((x) => !x.overlay).length > 1 ? (inPanel.indexOf(s) - 0.5) * (barW / 2 + 1) : 0;
					const barPath = (r: any, v: number) => {
						const x0 = x(r.seasonYear) - barW / 2;
						const y0 = y(base);
						const y1 = y(v);
						const up = y1 <= y0;
						const h = Math.abs(y0 - y1);
						const rr = Math.min(4, h, barW / 2);
						// Rounded at the data end, square at the baseline (which is 1.0
						// for the ratio, so bars point up or down from chance).
						return up
							? `M${x0},${y0}V${y1 + rr}Q${x0},${y1} ${x0 + rr},${y1}H${x0 + barW - rr}Q${x0 + barW},${y1} ${x0 + barW},${y1 + rr}V${y0}Z`
							: `M${x0},${y0}V${y1 - rr}Q${x0},${y1} ${x0 + rr},${y1}H${x0 + barW - rr}Q${x0 + barW},${y1} ${x0 + barW},${y1 - rr}V${y0}Z`;
					};
					p.append('g').attr('transform', `translate(${off},0)`).selectAll('path').data(pts).join('path')
						.attr('fill', s.color)
						.attr('fill-opacity', (r: any) => (isLive(r.seasonYear) ? 0.45 : 0.9))
						.attr('d', (r: any) => barPath(r, base))
						.transition()
						.delay((_: any, i: number) => dur(animate ? 120 + i * 22 : 0))
						.duration(dur(animate ? 600 : 0))
						.ease(ease)
						.attr('d', (r: any) => barPath(r, read(r, s.key)!));
					continue;
				}

				const finished = pts.filter((r) => !isLive(r.seasonYear));
				const liveRow = pts.find((r) => isLive(r.seasonYear));
				const line = d3.line().x((r: any) => x(r.seasonYear)).y((r: any) => y(read(r, s.key))).curve(d3.curveMonotoneX);
				const path = p.append('path').datum(finished).attr('fill', 'none').attr('stroke', s.color)
					.attr('stroke-width', 2).attr('stroke-linejoin', 'round').attr('stroke-linecap', 'round').attr('d', line);
				if (animate) {
					const len = path.node().getTotalLength();
					path.attr('stroke-dasharray', `${len} ${len}`).attr('stroke-dashoffset', len)
						.transition().delay(dur(s.overlay ? 500 : 0)).duration(dur(1100)).ease(d3.easeCubicInOut).attr('stroke-dashoffset', 0)
						.on('end', function (this: any) {
							d3.select(this).attr('stroke-dasharray', null);
						});
				}
				if (liveRow && finished.length) {
					const last = finished[finished.length - 1];
					p.append('path').attr('fill', 'none').attr('stroke', s.color).attr('stroke-width', 2).attr('stroke-dasharray', '2 4').attr('stroke-linecap', 'round')
						.attr('d', `M${x(last.seasonYear)},${y(read(last, s.key))}L${x(liveRow.seasonYear)},${y(read(liveRow, s.key))}`)
						.style('opacity', 0).transition().delay(dur(animate ? 1100 : 0)).duration(dur(300)).style('opacity', 1);
					p.append('circle').attr('cx', x(liveRow.seasonYear)).attr('cy', y(read(liveRow, s.key))).attr('r', 4.5)
						.attr('fill', SURFACE).attr('stroke', s.color).attr('stroke-width', 2);
				}
				if (finished.length > 2 && s === inPanel[0] && !s.overlay) {
					const peak = finished.reduce((a, r) => (read(r, s.key)! > read(a, s.key)! ? r : a));
					const low = finished.reduce((a, r) => (read(r, s.key)! < read(a, s.key)! ? r : a));
					for (const [r, where] of [[peak, 'peak'], [low, 'low']] as const) {
						const cx = x(r.seasonYear);
						const cy = y(read(r, s.key));
						const above = where === 'peak' ? cy - 10 >= 14 : cy + 18 > panelH - 4;
						const g = p.append('g').style('opacity', 0);
						g.append('circle').attr('cx', cx).attr('cy', cy).attr('r', 4).attr('fill', s.color).attr('stroke', SURFACE).attr('stroke-width', 2);
						g.append('text').attr('x', cx).attr('y', above ? cy - 10 : cy + 18)
							.attr('text-anchor', cx < 40 ? 'start' : cx > plotW - 40 ? 'end' : 'middle')
							.attr('fill', TEXT_SECONDARY).style('font-size', '11px')
							.text(`${where === 'peak' ? 'Peak' : 'Low'} ${fmt(s, read(r, s.key))} · ${r.seasonYear}`);
						g.transition().delay(dur(animate ? 900 : 0)).duration(dur(400)).style('opacity', 1);
					}
				}
				const end = liveRow ?? finished[finished.length - 1];
				if (end && !(view === 'bar' && !s.overlay)) {
					const ex = x(end.seasonYear);
					// Near the floor the two lines go above the point, clear of the axis.
					const ey = y(read(end, s.key)) - (y(read(end, s.key)) + 18 > panelH - 4 ? 16 : 0);
					const label = p.append('g').style('opacity', 0);
					label.append('text').attr('x', ex + 10).attr('y', ey + 4).attr('fill', TEXT_PRIMARY).style('font-size', '12px').style('font-weight', '600').text(fmt(s, read(end, s.key)));
					label.append('text').attr('x', ex + 10).attr('y', ey + 18).attr('fill', TEXT_MUTED).style('font-size', '10px')
						.text(isLive(end.seasonYear) && live ? `${end.seasonYear} · wk ${live.throughWeek}` : s.overlay ? '5-yr avg' : `${end.seasonYear}`);
					label.transition().delay(dur(animate ? 1100 : 0)).duration(dur(300)).style('opacity', 1);
				}
			}
		});

		const bottom = groups.length * panelH + (groups.length - 1) * PANEL_GAP;
		seasonAxis(frame, x, bottom, plotW, years.length, animate);
		crosshair(
			ctx,
			x,
			bottom,
			step,
			plotted,
			(r) =>
				view === 'bar'
					? []
					: groups.flatMap((inPanel, pi) =>
							inPanel
								.map((s) => ({ s, v: read(r, s.key) }))
								.filter((d) => d.v != null)
								.map((d) => ({ cy: pi * (panelH + PANEL_GAP) + yScales[pi](d.v), color: d.s.color }))
						),
			(r) => {
				const lines: TipLine[] = metric.series.map((s) => ({ color: s.color, value: fmt(s, read(r, s.key)), label: s.label }));
				for (const b of metric.bands ?? []) {
					lines.push({ value: `${fmt(metric.series[0], read(r, b.lo))} – ${fmt(metric.series[0], read(r, b.hi))}`, label: b.label });
				}
				if (metric.source === 'scoring' && read(r, 'maxWeeklyScore') != null) {
					lines.push({ value: fmt(metric.series[0], read(r, 'maxWeeklyScore')), label: 'Best single week' });
				}
				if (metric.id === 'balance' && r.skillShare != null) {
					lines.push({ value: pct(r.skillShare), label: 'of the spread was skill' });
				}
				showTooltip(seasonHead(r), lines);
			}
		);
	}

	// ---- champion: rank in scoring, one dot per season -------------------------

	function drawChampion(ctx: Ctx) {
		const { frame, plotW, plotH, animate } = ctx;
		const champs = rows.filter((r) => r.champion?.pointsRank != null);
		if (!champs.length) return;
		const maxTeams = d3.max(champs, (r: any) => r.teams) ?? 10;
		const x = d3.scaleLinear().domain(d3.extent(champs, (r: any) => r.seasonYear)).range([12, plotW - 12]);
		// Rank 1 at the top, with room above it for the chart title.
		const y = d3.scaleLinear().domain([1, maxTeams]).range([18, plotH]);
		const step = champs.length > 1 ? (plotW - 24) / (champs.length - 1) : plotW;

		// Top-three band.
		frame.append('rect').attr('x', 0).attr('width', plotW).attr('y', y(0.5)).attr('height', y(3.5) - y(0.5))
			.attr('fill', BLUE).attr('fill-opacity', 0.08);
		frame.append('text').attr('x', plotW - 4).attr('y', y(0.5) + 12).attr('text-anchor', 'end').attr('fill', TEXT_MUTED).style('font-size', '11px').text('Top 3 scorers');
		frame.append('g')
			.call(d3.axisLeft(y).ticks(maxTeams).tickSize(-plotW).tickFormat((d: number) => `#${d}`))
			.call(styleAxis)
			.selectAll('.tick line')
			.attr('stroke-opacity', 0.35);
		title(frame, 'Champion’s scoring rank');

		// A thin stem from the top so distance from #1 reads, then the dot.
		const g = frame.append('g');
		champs.forEach((r, i) => {
			const cx = x(r.seasonYear);
			const cy = y(r.champion.pointsRank);
			const delay = dur(animate ? 80 + i * 40 : 0);
			g.append('line').attr('x1', cx).attr('x2', cx).attr('y1', y(1)).attr('y2', y(1)).attr('stroke', BLUE).attr('stroke-opacity', 0.35).attr('stroke-width', 1.5)
				.transition().delay(delay).duration(dur(animate ? 400 : 0)).ease(d3.easeCubicOut).attr('y2', cy);
			g.append('circle').attr('cx', cx).attr('cy', y(1)).attr('r', 0).attr('fill', r.champion.pointsRank === 1 ? BLUE : SURFACE)
				.attr('stroke', BLUE).attr('stroke-width', 2)
				.transition().delay(delay).duration(dur(animate ? 400 : 0)).ease(d3.easeCubicOut).attr('cy', cy).attr('r', 6);
		});
		frame.append('text').attr('x', 0).attr('y', plotH + 30).attr('fill', TEXT_MUTED).style('font-size', '11px').text('Filled: the top scorer won it all');

		seasonAxis(frame, x, plotH, plotW, champs.length, animate);
		crosshair(
			ctx,
			x,
			plotH,
			step,
			champs,
			(r) => (r.champion?.pointsRank != null ? [{ cy: y(r.champion.pointsRank), color: BLUE }] : []),
			(r) => {
				if (!r.champion) return showTooltip(seasonHead(r), [{ value: 'No champion yet', label: '' }]);
				showTooltip(seasonHead(r), [
					{ color: BLUE, value: r.champion.manager, label: 'won the title' },
					{ value: `#${r.champion.pointsRank}`, label: `in points per game, of ${r.teams}` },
					{ value: `#${r.champion.recordRank}`, label: 'in regular-season record' }
				]);
			}
		);
	}

	// ---- championships: who won, who came close -------------------------------

	/** A manager's photo in a circle, or their initials when there's no photo. */
	function avatar(g: any, manager: string, r: number) {
		g.append('circle').attr('r', r + 1.5).attr('fill', SURFACE).attr('stroke', GRID).attr('stroke-width', 1);
		const url = getManagerProfilePicture(manager);
		if (url) {
			g.append('image')
				.attr('href', url)
				.attr('x', -r)
				.attr('y', -r)
				.attr('width', 2 * r)
				.attr('height', 2 * r)
				.attr('preserveAspectRatio', 'xMidYMid slice')
				.attr('clip-path', 'url(#avatar-clip)');
		} else {
			g.append('circle').attr('r', r).attr('fill', '#334155');
			g.append('text')
				.attr('text-anchor', 'middle')
				.attr('dy', '0.35em')
				.attr('fill', TEXT_SECONDARY)
				.style('font-size', `${Math.round(r * 0.8)}px`)
				.style('font-weight', '600')
				.text(getInitials(manager).slice(0, 2));
		}
		g.append('title').text(manager);
	}

	function finalTip(season: any): TipLine[] {
		const c = season?.champion;
		if (!c) return [];
		const t = (trends?.titles ?? []).find((m: any) => m.manager === c.manager);
		const nth = t ? t.years.indexOf(season.seasonYear) + 1 : null;
		const lines: TipLine[] = [{ color: YELLOW, value: c.manager, label: nth ? `won · title #${nth}` : 'won' }];
		if (c.runnerUp) lines.push({ value: c.runnerUp, label: 'lost the final' });
		if (c.score) lines.push({ value: `${c.score[0].toFixed(1)}–${c.score[1].toFixed(1)}`, label: 'final score' });
		if (c.pointsRank) lines.push({ value: `#${c.pointsRank}`, label: 'scorer in the regular season' });
		return lines;
	}

	/**
	 * Grid: a row per finalist, a column per season (turned on its side on a
	 * phone: a photo column per finalist, a row per season). Filled = title,
	 * outlined = final lost, faint = in the league.
	 */
	function drawTitleGrid(ctx: Ctx) {
		const { frame, plotW, plotH, margin, animate } = ctx;
		const t = trends?.titles ?? [];
		const finished = rows.filter((r) => !r.inProgress && r.champion);
		if (!t.length || !finished.length) return;
		const years = finished.map((r) => r.seasonYear as number);
		const vertical = isVerticalGrid(plotW + margin.left + margin.right);
		const season = d3.scaleBand().domain(years).range([0, vertical ? plotH : plotW]).paddingInner(0.14);
		const person = d3.scaleBand().domain(t.map((m: any) => m.manager)).range([0, vertical ? plotW : plotH]).paddingInner(0.2);
		// Tile size across (cw) and down (ch).
		const cw = vertical ? person.bandwidth() : season.bandwidth();
		const ch = vertical ? season.bandwidth() : person.bandwidth();
		const along = (year: number) => (vertical ? `translate(0,${season(year)})` : `translate(${season(year)},0)`);
		const rx = Math.min(4, Math.min(cw, ch) / 4);
		const fullW = plotW + margin.left + margin.right;

		// Legend, above the grid; wraps when it runs out of room.
		const legend = frame.append('g').attr('transform', `translate(${vertical ? -margin.left + 4 : 0},${-margin.top + 8})`);
		const items: [string, (g: any) => void][] = [
			['Won the title', (g) => g.append('rect').attr('width', 12).attr('height', 12).attr('rx', 3).attr('fill', YELLOW)],
			['Lost the final', (g) => g.append('rect').attr('x', 0.75).attr('y', 0.75).attr('width', 10.5).attr('height', 10.5).attr('rx', 3).attr('fill', 'none').attr('stroke', YELLOW).attr('stroke-width', 1.5)],
			['In the league', (g) => g.append('rect').attr('width', 12).attr('height', 12).attr('rx', 3).attr('fill', TEXT_PRIMARY).attr('fill-opacity', 0.07)]
		];
		let lx = 0;
		let ly = 0;
		for (const [label, mark] of items) {
			const g = legend.append('g');
			mark(g);
			const text = g.append('text').attr('x', 18).attr('y', 10).attr('fill', TEXT_SECONDARY).style('font-size', '11px').text(label);
			const w = 18 + (text.node()?.getComputedTextLength?.() || label.length * 6);
			if (lx > 0 && lx + w > fullW - 8) {
				lx = 0;
				ly += 18;
			}
			g.attr('transform', `translate(${lx},${ly})`);
			lx += w + 18;
		}

		// Hover highlight for one season, across every finalist.
		const seasonHi = frame.append('rect').attr('rx', 6).attr('fill', TEXT_PRIMARY).attr('fill-opacity', 0.06)
			.style('display', 'none').style('pointer-events', 'none');
		if (vertical) seasonHi.attr('x', -4).attr('width', plotW + 8).attr('height', ch + 6);
		else seasonHi.attr('y', -4).attr('height', plotH + 8).attr('width', cw + 6);

		const lanes = frame.append('g').selectAll('g.lane').data(t).join('g').attr('class', 'lane')
			.attr('transform', (m: any) => (vertical ? `translate(${person(m.manager)},0)` : `translate(0,${person(m.manager)})`));

		// Who: photo and name left of each row, or a photo over each column.
		const shortName = (name: string) => {
			const parts = name.split(' ');
			const clash = t.filter((m: any) => m.manager.split(' ')[0] === parts[0]).length > 1;
			return clash ? `${parts[0]} ${parts[parts.length - 1][0].toUpperCase()}.` : parts[0];
		};
		const narrowNames = margin.left < 150;
		lanes.each(function (this: any, m: any) {
			const g = d3.select(this);
			if (vertical) {
				avatar(g.append('g').attr('transform', `translate(${cw / 2},-16)`), m.manager, Math.min(11, cw / 2 - 1));
				return;
			}
			avatar(g.append('g').attr('transform', `translate(${-margin.left + 18},${ch / 2})`), m.manager, Math.min(12, ch / 2));
			g.append('text').attr('x', -margin.left + 36).attr('y', ch / 2).attr('dy', '0.35em')
				.attr('fill', TEXT_SECONDARY).style('font-size', '12px')
				.text(narrowNames ? shortName(m.manager) : m.manager);
		});

		// Tiles, swept in season by season.
		lanes.each(function (this: any, m: any) {
			const g = d3.select(this);
			const played = new Set(m.seasons);
			const won = new Set(m.years);
			const lost = new Set(m.finals);
			years.forEach((year, col) => {
				if (!played.has(year) && !won.has(year) && !lost.has(year)) return;
				const cell = g.append('g').attr('transform', `${along(year)} translate(${cw / 2},${ch / 2})`);
				const inner = cell.append('g').attr('transform', animate && !reduceMotion ? 'scale(0)' : null);
				const tile = () => inner.append('rect').attr('x', -cw / 2).attr('y', -ch / 2).attr('width', cw).attr('height', ch).attr('rx', rx);
				tile().attr('fill', TEXT_PRIMARY).attr('fill-opacity', 0.07);
				if (won.has(year)) {
					tile().attr('fill', YELLOW);
					if (cw >= 16 && ch >= 14) {
						inner.append('text').attr('text-anchor', 'middle').attr('dy', '0.35em').attr('fill', SURFACE)
							.style('font-size', '11px').style('font-weight', '700').style('font-variant-numeric', 'tabular-nums')
							.text(String(m.years.indexOf(year) + 1));
					}
				} else if (lost.has(year)) {
					inner.append('rect').attr('x', -cw / 2 + 1).attr('y', -ch / 2 + 1).attr('width', cw - 2).attr('height', ch - 2).attr('rx', rx)
						.attr('fill', 'none').attr('stroke', YELLOW).attr('stroke-width', 1.5);
				}
				if (animate && !reduceMotion) {
					inner.transition().delay(80 + col * 45).duration(320).ease(d3.easeBackOut.overshoot(won.has(year) ? 2.2 : 1)).attr('transform', 'scale(1)');
				}
			});
		});

		// Totals: titles as a bar with "of N finals" beside each row, or a count under each column.
		const maxTitles = d3.max(t, (m: any) => m.years.length) ?? 1;
		const totalsDelay = dur(animate ? 80 + years.length * 45 : 0);
		lanes.each(function (this: any, m: any) {
			const g = d3.select(this);
			if (vertical) {
				g.append('text').attr('x', cw / 2).attr('y', plotH + 18).attr('text-anchor', 'middle').attr('fill', TEXT_PRIMARY)
					.style('font-size', '12px').style('font-weight', '600').style('font-variant-numeric', 'tabular-nums')
					.text(String(m.years.length));
				return;
			}
			const bar = margin.right >= 100 ? 54 : 0;
			const bx = d3.scaleLinear().domain([0, maxTitles]).range([0, bar]);
			const tg = g.append('g').attr('transform', `translate(${plotW + 12},${ch / 2})`);
			if (bar) {
				tg.append('rect').attr('y', -4).attr('height', 8).attr('rx', 2).attr('fill', YELLOW).attr('width', 0)
					.transition().delay(totalsDelay).duration(dur(animate ? 500 : 0)).ease(d3.easeCubicOut)
					.attr('width', Math.max(m.years.length ? 2 : 0, bx(m.years.length)));
			}
			const label = tg.append('text').attr('x', bar ? bar + 8 : 0).attr('dy', '0.35em').style('font-variant-numeric', 'tabular-nums');
			label.append('tspan').attr('fill', TEXT_PRIMARY).style('font-size', '12px').style('font-weight', '600').text(String(m.years.length));
			if (bar) label.append('tspan').attr('fill', TEXT_MUTED).style('font-size', '11px').text(` of ${m.years.length + m.finals.length} finals`);
		});

		// Season labels: under the columns, or left of the rows.
		const axis = frame.append('g');
		years.forEach((year, i) => {
			const text = axis.append('text').attr('fill', TEXT_MUTED).style('font-size', '11px').style('font-variant-numeric', 'tabular-nums');
			if (vertical) {
				text.attr('x', -8).attr('y', season(year) + ch / 2).attr('dy', '0.35em').attr('text-anchor', 'end').text(String(year));
			} else {
				if (cw < 24 && i % 2) return text.remove();
				text.attr('x', season(year) + cw / 2).attr('y', plotH + 18).attr('text-anchor', 'middle')
					.text(cw < 30 ? `'${String(year).slice(2)}` : String(year));
			}
		});

		const focusLane = (manager: string | null) => {
			lanes.transition().duration(dur(150)).style('opacity', (m: any) => (manager == null || m.manager === manager ? 1 : 0.35));
		};
		const show = (col: number, manager: string | null = null) => {
			hoverIndex = col;
			const year = years[col];
			seasonHi.style('display', null);
			if (vertical) seasonHi.attr('y', season(year) - 3);
			else seasonHi.attr('x', season(year) - 3);
			focusLane(manager);
			const s = finished[col];
			const extra: TipLine[] = [];
			if (manager && manager !== s.champion?.manager && manager !== s.champion?.runnerUp) {
				const m = t.find((d: any) => d.manager === manager);
				extra.push({ value: manager, label: m?.seasons.includes(year) ? 'in the league, not in the final' : 'not in the league that season' });
			}
			showTooltip(String(year), [...finalTip(s), ...extra]);
			if (vertical) positionTooltip(4, fullW);
			else positionTooltip(season(year) + cw / 2 + margin.left, fullW);
		};
		const showManager = (m: any) => {
			hoverIndex = null;
			seasonHi.style('display', 'none');
			focusLane(m.manager);
			showTooltip(m.manager, [
				{ color: YELLOW, value: String(m.years.length), label: m.years.length === 1 ? 'title' : 'titles' },
				...(m.years.length ? [{ value: m.years.join(', '), label: '' }] : []),
				{ value: String(m.finals.length), label: m.finals.length === 1 ? 'final lost' : 'finals lost' },
				{ value: String(m.seasons.length), label: 'seasons played' }
			]);
			positionTooltip(4, fullW);
		};
		const hide = () => {
			hoverIndex = null;
			seasonHi.style('display', 'none');
			focusLane(null);
			hideTooltip();
		};
		// One hit area over labels and tiles: which season, and whose lane.
		const laneAt = (p: number) => t.find((m: any) => p >= person(m.manager) - (person.step() - person.bandwidth()) / 2 && p < person(m.manager) + person.step());
		frame.append('rect').attr('x', -margin.left).attr('y', -margin.top + 30).attr('width', fullW).attr('height', plotH + margin.top - 30 + 8)
			.attr('fill', 'transparent')
			.on('pointermove', (event: PointerEvent) => {
				const [mx, my] = d3.pointer(event);
				const lane = laneAt(vertical ? mx : my);
				const s = vertical ? my : mx;
				if (s < 0) return lane ? showManager(lane) : hide();
				show(Math.max(0, Math.min(years.length - 1, Math.floor(s / season.step()))), lane?.manager ?? null);
			})
			.on('pointerleave', hide);
		navLength = years.length;
		keyboardShow = (i: number) => show(i);
		keyboardHide = hide;
	}

	/** Race: each manager's running title count, drawn left to right. */
	function drawTitleRace(ctx: Ctx) {
		const { frame, plotW, plotH, margin, animate } = ctx;
		const t = (trends?.titles ?? []).filter((m: any) => m.years.length);
		const finished = rows.filter((r) => !r.inProgress && r.champion);
		if (!t.length || !finished.length) return;
		const years = finished.map((r) => r.seasonYear as number);
		const first = years[0];
		const last = years[years.length - 1];
		const maxTitles = d3.max(t, (m: any) => m.years.length) ?? 1;
		const x = d3.scaleLinear().domain([first, last]).range([0, plotW]);
		const y = d3.scaleLinear().domain([0, maxTitles]).range([plotH, 0]);
		const step = plotW / Math.max(1, years.length - 1);
		const countAt = (m: any, year: number) => m.years.filter((v: number) => v <= year).length;
		// Lines that share a count would sit on top of each other; spread them 2.5px.
		const nudge = (i: number) => (i - (t.length - 1) / 2) * 2.5;

		frame.append('g').call(d3.axisLeft(y).ticks(maxTitles).tickFormat(d3.format('d')).tickSize(-plotW)).call(styleAxis)
			.selectAll('.tick line').attr('stroke-opacity', 0.35);
		title(frame, 'Titles won, running total', -16);
		seasonAxis(frame, x, plotH, plotW, years.length, animate);

		// Lines are revealed by a clip that sweeps left to right.
		const clipId = `race-clip-${Math.random().toString(36).slice(2, 8)}`;
		const clip = frame.append('clipPath').attr('id', clipId).append('rect').attr('x', -6).attr('y', -10).attr('height', plotH + 20)
			.attr('width', animate && !reduceMotion ? 0 : plotW + 12);
		const lineG = frame.append('g').attr('clip-path', `url(#${clipId})`);
		const line = d3.line().curve(d3.curveStepAfter).x((d: any) => x(d[0])).y((d: any) => d[1]);
		const lines: { m: any; g: any; end: number }[] = t.map((m: any, i: number) => {
			const start = Math.max(first, m.seasons[0] ?? first);
			const end = Math.min(last, m.seasons[m.seasons.length - 1] ?? last);
			const pts = years.filter((yr) => yr >= start && yr <= end).map((yr) => [yr, y(countAt(m, yr)) + nudge(i)]);
			const g = lineG.append('g');
			g.append('path').datum(pts).attr('d', line).attr('fill', 'none').attr('stroke', TEXT_MUTED).attr('stroke-width', 2)
				.attr('stroke-opacity', 0.55).attr('stroke-linejoin', 'round');
			// A dot on each title, where the line steps up.
			for (const yr of m.years) {
				g.append('circle').attr('cx', x(yr)).attr('cy', y(countAt(m, yr)) + nudge(i)).attr('r', 3.5).attr('fill', YELLOW)
					.attr('stroke', SURFACE).attr('stroke-width', 1.5);
			}
			return { m, g, end };
		});

		// Photos ride the front of each line, and settle at its end.
		const heads = t.map((m: any) => {
			const g = frame.append('g').style('cursor', 'pointer');
			avatar(g, m.manager, 11);
			return g;
		});
		// Tied photos sit side by side, overlapping on a narrow chart.
		const stack = margin.right < 80 ? 13 : 25;
		const place = (front: number) => {
			const year = Math.min(last, x.invert(front));
			const at = t.map((m: any, i: number) => {
				const end = lines[i].end;
				const yr = Math.min(year, end);
				const settled = year >= end;
				return { i, cx: settled ? x(end) : front, count: countAt(m, Math.floor(yr)), settled };
			});
			// Photos that land on the same spot step right, one photo apart.
			const groups = d3.group(at, (d: any) => `${d.count}|${Math.round(d.cx)}`);
			for (const list of groups.values()) {
				list.forEach((d: any, k: number) => {
					heads[d.i].attr('transform', `translate(${d.cx + 16 + k * stack},${y(d.count)})`);
				});
			}
		};
		if (animate && !reduceMotion) {
			place(0);
			const duration = 2600;
			frame.transition().duration(duration).ease(d3.easeCubicInOut).tween('race', () => (u: number) => {
				const front = u * plotW;
				clip.attr('width', front + 6);
				place(front);
			});
		} else {
			place(plotW);
		}

		const focus = (manager: string | null) => {
			lines.forEach(({ m, g }) => {
				const on = m.manager === manager;
				g.raise();
				g.select('path').attr('stroke', on ? TEXT_PRIMARY : TEXT_MUTED).attr('stroke-width', on ? 3 : 2)
					.attr('stroke-opacity', manager == null ? 0.55 : on ? 1 : 0.18);
				g.selectAll('circle').attr('opacity', manager == null || on ? 1 : 0.25);
			});
			const hi = lines.find((l) => l.m.manager === manager);
			hi?.g.raise();
			heads.forEach((h: any, i: number) => h.style('opacity', manager == null || t[i].manager === manager ? 1 : 0.35));
		};

		// Crosshair: the standings after any season, with that season's champion lit.
		const cross = frame.append('line').attr('y1', -6).attr('y2', plotH).attr('stroke', TEXT_MUTED).attr('stroke-opacity', 0.6)
			.style('display', 'none').style('pointer-events', 'none');
		const show = (i: number) => {
			hoverIndex = i;
			const season = finished[i];
			const year = season.seasonYear;
			cross.style('display', null).attr('x1', x(year)).attr('x2', x(year));
			focus(season.champion.manager);
			const standings = t
				.map((m: any) => ({ manager: m.manager, n: countAt(m, year) }))
				.filter((d: any) => d.n > 0)
				.sort((a: any, b: any) => b.n - a.n)
				.slice(0, 5);
			showTooltip(String(year), [
				...finalTip(season).slice(0, 2),
				...standings.map((d: any) => ({ value: String(d.n), label: d.manager }))
			]);
			positionTooltip(x(year) + margin.left, plotW + margin.left + margin.right);
		};
		const hide = () => {
			hoverIndex = null;
			cross.style('display', 'none');
			focus(null);
			hideTooltip();
		};
		frame.append('rect').attr('x', -step / 2).attr('y', -margin.top).attr('width', plotW + step).attr('height', plotH + margin.top)
			.attr('fill', 'transparent')
			.on('pointermove', (event: PointerEvent) => {
				const [mx] = d3.pointer(event);
				const i = d3.minIndex(years, (yr: number) => Math.abs(x(yr) - mx));
				show(i);
			})
			.on('pointerleave', hide);
		// Photos sit above the crosshair layer, so they can be hovered on their own.
		heads.forEach((h: any, i: number) => {
			const m = t[i];
			h.raise()
				.on('pointerenter', () => {
					cross.style('display', 'none');
					focus(m.manager);
					showTooltip(m.manager, [
						{ color: YELLOW, value: String(m.years.length), label: `titles of ${m.years.length + m.finals.length} finals` },
						{ value: m.years.join(', '), label: '' }
					]);
					positionTooltip(plotW + margin.left - 4, plotW + margin.left + margin.right);
				})
				.on('pointerleave', hide);
		});
		navLength = years.length;
		keyboardShow = show;
		keyboardHide = hide;
	}

	// ---- stacked: shares of a whole ------------------------------------------

	function drawStacked(ctx: Ctx) {
		const { frame, metric, view, plotW, plotH, animate } = ctx;
		const data2 = rows.filter((r) => r.positionShare);
		if (!data2.length) return;
		const keys = metric.series.map((s) => s.key);
		const stack = d3.stack().keys(keys).value((r: any, k: string) => read(r, k) ?? 0);
		const layers = stack(data2);
		const years = data2.map((r) => r.seasonYear);
		const roughStep = years.length > 1 ? plotW / (years.length - 1) : plotW;
		const barW = Math.min(24, Math.max(4, roughStep * 0.6));
		const inset = view === 'stackedBar' ? barW / 2 + 6 : 0;
		const x = d3.scaleLinear().domain(d3.extent(years)).range([inset, plotW - inset]);
		const y = d3.scaleLinear().domain([0, 1]).range([plotH, 0]);
		const step = years.length > 1 ? (plotW - 2 * inset) / (years.length - 1) : plotW;

		frame.append('g').call(d3.axisLeft(y).ticks(5).tickSize(-plotW).tickFormat(d3.format('.0%'))).call(styleAxis)
			.selectAll('.tick line').attr('stroke-opacity', 0.35);
		title(frame, 'Share of starting-lineup points', -16);

		layers.forEach((layer: any, li: number) => {
			const s = metric.series[li];
			if (view === 'stackedArea') {
				const area = d3.area().x((d: any) => x(d.data.seasonYear)).y0((d: any) => y(d[0])).y1((d: any) => y(d[1])).curve(d3.curveMonotoneX);
				const flat = d3.area().x((d: any) => x(d.data.seasonYear)).y0((d: any) => y(d[0])).y1((d: any) => y(d[0])).curve(d3.curveMonotoneX);
				const path = frame.append('path').datum(layer).attr('fill', s.color).attr('fill-opacity', 0.85).attr('stroke', SURFACE).attr('stroke-width', 1);
				if (animate) path.attr('d', flat).transition().delay(dur(li * 90)).duration(dur(600)).ease(d3.easeCubicOut).attr('d', area);
				else path.attr('d', area);
			} else {
				frame.append('g').selectAll('rect').data(layer).join('rect')
					.attr('x', (d: any) => x(d.data.seasonYear) - barW / 2)
					.attr('width', barW)
					.attr('fill', s.color)
					.attr('fill-opacity', (d: any) => (isLive(d.data.seasonYear) ? 0.5 : 0.92))
					// 2px of surface between stacked segments.
					.attr('stroke', SURFACE)
					.attr('stroke-width', 1)
					.attr('y', (d: any) => y(d[0]))
					.attr('height', 0)
					.transition()
					.delay((_: any, i: number) => dur(animate ? li * 70 + i * 12 : 0))
					.duration(dur(animate ? 450 : 0))
					.ease(d3.easeCubicOut)
					.attr('y', (d: any) => y(d[1]))
					.attr('height', (d: any) => Math.max(0, y(d[0]) - y(d[1])));
			}
		});

		// Direct labels for the three biggest layers at the right edge.
		const last = layers.map((l: any) => l[l.length - 1]);
		[0, 1, 2].forEach((li) => {
			const d = last[li];
			const mid = (d[0] + d[1]) / 2;
			frame.append('text').attr('x', plotW + 10).attr('y', y(mid) + 4).attr('fill', TEXT_SECONDARY).style('font-size', '11px')
				.text(`${POSITIONS[li][0]} ${pct(d[1] - d[0])}`);
		});

		seasonAxis(frame, x, plotH, plotW, years.length, animate);
		crosshair(ctx, x, plotH, step, data2, () => [], (r) => {
			showTooltip(
				seasonHead(r),
				metric.series.map((s) => ({ color: s.color, value: fmt(s, read(r, s.key)), label: s.label }))
			);
		});
	}

	// ---- tooltip (built with textContent: labels are data) -------------------

	interface TipLine {
		value: string;
		label: string;
		color?: string;
	}

	const seasonHead = (r: any) =>
		isLive(r.seasonYear) && live ? `${r.seasonYear} · through week ${live.throughWeek}` : String(r.seasonYear);

	function showTooltip(head: string, lines: TipLine[]) {
		if (!tooltipEl) return;
		tooltipEl.replaceChildren();
		const h = document.createElement('div');
		h.className = 'tt-head';
		h.textContent = head;
		tooltipEl.appendChild(h);
		for (const l of lines) {
			const row = document.createElement('div');
			row.className = l.color ? 'tt-row' : 'tt-row tt-sub';
			if (l.color) {
				const key = document.createElement('span');
				key.className = 'tt-key';
				key.style.background = l.color;
				row.appendChild(key);
			}
			const val = document.createElement('strong');
			val.textContent = l.value;
			const label = document.createElement('span');
			label.className = 'tt-label';
			label.textContent = l.label;
			row.append(val, label);
			tooltipEl.appendChild(row);
		}
		tooltipEl.style.display = 'block';
	}

	function positionTooltip(anchorX: number, maxX: number) {
		if (!tooltipEl) return;
		const w = tooltipEl.offsetWidth;
		const left = anchorX + 14 + w > maxX ? anchorX - 14 - w : anchorX + 14;
		tooltipEl.style.left = `${Math.max(0, left)}px`;
		tooltipEl.style.top = '16px';
	}

	function hideTooltip() {
		if (tooltipEl) tooltipEl.style.display = 'none';
	}

	let keyboardShow: (i: number) => void = () => {};
	let keyboardHide: () => void = () => {};
	let navLength = 0;
	function onKey(e: KeyboardEvent) {
		if (!navLength) return;
		if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
			e.preventDefault();
			const start = hoverIndex ?? (e.key === 'ArrowRight' ? -1 : navLength);
			const next = Math.max(0, Math.min(navLength - 1, start + (e.key === 'ArrowRight' ? 1 : -1)));
			hoverIndex = next;
			keyboardShow(next);
		} else if (e.key === 'Escape') keyboardHide();
	}

	// ---- table view ----------------------------------------------------------

	$: tableColumns = (() => {
		const m = currentMetric;
		if (m.kind === 'champion')
			return [
				{ label: 'Champion', value: (r: any) => r.champion?.manager ?? '—' },
				{ label: 'Scoring rank', value: (r: any) => (r.champion ? `#${r.champion.pointsRank} of ${r.teams}` : '—') },
				{ label: 'Record rank', value: (r: any) => (r.champion ? `#${r.champion.recordRank}` : '—') }
			];
		if (m.kind === 'titles')
			return [
				{ label: 'Champion', value: (r: any) => r.champion?.manager ?? '—' },
				{ label: 'Lost the final', value: (r: any) => r.champion?.runnerUp ?? '—' },
				{ label: 'Final score', value: (r: any) => (r.champion?.score ? `${r.champion.score[0].toFixed(1)}–${r.champion.score[1].toFixed(1)}` : '—') }
			];
		const cols = m.series.map((s) => ({ label: s.label, value: (r: any) => fmt(s, read(r, s.key)) }));
		for (const b of m.bands ?? []) {
			cols.push({ label: b.label, value: (r: any) => `${fmt(m.series[0], read(r, b.lo))} – ${fmt(m.series[0], read(r, b.hi))}` });
		}
		if (m.id === 'balance') cols.push({ label: 'Skill share', value: (r: any) => pct(r.skillShare) });
		return cols;
	})();

	// ---- lifecycle -----------------------------------------------------------

	onMount(async () => {
		if (!browser) return;
		reduceMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ?? false;
		d3 = await import('d3');
		let lastWidth = container?.clientWidth ?? 0;
		resizeObserver = new ResizeObserver(() => {
			const w = container?.clientWidth ?? 0;
			if (Math.abs(w - lastWidth) < 2) return;
			lastWidth = w;
			render(false);
		});
		if (container) resizeObserver.observe(container);
	});

	onDestroy(() => resizeObserver?.disconnect());

	$: if (d3) {
		selectedMetric;
		selectedView;
		data;
		tick().then(() => render(true));
	}
</script>

<div class="d3-overview-container">
	<!-- Headline numbers: what twenty seasons say about this league -->
	{#if headline}
		<div class="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
			<div class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-4">
				<div class="text-sm text-slate-400">Last season's record predicts this one's</div>
				<div class="mt-1 text-2xl font-semibold text-white">
					r = {headline.recordPersistence.r == null ? '—' : headline.recordPersistence.r.toFixed(2)}
				</div>
				<div class="mt-1 text-xs text-slate-400">
					Essentially no carry-over, across {headline.recordPersistence.n} manager-seasons.
				</div>
			</div>
			<div class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-4">
				<div class="text-sm text-slate-400">Share of the spread in records that's skill</div>
				<div class="mt-1 text-2xl font-semibold text-white">{pct(headline.skillShare)}</div>
				<div class="mt-1 text-xs text-slate-400">The rest is luck (Tango's method, {headline.seasons} seasons).</div>
			</div>
			<div class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-4">
				<div class="text-sm text-slate-400">Champions who were a top-3 scorer</div>
				<div class="mt-1 text-2xl font-semibold text-white">
					{headline.championTopThree.count} of {headline.championTopThree.seasons}
				</div>
				<div class="mt-1 text-xs text-slate-400">The season's top scorer won {headline.topScorerTitles}.</div>
			</div>
			<div class="rounded-xl border border-slate-700/50 bg-slate-800/40 p-4">
				<div class="text-sm text-slate-400">Starting points from a team's own picks</div>
				<div class="mt-1 text-2xl font-semibold text-white">{pct(headline.draftedShare)}</div>
				<div class="mt-1 text-xs text-slate-400">The rest came off waivers and trades.</div>
			</div>
		</div>
	{/if}

	<!-- Controls: measure, then the views that suit it -->
	<div class="mb-3 flex flex-wrap items-end gap-4">
		<label class="min-w-[14rem] flex-1">
			<span class="mb-2 block text-sm font-medium text-slate-300">Measure</span>
			<select bind:value={selectedMetric} class="w-full rounded-lg border border-slate-600 bg-slate-700 px-3 py-2 text-white">
				{#each metrics as metric}
					<option value={metric.id}>{metric.name}</option>
				{/each}
			</select>
		</label>
		{#if currentMetric.views.length > 1}
			<div>
				<span class="mb-2 block text-sm font-medium text-slate-300" id="view-label">View</span>
				<div class="inline-flex rounded-lg border border-slate-600 bg-slate-800/60 p-1" role="group" aria-labelledby="view-label">
					{#each currentMetric.views as view}
						<button
							type="button"
							class="rounded-md px-3 py-1.5 text-sm font-medium transition-colors {selectedView === view
								? 'bg-slate-600 text-white'
								: 'text-slate-300 hover:text-white'}"
							aria-pressed={selectedView === view}
							on:click={() => (selectedView = view)}
						>
							{VIEW_NAMES[view]}
						</button>
					{/each}
				</div>
			</div>
		{/if}
	</div>

	<p class="mb-1 text-base font-medium text-slate-200">{currentMetric.question}</p>
	<p class="mb-4 text-sm text-slate-400">{currentMetric.description}</p>

	<div class="chart-container relative rounded-xl border border-slate-700/50 bg-slate-800/30 p-4 sm:p-6">
		<div class="mb-3 flex flex-wrap items-center justify-between gap-3">
			<h3 class="text-lg font-semibold text-white">{currentMetric.name}</h3>
			<div class="flex flex-wrap items-center gap-4 text-xs text-slate-300">
				{#if currentMetric.kind === 'stacked'}
					{#each currentMetric.series as s}
						<span class="flex items-center gap-2"><span class="h-2.5 w-2.5 rounded-sm" style="background:{s.color}"></span>{s.label}</span>
					{/each}
				{:else if currentMetric.series.length > 1}
					{#each currentMetric.series as s}
						<span class="flex items-center gap-2"><span class="h-0.5 w-4 rounded" style="background:{s.color}"></span>{s.label}</span>
					{/each}
				{:else if currentMetric.bands}
					{#each currentMetric.bands as b}
						<span class="flex items-center gap-2"
							><span class="h-2.5 w-4 rounded-sm" style="background:{currentMetric.series[0].color};opacity:{b.opacity * 2.5}"></span>{b.label}</span
						>
					{/each}
				{/if}
				{#if live && currentMetric.kind !== 'champion' && currentMetric.kind !== 'titles' && currentMetric.id !== 'balance'}
					<span class="flex items-center gap-2 text-slate-400"
						><span class="h-2.5 w-2.5 rounded-full border-2" style="border-color:{BLUE}"></span>{live.seasonYear} in progress</span
					>
				{/if}
				<button
					type="button"
					class="rounded-md border border-slate-600 px-2 py-1 text-slate-300 hover:text-white"
					aria-pressed={showTable}
					on:click={() => (showTable = !showTable)}
				>
					{showTable ? 'Hide table' : 'Show table'}
				</button>
			</div>
		</div>

		<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
		<div
			bind:this={container}
			class="viz-host relative w-full outline-none focus-visible:ring-2 focus-visible:ring-slate-400"
			tabindex="0"
			role="group"
			aria-label="{currentMetric.name}. Use left and right arrow keys to read each season."
			on:keydown={onKey}
			on:blur={() => keyboardHide()}
		>
			<div bind:this={tooltipEl} class="tooltip" role="status" aria-live="polite"></div>
		</div>

		{#if !data}
			<div class="flex h-64 items-center justify-center text-slate-400">Loading league data...</div>
		{/if}

		{#if showTable && rows.length}
			<div class="mt-4 max-h-80 overflow-auto rounded-lg border border-slate-700/60">
				<table class="w-full text-sm">
					<thead class="sticky top-0 bg-slate-800 text-left text-xs uppercase tracking-wide text-slate-400">
						<tr>
							<th class="px-3 py-2">Season</th>
							{#each tableColumns as c}<th class="px-3 py-2 text-right">{c.label}</th>{/each}
						</tr>
					</thead>
					<tbody>
						{#each rows as r}
							<tr class="border-t border-slate-700/40 text-slate-200">
								<td class="px-3 py-1.5"
									>{r.seasonYear}{#if isLive(r.seasonYear)}<span class="ml-1 text-xs text-slate-400">(in progress)</span>{/if}</td
								>
								{#each tableColumns as c}<td class="px-3 py-1.5 text-right tabular-nums">{c.value(r)}</td>{/each}
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	</div>

	{#if data}
		<div class="mt-6 rounded-lg bg-slate-800/50 p-4">
			<h3 class="mb-2 font-bold text-white">Key Insights</h3>
			<div class="space-y-1 text-sm text-slate-300">
				{#each insights as insight}
					<div>• {insight}</div>
				{:else}
					<div class="text-slate-400">Insights appear once season data loads.</div>
				{/each}
			</div>
		</div>
	{/if}
</div>

<style>
	.d3-overview-container {
		width: 100%;
		max-width: 1200px;
		margin: 0 auto;
	}
	.viz-host :global(svg.viz) {
		display: block;
		overflow: visible;
		font-family: inherit;
	}
	.tooltip {
		display: none;
		position: absolute;
		z-index: 10;
		pointer-events: none;
		min-width: 11rem;
		padding: 0.5rem 0.65rem;
		border-radius: 0.5rem;
		border: 1px solid rgb(71 85 105 / 0.7);
		background: rgb(15 23 42 / 0.96);
		box-shadow: 0 8px 24px rgb(0 0 0 / 0.35);
		color: #f1f5f9;
		font-size: 12px;
	}
	.tooltip :global(.tt-head) {
		margin-bottom: 0.3rem;
		color: #94a3b8;
		font-weight: 600;
	}
	.tooltip :global(.tt-row) {
		display: flex;
		align-items: center;
		gap: 0.45rem;
		line-height: 1.6;
	}
	.tooltip :global(.tt-row strong) {
		font-variant-numeric: tabular-nums;
	}
	.tooltip :global(.tt-key) {
		display: inline-block;
		width: 12px;
		height: 2px;
		border-radius: 1px;
	}
	.tooltip :global(.tt-label) {
		color: #cbd5e1;
	}
	.tooltip :global(.tt-sub) {
		padding-left: calc(12px + 0.45rem);
		color: #cbd5e1;
	}
</style>

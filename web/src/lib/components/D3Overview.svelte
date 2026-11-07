<script lang="ts">
	// League trends over time, drawn with D3.
	//
	// Design rules (from the dataviz guidance this site follows):
	//  - One y-scale per plot. Two measures on different scales get two panels
	//    that share the season axis and one crosshair, never a second y-axis:
	//    the old dual-axis chart made its two lines "cross" wherever the two
	//    arbitrary scales happened to line up.
	//  - Scoring shows its spread directly, as bands of the real weekly scores,
	//    rather than a "volatility" line on its own axis.
	//  - The season still being played is marked as partial and left out of the
	//    peak/low call-outs and the insights.
	//  - Hover is a crosshair that snaps to the nearest season and lists every
	//    series; arrow keys do the same; a table view carries every value.
	//  - Motion has a job: lines draw in, bars grow from the baseline, and a
	//    change of metric or view transitions instead of blinking.
	import { onDestroy, onMount, tick } from 'svelte';
	import { browser } from '$app/environment';

	export let data: any = null;

	let d3: any = null;
	let container: HTMLDivElement;
	let tooltipEl: HTMLDivElement;
	let selectedMetric = 'scoring';
	let selectedView: ViewId = 'line';
	let showTable = false;

	type ViewId = 'line' | 'area' | 'bar' | 'relation';

	interface SeriesDef {
		key: string;
		label: string;
		unit: string;
		decimals: number;
		color: string;
	}

	interface MetricDef {
		id: string;
		name: string;
		description: string;
		/** 'shared': every series on one axis. 'panels': one stacked panel per series. */
		layout: 'shared' | 'panels';
		series: SeriesDef[];
		/** Percentile bands drawn behind the first series (scoring only). */
		bands?: { lo: string; hi: string; label: string; opacity: number }[];
		/** The two measures for the relationship view: x, then y. */
		relation: [SeriesDef, SeriesDef];
		axisLabel: string;
	}

	// Categorical slots, dark steps (validated against this page's navy surface:
	// lightness band, chroma, CVD and contrast all pass).
	const BLUE = '#3987e5';
	const ORANGE = '#d95926';
	const AQUA = '#199e70';

	const S = {
		avgScore: { key: 'avgWeeklyScore', label: 'Average team score', unit: ' pts', decimals: 1, color: BLUE },
		volatility: { key: 'scoreVolatility', label: 'Week-to-week spread (sd)', unit: ' pts', decimals: 1, color: ORANGE },
		parity: { key: 'winParityScore', label: 'Win parity', unit: '', decimals: 1, color: BLUE },
		closeGames: { key: 'closeGamesScore', label: 'Close games', unit: '', decimals: 1, color: ORANGE },
		transactions: { key: 'totalTransactions', label: 'Transactions', unit: '', decimals: 0, color: BLUE },
		trades: { key: 'totalTrades', label: 'Trades', unit: '', decimals: 0, color: ORANGE },
		playerPoints: { key: 'avgFantasyPoints', label: 'Average player points', unit: ' pts', decimals: 1, color: BLUE },
		draftSlot: { key: 'avgDraftPosition', label: 'Average draft position', unit: '', decimals: 1, color: ORANGE },
		highScore: { key: 'highestSingleWeekScore', label: 'Highest single-week score', unit: ' pts', decimals: 1, color: BLUE },
		pointSpread: { key: 'pointSpreadScore', label: 'Point spread score', unit: '', decimals: 1, color: AQUA }
	} satisfies Record<string, SeriesDef>;

	const metrics: MetricDef[] = [
		{
			id: 'scoring',
			name: 'Scoring Evolution',
			description:
				'Average team score per game, inside the range of real weekly scores: the darker band is the middle half of all scores that season, the lighter one the middle 80%.',
			layout: 'shared',
			series: [S.avgScore],
			bands: [
				{ lo: 'p10Score', hi: 'p90Score', label: 'Middle 80% of weekly scores', opacity: 0.12 },
				{ lo: 'p25Score', hi: 'p75Score', label: 'Middle 50%', opacity: 0.2 }
			],
			relation: [S.avgScore, S.volatility],
			axisLabel: 'Points per game'
		},
		{
			id: 'competitiveness',
			name: 'League Competitiveness',
			description: 'Two 0–100 scores: how evenly wins were spread, and how many games were close.',
			layout: 'shared',
			series: [S.parity, S.closeGames],
			relation: [S.parity, S.closeGames],
			axisLabel: 'Score (0–100)'
		},
		{
			id: 'activity',
			name: 'League Activity',
			description: 'Every add, drop and trade, and trades on their own.',
			layout: 'panels',
			series: [S.transactions, S.trades],
			relation: [S.transactions, S.trades],
			axisLabel: ''
		},
		{
			id: 'players',
			name: 'Player Value Trends',
			description: 'Average season points of players who scored, and their average draft position.',
			layout: 'panels',
			series: [S.playerPoints, S.draftSlot],
			relation: [S.draftSlot, S.playerPoints],
			axisLabel: ''
		},
		{
			id: 'spread',
			name: 'Scoring Distribution',
			description: 'The best single week of each season, and how far apart teams finished (0–100, higher is tighter).',
			layout: 'panels',
			series: [S.highScore, S.pointSpread],
			relation: [S.pointSpread, S.highScore],
			axisLabel: ''
		}
	];

	const views: { id: ViewId; name: string }[] = [
		{ id: 'line', name: 'Line' },
		{ id: 'area', name: 'Area' },
		{ id: 'bar', name: 'Bars' },
		{ id: 'relation', name: 'Relationship' }
	];

	$: currentMetric = metrics.find((m) => m.id === selectedMetric) ?? metrics[0];
	$: live = data?.seasonInProgress ?? null;

	// ---- data ----------------------------------------------------------------

	const num = (v: unknown): number | null => {
		if (v === null || v === undefined || v === '') return null;
		const n = typeof v === 'number' ? v : Number(v);
		return Number.isFinite(n) ? n : null;
	};
	const snake = (k: string) => k.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
	const read = (row: any, key: string) => num(row?.[key] ?? row?.[snake(key)]);
	const yearOf = (row: any) => num(row?.seasonYear ?? row?.season_year);

	/** Rows keyed by season, merged across sources by season (never by position). */
	function rowsFor(metricId: string): any[] {
		if (!data) return [];
		const bySeason = new Map<number, any>();
		const merge = (rows: any[] | undefined) => {
			for (const r of rows ?? []) {
				const y = yearOf(r);
				if (y == null) continue;
				bySeason.set(y, { ...(bySeason.get(y) ?? { seasonYear: y }), ...r, seasonYear: y });
			}
		};
		switch (metricId) {
			case 'scoring':
				merge(data.scoringPatterns ?? data.scoring_patterns);
				break;
			case 'competitiveness':
				merge(data.competitiveness);
				break;
			case 'activity': {
				merge(data.leagueEvolution ?? data.league_evolution);
				// A season with no row in trade activity had no trades.
				const trades = new Map(
					(data.tradeActivity ?? data.trade_activity ?? []).map((t: any) => [yearOf(t), read(t, 'totalTrades')])
				);
				for (const [y, r] of bySeason) r.totalTrades = trades.get(y) ?? 0;
				break;
			}
			case 'players':
				merge(data.playerTrends ?? data.player_trends);
				break;
			case 'spread': {
				merge(data.leagueEvolution ?? data.league_evolution);
				const comp = new Map((data.competitiveness ?? []).map((c: any) => [yearOf(c), read(c, 'pointSpreadScore')]));
				for (const [y, r] of bySeason) r.pointSpreadScore = comp.get(y) ?? null;
				break;
			}
		}
		return [...bySeason.values()].sort((a, b) => a.seasonYear - b.seasonYear);
	}

	$: rows = data ? rowsFor(selectedMetric) : [];
	$: isLive = (year: number) => live != null && year === live.seasonYear;

	function fmt(s: SeriesDef, v: number | null): string {
		if (v == null) return '—';
		return v.toLocaleString(undefined, { minimumFractionDigits: s.decimals, maximumFractionDigits: s.decimals }) + s.unit;
	}

	// Insights from the primary series, finished seasons only.
	$: insights = (() => {
		const s = currentMetric.series[0];
		const pts = rows
			.filter((r) => !isLive(r.seasonYear))
			.map((r) => ({ year: r.seasonYear as number, val: read(r, s.key) }))
			.filter((p): p is { year: number; val: number } => p.val != null);
		if (pts.length < 2) return [];
		const first = pts[0];
		const last = pts[pts.length - 1];
		const peak = pts.reduce((m, p) => (p.val > m.val ? p : m));
		const low = pts.reduce((m, p) => (p.val < m.val ? p : m));
		const avg = pts.reduce((a, p) => a + p.val, 0) / pts.length;
		const pct = first.val !== 0 ? Math.round(((last.val - first.val) / Math.abs(first.val)) * 100) : null;
		return [
			`${s.label}: ${fmt(s, first.val)} in ${first.year} → ${fmt(s, last.val)} in ${last.year}` +
				(pct ? `, ${pct > 0 ? 'up' : 'down'} ${Math.abs(pct)}%` : ''),
			`Peak ${fmt(s, peak.val)} (${peak.year}) · low ${fmt(s, low.val)} (${low.year})`,
			`${pts.length}-season average: ${fmt(s, avg)}` + (live ? ` · ${live.seasonYear} is still being played` : '')
		];
	})();

	// ---- rendering -----------------------------------------------------------

	const TEXT_PRIMARY = '#f1f5f9';
	const TEXT_SECONDARY = '#cbd5e1';
	const TEXT_MUTED = '#94a3b8';
	const GRID = '#334155';
	const SURFACE = '#1c2746';
	const MARGIN = { top: 16, right: 92, bottom: 34, left: 56 };
	const PANEL_GAP = 48;

	let width = 0;
	let hoverIndex: number | null = null;
	let resizeObserver: ResizeObserver | null = null;
	let reduceMotion = false;

	const dur = (ms: number) => (reduceMotion ? 0 : ms);

	function render(animate: boolean) {
		if (!d3 || !container || !rows.length) return;
		width = container.clientWidth;
		if (width <= 0) return;

		const metric = currentMetric;
		const view = selectedView;
		const panels = view === 'relation' ? 1 : metric.layout === 'panels' ? metric.series.length : 1;
		const panelHeight = view === 'relation' ? 380 : metric.layout === 'panels' ? 190 : 360;
		const plotW = Math.max(200, width - MARGIN.left - MARGIN.right);
		const height = MARGIN.top + panels * panelHeight + (panels - 1) * PANEL_GAP + MARGIN.bottom;

		const root = d3.select(container);
		let svg = root.select('svg.viz');
		if (svg.empty()) {
			svg = root.append('svg').attr('class', 'viz').attr('role', 'img');
		}
		svg
			.attr('aria-label', `${metric.name}, ${view === 'relation' ? 'relationship' : view} chart by season`)
			.transition()
			.duration(dur(animate ? 350 : 0))
			.attr('viewBox', `0 0 ${width} ${height}`)
			.attr('height', height);
		svg.attr('width', width);

		// Old frame fades out while the new one draws in.
		svg
			.selectAll('g.frame')
			.attr('class', 'frame-old')
			.style('pointer-events', 'none')
			.transition()
			.duration(dur(animate ? 220 : 0))
			.style('opacity', 0)
			.remove();

		const frame = svg.append('g').attr('class', 'frame').attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);
		ensureDefs(svg, metric);

		if (view === 'relation') {
			drawRelation(frame, metric, plotW, panelHeight, animate);
		} else {
			drawTimeSeries(frame, metric, view, plotW, panelHeight, panels, animate);
		}
	}

	function ensureDefs(svg: any, metric: MetricDef) {
		let defs = svg.select('defs');
		if (defs.empty()) defs = svg.append('defs');
		for (const s of [...metric.series, ...metric.relation]) {
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

	function drawTimeSeries(
		frame: any,
		metric: MetricDef,
		view: ViewId,
		plotW: number,
		panelHeight: number,
		panels: number,
		animate: boolean
	) {
		const years = rows.map((r) => r.seasonYear as number);
		const roughStep = years.length > 1 ? plotW / (years.length - 1) : plotW;
		const barW = Math.min(24, Math.max(4, roughStep * 0.55));
		// Bars are centred on their season, so inset the axis by half a bar (and
		// the 2px air) or the first bar sits on the y-axis labels.
		const inset = view === 'bar' ? barW / 2 + 6 : 0;
		const x = d3.scaleLinear().domain(d3.extent(years)).range([inset, plotW - inset]);
		const step = years.length > 1 ? (plotW - 2 * inset) / (years.length - 1) : plotW;
		const xBar = (y: number) => x(y);
		const ease = d3.easeCubicOut;

		const groups = metric.layout === 'panels' ? metric.series.map((s) => [s]) : [metric.series];
		const yScales: any[] = [];

		groups.forEach((seriesInPanel, pi) => {
			const top = pi * (panelHeight + PANEL_GAP);
			const p = frame.append('g').attr('class', 'panel').attr('transform', `translate(0,${top})`);

			// y domain: series values plus any bands; bars and areas start at zero.
			const vals: number[] = [];
			for (const r of rows) {
				for (const s of seriesInPanel) {
					const v = read(r, s.key);
					if (v != null) vals.push(v);
				}
				if (pi === 0) for (const b of metric.bands ?? []) {
					const lo = read(r, b.lo);
					const hi = read(r, b.hi);
					if (lo != null) vals.push(lo);
					if (hi != null) vals.push(hi);
				}
			}
			const fromZero = view !== 'line' || metric.layout === 'panels';
			const [lo, hi] = d3.extent(vals);
			const y = d3
				.scaleLinear()
				.domain([fromZero ? Math.min(0, lo) : lo - (hi - lo) * 0.08, hi + (hi - lo) * 0.06])
				.nice(5)
				.range([panelHeight, 0]);
			yScales.push(y);

			// Grid + y axis (hairline, solid, recessive).
			const yAxis = p
				.append('g')
				.call(d3.axisLeft(y).ticks(panelHeight < 220 ? 4 : 6).tickSize(-plotW).tickFormat(d3.format('~s')))
				.call(styleAxis);
			yAxis.selectAll('.tick line').attr('stroke-opacity', 0.6);
			if (animate) yAxis.style('opacity', 0).transition().duration(dur(400)).style('opacity', 1);

			// Panel title (panels) or axis label (shared).
			const title = metric.layout === 'panels' ? seriesInPanel[0].label : metric.axisLabel;
			if (title) {
				p.append('text')
					.attr('x', 0)
					.attr('y', -4)
					.attr('fill', TEXT_SECONDARY)
					.style('font-size', '12px')
					.style('font-weight', '600')
					.text(title);
			}

			// Percentile bands behind the primary line.
			if (pi === 0 && metric.bands && view !== 'bar') {
				for (const b of metric.bands) {
					const band = d3
						.area()
						.defined((r: any) => read(r, b.lo) != null && read(r, b.hi) != null)
						.x((r: any) => x(r.seasonYear))
						.y0((r: any) => y(read(r, b.lo)))
						.y1((r: any) => y(read(r, b.hi)))
						.curve(d3.curveMonotoneX);
					// Bands open out from the average line.
					const collapsed = d3
						.area()
						.defined((r: any) => read(r, b.lo) != null)
						.x((r: any) => x(r.seasonYear))
						.y0((r: any) => y(read(r, metric.series[0].key)))
						.y1((r: any) => y(read(r, metric.series[0].key)))
						.curve(d3.curveMonotoneX);
					const path = p
						.append('path')
						.datum(rows)
						.attr('fill', metric.series[0].color)
						.attr('fill-opacity', b.opacity);
					if (animate) {
						path.attr('d', collapsed).transition().delay(dur(350)).duration(dur(700)).ease(ease).attr('d', band);
					} else path.attr('d', band);
				}
			}

			for (const s of seriesInPanel) {
				const pts = rows.filter((r) => read(r, s.key) != null);
				if (view === 'bar') {
					const radius = 4;
					const barPath = (r: any, h: number) => {
						const x0 = xBar(r.seasonYear) - barW / 2;
						const y0 = y(0);
						const yt = y0 - h;
						const rr = Math.min(radius, h, barW / 2);
						return `M${x0},${y0}V${yt + rr}Q${x0},${yt} ${x0 + rr},${yt}H${x0 + barW - rr}Q${x0 + barW},${yt} ${x0 + barW},${yt + rr}V${y0}Z`;
					};
					const offset = seriesInPanel.length > 1 ? (seriesInPanel.indexOf(s) - 0.5) * (barW / 2 + 1) : 0;
					p.append('g')
						.attr('transform', `translate(${offset},0)`)
						.selectAll('path')
						.data(pts)
						.join('path')
						.attr('fill', s.color)
						.attr('fill-opacity', (r: any) => (isLive(r.seasonYear) ? 0.45 : 0.9))
						.attr('d', (r: any) => barPath(r, 0))
						.transition()
						.delay((_: any, i: number) => dur(animate ? 120 + i * 22 : 0))
						.duration(dur(animate ? 600 : 0))
						.ease(ease)
						.attr('d', (r: any) => barPath(r, Math.max(0, y(0) - y(read(r, s.key)))));
					continue;
				}

				const finished = pts.filter((r) => !isLive(r.seasonYear));
				const liveRow = pts.find((r) => isLive(r.seasonYear));
				const line = d3
					.line()
					.x((r: any) => x(r.seasonYear))
					.y((r: any) => y(read(r, s.key)))
					.curve(d3.curveMonotoneX);

				if (view === 'area') {
					const area = d3
						.area()
						.x((r: any) => x(r.seasonYear))
						.y0(y(Math.max(0, y.domain()[0])))
						.y1((r: any) => y(read(r, s.key)))
						.curve(d3.curveMonotoneX);
					const flat = d3
						.area()
						.x((r: any) => x(r.seasonYear))
						.y0(y(Math.max(0, y.domain()[0])))
						.y1(y(Math.max(0, y.domain()[0])))
						.curve(d3.curveMonotoneX);
					const a = p.append('path').datum(pts).attr('fill', `url(#grad-${s.key})`);
					if (animate) a.attr('d', flat).transition().duration(dur(800)).ease(ease).attr('d', area);
					else a.attr('d', area);
				}

				// Finished seasons: a solid line that draws itself in.
				const path = p
					.append('path')
					.datum(finished)
					.attr('fill', 'none')
					.attr('stroke', s.color)
					.attr('stroke-width', 2)
					.attr('stroke-linejoin', 'round')
					.attr('stroke-linecap', 'round')
					.attr('d', line);
				if (animate) {
					const len = path.node().getTotalLength();
					path
						.attr('stroke-dasharray', `${len} ${len}`)
						.attr('stroke-dashoffset', len)
						.transition()
						.duration(dur(1100))
						.ease(d3.easeCubicInOut)
						.attr('stroke-dashoffset', 0)
						.on('end', function (this: any) {
							d3.select(this).attr('stroke-dasharray', null);
						});
				}

				// The season in progress: a dotted continuation to a hollow marker.
				if (liveRow && finished.length) {
					const last = finished[finished.length - 1];
					p.append('path')
						.attr('fill', 'none')
						.attr('stroke', s.color)
						.attr('stroke-width', 2)
						.attr('stroke-dasharray', '2 4')
						.attr('stroke-linecap', 'round')
						.attr('d', `M${x(last.seasonYear)},${y(read(last, s.key))}L${x(liveRow.seasonYear)},${y(read(liveRow, s.key))}`)
						.style('opacity', 0)
						.transition()
						.delay(dur(animate ? 1100 : 0))
						.duration(dur(300))
						.style('opacity', 1);
					p.append('circle')
						.attr('cx', x(liveRow.seasonYear))
						.attr('cy', y(read(liveRow, s.key)))
						.attr('r', 4.5)
						.attr('fill', SURFACE)
						.attr('stroke', s.color)
						.attr('stroke-width', 2);
				}

				// Peak and low of finished seasons, labelled; value at the end.
				if (finished.length > 2 && s === seriesInPanel[0]) {
					const peak = finished.reduce((m, r) => (read(r, s.key)! > read(m, s.key)! ? r : m));
					const low = finished.reduce((m, r) => (read(r, s.key)! < read(m, s.key)! ? r : m));
					for (const [r, where] of [
						[peak, 'peak'],
						[low, 'low']
					] as const) {
						const cx = x(r.seasonYear);
						const cy = y(read(r, s.key));
						const g = p.append('g').style('opacity', 0);
						g.append('circle').attr('cx', cx).attr('cy', cy).attr('r', 4).attr('fill', s.color).attr('stroke', SURFACE).attr('stroke-width', 2);
						// Above for a peak and below for a low, unless that would leave the
						// panel (into its title or the axis below): then the other side.
						const above = where === 'peak' ? cy - 10 >= 14 : cy + 18 > panelHeight - 4;
						g.append('text')
							.attr('x', cx)
							.attr('y', above ? cy - 10 : cy + 18)
							.attr('text-anchor', cx < 40 ? 'start' : cx > plotW - 40 ? 'end' : 'middle')
							.attr('fill', TEXT_SECONDARY)
							.style('font-size', '11px')
							.text(`${where === 'peak' ? 'Peak' : 'Low'} ${fmt(s, read(r, s.key))} · ${r.seasonYear}`);
						g.transition().delay(dur(animate ? 900 : 0)).duration(dur(400)).style('opacity', 1);
					}
				}

				const end = liveRow ?? finished[finished.length - 1];
				if (end) {
					const ex = x(end.seasonYear);
					const ey = y(read(end, s.key));
					const label = p.append('g').style('opacity', 0);
					label
						.append('text')
						.attr('x', ex + 10)
						.attr('y', ey + 4)
						.attr('fill', TEXT_PRIMARY)
						.style('font-size', '12px')
						.style('font-weight', '600')
						.text(fmt(s, read(end, s.key)));
					label
						.append('text')
						.attr('x', ex + 10)
						.attr('y', ey + 18)
						.attr('fill', TEXT_MUTED)
						.style('font-size', '10px')
						.text(isLive(end.seasonYear) && live ? `${end.seasonYear} · wk ${live.throughWeek}` : `${end.seasonYear}`);
					label.transition().delay(dur(animate ? 1100 : 0)).duration(dur(300)).style('opacity', 1);
				}
			}
		});

		// Shared season axis under the last panel.
		const bottom = panels * panelHeight + (panels - 1) * PANEL_GAP;
		const xAxis = frame
			.append('g')
			.attr('transform', `translate(0,${bottom})`)
			.call(
				d3
					.axisBottom(x)
					.ticks(Math.min(years.length, Math.floor(plotW / 64)))
					.tickFormat(d3.format('d'))
					.tickSize(0)
					.tickPadding(10)
			)
			.call(styleAxis);
		if (animate) xAxis.style('opacity', 0).transition().duration(dur(400)).style('opacity', 1);

		// Crosshair across every panel, snapping to the nearest season.
		const cross = frame.append('g').attr('class', 'cross').style('display', 'none').style('pointer-events', 'none');
		cross.append('line').attr('y1', -6).attr('y2', bottom).attr('stroke', TEXT_MUTED).attr('stroke-width', 1).attr('stroke-opacity', 0.6);
		const dots = cross.append('g');

		const show = (i: number) => {
			hoverIndex = i;
			const r = rows[i];
			const cx = x(r.seasonYear);
			cross.style('display', null).attr('transform', `translate(${cx},0)`);
			dots.selectAll('*').remove();
			groups.forEach((seriesInPanel, pi) => {
				for (const s of seriesInPanel) {
					const v = read(r, s.key);
					if (v == null || view === 'bar') continue;
					dots.append('circle')
						.attr('cx', 0)
						.attr('cy', pi * (panelHeight + PANEL_GAP) + yScales[pi](v))
						.attr('r', 5)
						.attr('fill', s.color)
						.attr('stroke', SURFACE)
						.attr('stroke-width', 2);
				}
			});
			showTooltip(r, cx + MARGIN.left, plotW + MARGIN.left);
		};
		const hide = () => {
			hoverIndex = null;
			cross.style('display', 'none');
			hideTooltip();
		};

		frame
			.append('rect')
			.attr('class', 'hit')
			.attr('x', -step / 2)
			.attr('y', -MARGIN.top)
			.attr('width', plotW + step)
			.attr('height', bottom + MARGIN.top)
			.attr('fill', 'transparent')
			.on('pointermove', (event: PointerEvent) => {
				const [mx] = d3.pointer(event);
				const year = x.invert(mx);
				let best = 0;
				rows.forEach((r, i) => {
					if (Math.abs(r.seasonYear - year) < Math.abs(rows[best].seasonYear - year)) best = i;
				});
				show(best);
			})
			.on('pointerleave', hide);

		keyboardShow = show;
		keyboardHide = hide;
	}

	function drawRelation(frame: any, metric: MetricDef, plotW: number, plotH: number, animate: boolean) {
		const [sx, sy] = metric.relation;
		const pts = rows
			.map((r) => ({ r, xv: read(r, sx.key), yv: read(r, sy.key) }))
			.filter((p): p is { r: any; xv: number; yv: number } => p.xv != null && p.yv != null);
		if (!pts.length) return;
		const pad = (ext: [number, number]) => {
			const d = ext[1] - ext[0] || 1;
			return [ext[0] - d * 0.08, ext[1] + d * 0.08];
		};
		const x = d3.scaleLinear().domain(pad(d3.extent(pts, (p: any) => p.xv))).nice().range([0, plotW]);
		const y = d3.scaleLinear().domain(pad(d3.extent(pts, (p: any) => p.yv))).nice().range([plotH, 0]);

		frame
			.append('g')
			.call(d3.axisLeft(y).ticks(6).tickSize(-plotW).tickFormat(d3.format('~s')))
			.call(styleAxis)
			.selectAll('.tick line')
			.attr('stroke-opacity', 0.6);
		frame
			.append('g')
			.attr('transform', `translate(0,${plotH})`)
			.call(d3.axisBottom(x).ticks(6).tickSize(-plotH).tickFormat(d3.format('~s')))
			.call(styleAxis)
			.selectAll('.tick line')
			.attr('stroke-opacity', 0.35);
		frame.append('text').attr('x', plotW).attr('y', plotH + 30).attr('text-anchor', 'end').attr('fill', TEXT_SECONDARY).style('font-size', '12px').text(`${sx.label} →`);
		frame.append('text').attr('x', 0).attr('y', -4).attr('fill', TEXT_SECONDARY).style('font-size', '12px').style('font-weight', '600').text(`↑ ${sy.label}`);

		// Seasons joined in order, fading from oldest to newest.
		const years = pts.map((p) => p.r.seasonYear);
		const age = d3.scaleLinear().domain(d3.extent(years)).range([0.25, 1]);
		const trail = frame.append('g');
		for (let i = 1; i < pts.length; i++) {
			const a = pts[i - 1];
			const b = pts[i];
			trail
				.append('line')
				.attr('x1', x(a.xv))
				.attr('y1', y(a.yv))
				.attr('x2', x(a.xv))
				.attr('y2', y(a.yv))
				.attr('stroke', BLUE)
				.attr('stroke-width', 1.5)
				.attr('stroke-opacity', age(b.r.seasonYear) * 0.7)
				.transition()
				.delay(dur(animate ? i * 45 : 0))
				.duration(dur(animate ? 160 : 0))
				.attr('x2', x(b.xv))
				.attr('y2', y(b.yv));
		}
		frame
			.append('g')
			.selectAll('circle')
			.data(pts)
			.join('circle')
			.attr('cx', (p: any) => x(p.xv))
			.attr('cy', (p: any) => y(p.yv))
			.attr('r', 0)
			.attr('fill', (p: any) => (isLive(p.r.seasonYear) ? SURFACE : BLUE))
			.attr('fill-opacity', (p: any) => (isLive(p.r.seasonYear) ? 1 : age(p.r.seasonYear)))
			.attr('stroke', (p: any) => (isLive(p.r.seasonYear) ? BLUE : SURFACE))
			.attr('stroke-width', 2)
			.transition()
			.delay((_: any, i: number) => dur(animate ? i * 45 : 0))
			.duration(dur(animate ? 250 : 0))
			.attr('r', 5);

		// Label the first and latest seasons so the direction of travel reads.
		for (const p of [pts[0], pts[pts.length - 1]]) {
			frame
				.append('text')
				.attr('x', x(p.xv) + 9)
				.attr('y', y(p.yv) - 8)
				.attr('fill', TEXT_SECONDARY)
				.style('font-size', '11px')
				.text(String(p.r.seasonYear))
				.style('opacity', 0)
				.transition()
				.delay(dur(animate ? pts.length * 45 : 0))
				.duration(dur(300))
				.style('opacity', 1);
		}

		// Nearest season to the pointer (Delaunay), not a pinpoint on the dot.
		const delaunay = d3.Delaunay.from(pts, (p: any) => x(p.xv), (p: any) => y(p.yv));
		const ring = frame.append('circle').attr('r', 9).attr('fill', 'none').attr('stroke', TEXT_PRIMARY).attr('stroke-width', 1.5).style('display', 'none');
		const show = (i: number) => {
			const p = pts[i];
			hoverIndex = rows.indexOf(p.r);
			ring.style('display', null).attr('cx', x(p.xv)).attr('cy', y(p.yv));
			showTooltip(p.r, x(p.xv) + MARGIN.left, plotW + MARGIN.left, [sx, sy]);
		};
		const hide = () => {
			hoverIndex = null;
			ring.style('display', 'none');
			hideTooltip();
		};
		frame
			.append('rect')
			.attr('width', plotW)
			.attr('height', plotH)
			.attr('fill', 'transparent')
			.on('pointermove', (event: PointerEvent) => {
				const [mx, my] = d3.pointer(event);
				show(delaunay.find(mx, my));
			})
			.on('pointerleave', hide);
		keyboardShow = (i: number) => {
			const k = pts.findIndex((p) => p.r === rows[i]);
			if (k >= 0) show(k);
		};
		keyboardHide = hide;
	}

	// ---- tooltip (built with textContent: labels are data) -------------------

	function showTooltip(r: any, anchorX: number, maxX: number, only?: SeriesDef[]) {
		if (!tooltipEl) return;
		const metric = currentMetric;
		tooltipEl.replaceChildren();
		const head = document.createElement('div');
		head.className = 'tt-head';
		head.textContent = isLive(r.seasonYear) && live ? `${r.seasonYear} · through week ${live.throughWeek}` : String(r.seasonYear);
		tooltipEl.appendChild(head);
		const list = only ?? metric.series;
		for (const s of list) {
			const row = document.createElement('div');
			row.className = 'tt-row';
			const key = document.createElement('span');
			key.className = 'tt-key';
			key.style.background = s.color;
			const val = document.createElement('strong');
			val.textContent = fmt(s, read(r, s.key));
			const label = document.createElement('span');
			label.className = 'tt-label';
			label.textContent = s.label;
			row.append(key, val, label);
			tooltipEl.appendChild(row);
		}
		if (!only && metric.bands) {
			for (const b of metric.bands) {
				const row = document.createElement('div');
				row.className = 'tt-row tt-sub';
				const val = document.createElement('strong');
				val.textContent = `${fmt(metric.series[0], read(r, b.lo))} – ${fmt(metric.series[0], read(r, b.hi))}`;
				const label = document.createElement('span');
				label.className = 'tt-label';
				label.textContent = b.label;
				row.append(val, label);
				tooltipEl.appendChild(row);
			}
		}
		tooltipEl.style.display = 'block';
		const w = tooltipEl.offsetWidth;
		const left = anchorX + 14 + w > maxX + MARGIN.right ? anchorX - 14 - w : anchorX + 14;
		tooltipEl.style.left = `${Math.max(0, left)}px`;
		tooltipEl.style.top = `${MARGIN.top}px`;
	}

	function hideTooltip() {
		if (tooltipEl) tooltipEl.style.display = 'none';
	}

	// Arrow keys walk the seasons, like the crosshair.
	let keyboardShow: (i: number) => void = () => {};
	let keyboardHide: () => void = () => {};
	function onKey(e: KeyboardEvent) {
		if (!rows.length) return;
		if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
			e.preventDefault();
			const start = hoverIndex ?? (e.key === 'ArrowRight' ? -1 : rows.length);
			const next = Math.max(0, Math.min(rows.length - 1, start + (e.key === 'ArrowRight' ? 1 : -1)));
			keyboardShow(next);
		} else if (e.key === 'Escape') keyboardHide();
	}

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

	// Draw (animated) once d3 loads, and again when the metric, view or data change.
	$: if (d3) {
		selectedMetric;
		selectedView;
		data;
		tick().then(() => render(true));
	}
</script>

<div class="d3-overview-container">
	<!-- Controls: metric and view in one row above the chart -->
	<div class="mb-4 flex flex-wrap items-end gap-4">
		<label class="min-w-[14rem] flex-1">
			<span class="mb-2 block text-sm font-medium text-slate-300">Metric</span>
			<select
				bind:value={selectedMetric}
				class="w-full rounded-lg border border-slate-600 bg-slate-700 px-3 py-2 text-white"
			>
				{#each metrics as metric}
					<option value={metric.id}>{metric.name}</option>
				{/each}
			</select>
		</label>

		<div>
			<span class="mb-2 block text-sm font-medium text-slate-300" id="view-label">View</span>
			<div class="inline-flex rounded-lg border border-slate-600 bg-slate-800/60 p-1" role="group" aria-labelledby="view-label">
				{#each views as view}
					<button
						type="button"
						class="rounded-md px-3 py-1.5 text-sm font-medium transition-colors {selectedView === view.id
							? 'bg-slate-600 text-white'
							: 'text-slate-300 hover:text-white'}"
						aria-pressed={selectedView === view.id}
						on:click={() => (selectedView = view.id)}
					>
						{view.name}
					</button>
				{/each}
			</div>
		</div>
	</div>

	<p class="mb-4 text-sm text-slate-400">{currentMetric.description}</p>

	<div class="chart-container relative rounded-xl border border-slate-700/50 bg-slate-800/30 p-4 sm:p-6">
		<div class="mb-3 flex flex-wrap items-center justify-between gap-3">
			<h3 class="text-lg font-semibold text-white">{currentMetric.name}</h3>
			<div class="flex flex-wrap items-center gap-4 text-xs text-slate-300">
				{#if selectedView === 'relation'}
					<span class="flex items-center gap-2"
						><span class="h-2.5 w-2.5 rounded-full" style="background:{BLUE}"></span>Each season, oldest faint → newest solid</span
					>
				{:else if currentMetric.series.length > 1}
					{#each currentMetric.series as s}
						<span class="flex items-center gap-2"><span class="h-0.5 w-4 rounded" style="background:{s.color}"></span>{s.label}</span>
					{/each}
				{:else if currentMetric.bands && selectedView !== 'bar'}
					{#each currentMetric.bands as b}
						<span class="flex items-center gap-2"
							><span class="h-2.5 w-4 rounded-sm" style="background:{currentMetric.series[0].color};opacity:{b.opacity * 2.5}"></span>{b.label}</span
						>
					{/each}
				{/if}
				{#if live}
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
							{#each currentMetric.series as s}<th class="px-3 py-2 text-right">{s.label}</th>{/each}
							{#each currentMetric.bands ?? [] as b}<th class="px-3 py-2 text-right">{b.label}</th>{/each}
						</tr>
					</thead>
					<tbody>
						{#each rows as r}
							<tr class="border-t border-slate-700/40 text-slate-200">
								<td class="px-3 py-1.5"
									>{r.seasonYear}{#if isLive(r.seasonYear)}<span class="ml-1 text-xs text-slate-400">(in progress)</span>{/if}</td
								>
								{#each currentMetric.series as s}<td class="px-3 py-1.5 text-right tabular-nums">{fmt(s, read(r, s.key))}</td>{/each}
								{#each currentMetric.bands ?? [] as b}
									<td class="px-3 py-1.5 text-right tabular-nums"
										>{fmt(currentMetric.series[0], read(r, b.lo))} – {fmt(currentMetric.series[0], read(r, b.hi))}</td
									>
								{/each}
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

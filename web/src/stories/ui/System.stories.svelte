<script module>
	/**
	 * The System board, built from the WP1 parts in the board's order. The
	 * frame (the page, the section labels, the specimen boxes) is the board's
	 * own layout; every part inside it is the real component.
	 */
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import Button from '$lib/components/ui/Button.svelte';
	import DataTable from '$lib/components/ui/DataTable.svelte';
	import Movement from '$lib/components/ui/Movement.svelte';
	import OddsBar from '$lib/components/ui/OddsBar.svelte';
	import Segmented from '$lib/components/ui/Segmented.svelte';
	import StatTile from '$lib/components/ui/StatTile.svelte';
	import StatTip from '$lib/components/ui/StatTip.svelte';
	import Tag from '$lib/components/ui/Tag.svelte';
	import StatTipCard from '$lib/components/ui/StatTipCard.svelte';
	import { LUCK_FACTOR, seedDefinitions } from './fixtures';

	seedDefinitions();

	const { Story } = defineMeta({
		title: 'UI/System',
		parameters: { layout: 'fullscreen' }
	});

	const swatches = [
		{ name: 'Page', token: 'ground', use: 'ground' },
		{ name: 'Card', token: 'card', use: 'surfaces' },
		{ name: 'Ink', token: 'ink', use: 'text' },
		{ name: 'Muted', token: 'muted', use: 'meta' },
		{ name: 'Clubhouse', token: 'clubhouse', use: 'header band' },
		{ name: 'Field', token: 'field', use: 'accent, up' },
		{ name: 'Sage', token: 'sage', use: 'series 3' },
		{ name: 'Pennant', token: 'pennant', use: 'titles, down' }
	];

	const weeks = [1, 2, 3].map((week) => ({ value: week, label: `Wk ${week}` }));

	const rowColumns = [
		{ key: 'rank', label: '#', width: '24px' },
		{ key: 'name', label: 'Manager' },
		{ key: 'record', label: 'W-L', numeric: true, width: '60px' },
		{ key: 'pct', label: 'Win %', numeric: true, width: '44px' }
	];
	const rowData = [{ rank: 1, name: 'Trevor', record: '187-145', pct: '.563' }];
</script>

<script lang="ts">
	let week = $state(3);

	/** The hex a swatch's token resolves to, read from the page so it can't drift. */
	function hexOf(token: string) {
		if (typeof window === 'undefined') return '';
		return getComputedStyle(document.documentElement)
			.getPropertyValue(`--color-${token}`)
			.trim()
			.toUpperCase();
	}
</script>

<Story name="System">
	{#snippet template()}
		<div class="board">
			<div class="board-head">
				<div>
					<div class="kicker">Direction C</div>
					<div class="board-title">Record Book</div>
				</div>
				<p class="board-intro">
					A slab serif for headlines and big numbers, field green for the league and pennant orange
					for titles. Everything else sits on white cards over a cool gray page.
				</p>
			</div>

			<div class="two">
				<section class="stack">
					<div class="section-label">Type</div>
					<div>
						<div class="slab" style="font-size: 60px; line-height: 1">Hall of Fame</div>
						<div class="note" style="margin-top: 6px">Display · Zilla Slab 700 · 52-60</div>
					</div>
					<div>
						<div class="slab" style="font-size: 28px">Power Rankings</div>
						<div class="note">Card title · Zilla Slab 700 · 28</div>
					</div>
					<div>
						<div class="kicker">2026 Season · Week 3 of 14</div>
						<div class="note">Kicker · Instrument Sans 700 · 13 / caps</div>
					</div>
					<div>
						<div style="font-size: 16px; line-height: 1.5; color: var(--color-ink-2)">
							Trevor has seven titles in 22 seasons and an .563 career win rate.
						</div>
						<div class="note">Body &amp; UI · Instrument Sans 400-600 · 15-18</div>
					</div>
					<div style="display: flex; gap: 20px; align-items: baseline">
						<span class="slab" style="font-size: 44px">465.5</span>
						<span style="font-size: 16px; font-weight: 600">424.1 · 380.5 · 337.0</span>
						<span class="note">Big stats slab, table stats sans</span>
					</div>
				</section>

				<section class="stack">
					<div class="section-label">Color</div>
					<div class="swatches">
						{#each swatches as swatch (swatch.token)}
							<div>
								<div class="chip" style:background="var(--color-{swatch.token})"></div>
								<div style="margin-top: 6px; font-size: 14px; font-weight: 600">{swatch.name}</div>
								<div style="font-size: 12px; color: var(--color-muted)">
									{hexOf(swatch.token)} · {swatch.use}
								</div>
							</div>
						{/each}
					</div>
					<p style="margin: 4px 0 0; font-size: 14px; line-height: 1.5; color: var(--color-ink-2)">
						Charts keep their current shapes. Green is series one and "up", orange is series two,
						titles and "down", then the sage and grays. The green band is only the site header.
					</p>
				</section>
			</div>

			<section class="stack">
				<div class="section-label">Components</div>
				<div class="three">
					<div class="specimen">
						<div class="note">Buttons</div>
						<div style="display: flex; gap: 10px; flex-wrap: wrap">
							<Button variant="primary">Submit proposal</Button>
							<Button variant="secondary">Cancel</Button>
						</div>
						<div><Button variant="link" href="#">Full rankings →</Button></div>
						<div class="note" style="margin-top: 6px">Season / week selector</div>
						<div><Segmented label="Week" options={weeks} bind:value={week} /></div>
					</div>

					<div class="specimen">
						<div class="note">Status tags</div>
						<div style="display: flex; gap: 8px; flex-wrap: wrap">
							<Tag variant="clinched">Clinched</Tag>
							<Tag variant="bye">Bye</Tag>
							<Tag variant="eliminated">Eliminated</Tag>
							<Tag variant="champion">Champion</Tag>
						</div>
						<div class="note" style="margin-top: 6px">Movement</div>
						<div style="display: flex; gap: 18px; font-size: 15px">
							<Movement change={4} />
							<Movement change={-2} />
							<Movement change={0} />
						</div>
						<div class="note" style="margin-top: 6px">Odds bar</div>
						<OddsBar percent={85} />
					</div>

					<div class="specimen">
						<div class="note">Stat cell</div>
						<StatTile label="Championships" value="7" sub="Trevor · 22 seasons" />
						<div class="note" style="margin-top: 6px">Table row</div>
						<DataTable label="Career record" columns={rowColumns} rows={rowData} hideHeader>
							{#snippet cell(row, column)}
								{#if column.key === 'rank'}<span class="slab" style="font-size: 18px"
										>{row.rank}</span
									>
								{:else if column.key === 'name'}<span style="font-weight: 600">{row.name}</span>
								{:else}{row[column.key]}{/if}
							{/snippet}
						</DataTable>
					</div>
				</div>
			</section>

			<section class="tips">
				<div style="display: flex; flex-direction: column; gap: 12px">
					<div class="section-label">Stat tooltips</div>
					<p style="margin: 0; font-size: 16px; line-height: 1.5; color: var(--color-ink-2)">
						Any stat you can't read at a glance gets a green <span
							style="color: var(--color-field); font-weight: 700">ⓘ</span
						> after its label, and they all open the same card. W-L, PF and Win % don't need one. Luck,
						Pyth W, Power score, HoF index, draft grades, Hit rate and expected wins do.
					</p>
					<div class="tip-labels">
						<StatTip metricId="pythagorean_wins" label="Pyth W" />
						<StatTip metricId="power_score" label="Power score" />
						<StatTip metricId="hall_of_fame_index" label="HoF index" />
						<StatTip metricId="draft_grade" label="Grade" />
					</div>
					<p style="margin: 0; font-size: 14px; line-height: 1.5; color: var(--color-muted)">
						The card has the same parts every time: name, the category pill, a one-line definition,
						Calculation, How to read it, and a link to the full entry. The copy comes from the Data
						Dictionary, so the two never disagree. It opens on hover or tap, closes with Escape, and
						the label is a real button with aria-describedby.
					</p>
				</div>
				<StatTipCard definition={LUCK_FACTOR} />
			</section>

			<section class="stack" style="gap: 12px">
				<div class="section-label">House rules</div>
				<div class="rules">
					<div>
						<b>Every page opens the same way.</b> Green kicker, slab headline, one gray line. Left aligned,
						no hero icons.
					</div>
					<div>
						<b>One card style.</b> White, 1px border, 12px corners, no shadow. Tables get a 2px ink rule
						under the header.
					</div>
					<div>
						<b>Two colors with jobs.</b> Green is the league and good news. Orange is titles and bad
						news. Nothing else is tinted.
					</div>
					<div>
						<b>Slab for headlines and big numbers.</b> Everything you read in a row is Instrument Sans.
					</div>
				</div>
			</section>
		</div>
	{/snippet}
</Story>

<style>
	.board {
		width: 1120px;
		box-sizing: border-box;
		padding: 56px;
		background: var(--color-ground);
		color: var(--color-ink);
		display: flex;
		flex-direction: column;
		gap: 36px;
	}

	.board-head {
		display: flex;
		justify-content: space-between;
		align-items: flex-end;
		gap: 24px;
		padding-bottom: 20px;
		border-bottom: 2px solid var(--color-ink);
	}

	.board-title {
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 56px;
		line-height: 1.02;
	}

	.board-intro {
		margin: 0;
		max-width: 520px;
		font-size: 17px;
		line-height: 1.5;
		color: var(--color-ink-2);
	}

	.kicker {
		font-size: 13px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-field);
	}

	.slab {
		font-family: var(--font-slab);
		font-weight: 700;
	}

	.note {
		font-size: 13px;
		color: var(--color-muted);
	}

	.section-label {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-ink-2);
	}

	.two {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 40px;
	}

	.stack {
		display: flex;
		flex-direction: column;
		gap: 18px;
	}

	.swatches {
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr));
		gap: 14px;
	}

	/* The board's boxes are content-box: 64px plus the border. */
	.chip {
		box-sizing: content-box;
		height: 64px;
		border-radius: 8px;
		border: 1px solid var(--color-line-strong);
	}

	.three {
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 20px;
	}

	/* The board's specimen boxes, not the Card part: 22px all round. */
	.specimen {
		background: var(--color-card);
		border: 1px solid var(--color-line);
		border-radius: var(--radius-card);
		padding: 22px;
		display: flex;
		flex-direction: column;
		gap: 14px;
	}

	.tips {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 360px;
		gap: 40px;
		align-items: start;
	}

	.tip-labels {
		display: flex;
		flex-wrap: wrap;
		gap: 8px 18px;
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-transform: uppercase;
		color: var(--color-ink-2);
	}

	.rules {
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr));
		gap: 24px;
		font-size: 15px;
		line-height: 1.45;
		color: var(--color-ink-2);
	}

	.rules b {
		color: var(--color-ink);
	}
</style>

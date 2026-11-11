<script lang="ts">
	/**
	 * A season's draft as a grid (WP8 S8.8): one column per manager in round
	 * 1's pick order (boardColumns in $lib/draft-display), each manager's season
	 * draft grade under their name, and one cell per pick, tinted by position.
	 * Each cell is a button that hands its pick to `onpick`; the selected one
	 * carries an ink ring and aria-pressed.
	 *
	 * Grades and tags judge a pick against what its position and round were
	 * expected to score (pickGrade and pickVerdict in $lib/draft-value). Kickers
	 * and defenses get a grade but never a tag. The Show tags chips hide a tag
	 * on the cells; they never hide a cell.
	 */
	import { Avatar, Button, Card, EmptyValue, GradeChip, Tag } from '$lib/components/ui';
	import {
		BOARD_TAGS,
		boardCell,
		boardColumns,
		cellTagCounts,
		pickGradeColor,
		pointsOf,
		signed,
		type BoardPick,
		type BoardTag
	} from '$lib/draft-display';

	interface SeasonGrade {
		managerName: string;
		seasonYear: number | string;
		grade: string;
		score: number;
	}

	interface Props {
		picks: (BoardPick & { playerName: string })[];
		season: string;
		grades: SeasonGrade[];
		selected: number | null;
		onpick: (overallPick: number) => void;
	}

	let { picks, season, grades, selected, onpick }: Props = $props();

	const LEGEND = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF'];

	let hidden = $state<Record<BoardTag, boolean>>({
		steal: false,
		value: false,
		letdown: false,
		bust: false
	});

	const layout = $derived(boardColumns(picks));
	// Each pick's grade, tag, colors and text, worked out once per season.
	const cells = $derived(new Map(picks.map((p) => [p.overallPick, boardCell(p)] as const)));
	const counts = $derived(cellTagCounts(cells.values()));
	const gradeOf = $derived(
		new Map(
			grades
				.filter((g) => String(g.seasonYear) === String(season))
				.map((g) => [g.managerName, g] as const)
		)
	);
</script>

<Card
	title="Draft board"
	padding="20px 20px 16px"
	headAlign="center"
	headSpacing="10px 16px"
	phoneHeadStack
	phoneHeadStackGap={10}
	phoneHeadGap={14}
	class="draft-board"
>
	{#snippet action()}
		<span class="legend" role="group" aria-label="Positions">
			{#each LEGEND as pos (pos)}
				<span class="legend-item"
					><span
						class="swatch"
						aria-hidden="true"
						style:background="var(--color-pos-{pos.toLowerCase()}-tint)"
						style:box-shadow="inset 0 3px 0 var(--color-pos-{pos.toLowerCase()})"
					></span>{pos}</span
				>
			{/each}
		</span>
	{/snippet}

	<div class="tags" role="group" aria-label="Show tags">
		<span class="tags-label">Show tags</span>
		{#each BOARD_TAGS as t (t.key)}
			<Button
				variant="chip"
				aria-pressed={!hidden[t.key]}
				onclick={() => (hidden = { ...hidden, [t.key]: !hidden[t.key] })}
			>
				<Tag variant={t.key} size="sample">{t.label}</Tag>{counts[t.key]} picks
			</Button>
		{/each}
		<span class="tags-note"
			>No tag for picks that scored about what their slot usually does, or for kickers and
			defenses.</span
		>
	</div>
	<p class="how">
		The letter under each name is that manager's {season} draft grade, graded against every draft in
		league history. One column per manager in draft order. Snake order, so the pick numbers run back
		and forth. <span class="click">Click any pick to see it above.</span><span class="tap"
			>Tap any pick to see it above.</span
		>
	</p>

	<div class="scroller">
		<div class="grid-box" style:--cols={layout.columns.length}>
			<div class="grid head">
				<span class="round"></span>
				{#each layout.columns as manager (manager)}
					{@const g = gradeOf.get(manager)}
					<span class="col-head">
						<Avatar name={manager} size={34} alt="" initialTone="field" initialSize={13} />
						<span class="col-name">{manager}</span>
						<span class="col-grade" title="{season} draft grade"
							>{#if g}<GradeChip grade={g.grade} shape="inline" />{signed(g.score, 2)}{:else}<EmptyValue
								/>{/if}</span
						>
					</span>
				{/each}
			</div>
			{#each layout.rows as row (row.round)}
				<div class="grid">
					<span class="round round-label">R{row.round}</span>
					{#each row.cells as pick, i (pick?.overallPick ?? `empty-${i}`)}
						{#if pick}
							{@const c = cells.get(pick.overallPick)!}
							{@const on = pick.overallPick === selected}
							<button
								type="button"
								class="cell"
								class:on
								aria-pressed={on}
								aria-label={c.label}
								style:background={c.bg}
								style:--bar={c.bar}
								onclick={() => onpick(pick.overallPick)}
							>
								<span class="cell-top"
									><span>{c.slot}</span><span
										class="cell-grade"
										style:color="var(--color-{pickGradeColor(c.grade)})"
										>{#if c.grade}{c.grade}{:else}<EmptyValue />{/if}</span
									></span
								>
								<span class="cell-player">{pick.playerName}</span>
								<span class="cell-bottom"
									><span>{pointsOf(pick).toFixed(1)} pts</span>{#if c.tag && !hidden[c.tag]}<Tag
											variant={c.tag}
											size="cell">{c.tagLabel}</Tag
										>{/if}</span
								>
							</button>
						{:else}
							<span></span>
						{/if}
					{/each}
				</div>
			{/each}
		</div>
	</div>
</Card>

<style>
	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 6px 12px;
		align-items: center;
		font-size: 13px;
		font-weight: 600;
	}

	.legend-item {
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}

	.swatch {
		width: 14px;
		height: 14px;
		border-radius: 4px;
	}

	.tags {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px;
	}

	.tags-label {
		margin-right: 2px;
		font-size: 13px;
		font-weight: 600;
		color: var(--color-ink-2);
	}

	.tags-note,
	.how {
		font-size: 13px;
		color: var(--color-muted);
	}

	.how {
		margin: 8px 0 0;
	}

	.tap {
		display: none;
	}

	@media (hover: none) {
		.click {
			display: none;
		}

		.tap {
			display: inline;
		}
	}

	.scroller {
		overflow-x: auto;
		margin-top: 14px;
	}

	.grid-box {
		min-width: 1100px;
	}

	.grid {
		display: grid;
		grid-template-columns: 34px repeat(var(--cols), minmax(0, 1fr));
		gap: 6px;
		padding-top: 6px;
	}

	.grid.head {
		padding-top: 0;
		padding-bottom: 8px;
		border-bottom: 2px solid var(--color-ink);
	}

	.col-head {
		display: flex;
		flex-direction: column;
		align-items: center;
		gap: 4px;
		text-align: center;
	}

	.col-name {
		font-size: 12px;
		font-weight: 700;
		line-height: 1.2;
	}

	.col-grade {
		display: inline-flex;
		align-items: center;
		gap: 4px;
		font-size: 11px;
		color: var(--color-ink-2);
	}

	.round-label {
		align-self: center;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 16px;
		color: var(--color-muted);
	}

	/* A button, so border-box as the board's are: 92px with the padding. */
	.cell {
		position: relative;
		display: flex;
		flex-direction: column;
		gap: 4px;
		min-width: 0;
		min-height: 92px;
		padding: 9px 8px 7px;
		border: 0;
		border-radius: 8px;
		box-shadow:
			inset 0 3px 0 var(--bar),
			0 0 0 0 transparent;
		text-align: left;
		cursor: pointer;
		font-family: var(--font-sans);
		color: var(--color-ink);
	}

	.cell.on {
		box-shadow:
			inset 0 3px 0 var(--bar),
			0 0 0 2.5px var(--color-ink);
	}

	.cell:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.cell-top {
		display: flex;
		justify-content: space-between;
		align-items: center;
		font-size: 11px;
		font-weight: 700;
		color: var(--color-ink-2);
	}

	.cell-grade {
		font-family: var(--font-slab);
		font-size: 14px;
	}

	.cell-player {
		display: block;
		flex: 1;
		font-weight: 700;
		font-size: 13px;
		line-height: 1.22;
		overflow-wrap: anywhere;
	}

	.cell-bottom {
		display: flex;
		flex-wrap: wrap;
		justify-content: space-between;
		align-items: center;
		gap: 2px 4px;
		font-size: 11px;
		color: var(--color-ink-2);
	}

	/* ---- S8.10 phones ---- */
	@media (max-width: 639.98px) {
		.tags-label {
			flex: 1 1 100%;
			margin-right: 0;
		}

		.tags-note {
			flex: 1 1 100%;
		}

		.scroller {
			margin: 14px -16px 0;
		}

		.grid-box {
			width: 1002px;
			min-width: 0;
		}

		.grid {
			grid-template-columns: 48px repeat(var(--cols), 88px);
			gap: 0 6px;
		}

		/* The round column stays put while the managers slide under it. */
		.round {
			position: sticky;
			left: 0;
			z-index: 1;
			padding-left: 16px;
			background: var(--color-card);
		}

		.round-label {
			align-self: stretch;
			display: flex;
			align-items: center;
		}
	}
</style>

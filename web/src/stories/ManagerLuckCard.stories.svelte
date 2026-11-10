<script module lang="ts">
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import ManagerLuckCard from '$lib/components/ManagerLuckCard.svelte';
	import type { ComponentProps } from 'svelte';
	import { seedDefinitions } from './ui/fixtures';

	type Luck = ComponentProps<typeof ManagerLuckCard>['manager'];

	seedDefinitions();

	const { Story } = defineMeta({
		title: 'Components/ManagerLuckCard',
		component: ManagerLuckCard,
		tags: ['autodocs']
	});

	// Gabe the Younger on the Managers board: 16 finished seasons, 2026 live.
	const live: Luck = {
		careerRegularSeasonWins: 147,
		careerPythagoreanWins: 145,
		careerLuckFactor: 2.04,
		luckSeasons: 16,
		latestSeason: 2026,
		currentSeason: {
			seasonYear: 2026,
			throughWeek: 3,
			final: false,
			regularSeasonWins: 1,
			pythagoreanWins: 1.44,
			luckFactor: -0.44,
			playoffOdds: 0.45,
			inPlayoffs: null
		}
	};

	// Trevor between seasons: 2025 is over and counted in the career.
	const final: Luck = {
		careerRegularSeasonWins: 152.5,
		careerPythagoreanWins: 155.4,
		careerLuckFactor: -2.9,
		luckSeasons: 21,
		latestSeason: 2025,
		currentSeason: {
			seasonYear: 2025,
			throughWeek: 14,
			final: true,
			regularSeasonWins: 8,
			pythagoreanWins: 7.6,
			luckFactor: 0.4,
			playoffOdds: null,
			inPlayoffs: null
		}
	};

	// The bracket is set: the odds give way to in or out.
	const bracket: Luck = {
		...live,
		currentSeason: { ...live!.currentSeason!, throughWeek: 14, playoffOdds: null, inPlayoffs: true }
	};

	// Bobby, who left the league: no row for the latest season.
	const satOut: Luck = {
		careerRegularSeasonWins: 41,
		careerPythagoreanWins: 43.2,
		careerLuckFactor: -2.2,
		luckSeasons: 6,
		latestSeason: 2025,
		currentSeason: null
	};
</script>

{#snippet frame(manager: Luck, variant: 'compact' | 'full')}
	<div style="max-width: {variant === 'compact' ? 340 : 560}px">
		<ManagerLuckCard {manager} {variant} />
	</div>
{/snippet}

<Story name="Compact, live season">
	{#snippet template()}{@render frame(live, 'compact')}{/snippet}
</Story>

<Story name="Compact, bracket set">
	{#snippet template()}{@render frame(bracket, 'compact')}{/snippet}
</Story>

<Story name="Compact, final season">
	{#snippet template()}{@render frame(final, 'compact')}{/snippet}
</Story>

<Story name="Compact, didn't play">
	{#snippet template()}{@render frame(satOut, 'compact')}{/snippet}
</Story>

<Story name="Full, live season">
	{#snippet template()}{@render frame(live, 'full')}{/snippet}
</Story>

<Story name="Full, final season">
	{#snippet template()}{@render frame(final, 'full')}{/snippet}
</Story>

<Story name="Full, didn't play">
	{#snippet template()}{@render frame(satOut, 'full')}{/snippet}
</Story>

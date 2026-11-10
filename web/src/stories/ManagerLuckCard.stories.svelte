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

	// Career only: the live season is on Season history and This Season.
	// Gabe the Younger on the Managers board: 16 finished seasons.
	const career: Luck = {
		careerRegularSeasonWins: 147,
		careerPythagoreanWins: 145,
		careerLuckFactor: 2.04,
		luckSeasons: 16,
		latestSeason: 2026
	};

	// Trevor, unlucky over 21 seasons.
	const unlucky: Luck = {
		careerRegularSeasonWins: 153.5,
		careerPythagoreanWins: 156.2,
		careerLuckFactor: -2.69,
		luckSeasons: 21,
		latestSeason: 2026
	};

	// A manager in their first season: no finished season yet.
	const rookie: Luck = {
		careerRegularSeasonWins: null,
		careerPythagoreanWins: null,
		careerLuckFactor: null,
		luckSeasons: 0,
		latestSeason: 2026
	};
</script>

{#snippet frame(manager: Luck, variant: 'compact' | 'full')}
	<div style="max-width: {variant === 'compact' ? 340 : 560}px">
		<ManagerLuckCard {manager} {variant} />
	</div>
{/snippet}

<Story name="Compact, lucky">
	{#snippet template()}{@render frame(career, 'compact')}{/snippet}
</Story>

<Story name="Compact, unlucky">
	{#snippet template()}{@render frame(unlucky, 'compact')}{/snippet}
</Story>

<Story name="Compact, no finished season">
	{#snippet template()}{@render frame(rookie, 'compact')}{/snippet}
</Story>

<Story name="Full, unlucky">
	{#snippet template()}{@render frame(unlucky, 'full')}{/snippet}
</Story>

<Story name="Full, no finished season">
	{#snippet template()}{@render frame(rookie, 'full')}{/snippet}
</Story>

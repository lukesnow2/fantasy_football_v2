<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import GradeChip from '$lib/components/ui/GradeChip.svelte';

	const { Story } = defineMeta({ title: 'UI/GradeChip', component: GradeChip, tags: ['autodocs'] });

	const grades = ['A+', 'A', 'B+', 'B', 'C+', 'C', 'D', 'F'];
</script>

<Story name="Career table, 34px">
	{#snippet template()}
		<div style="display: flex; gap: 12px; align-items: center">
			{#each ['A', 'B', 'C', 'D', 'F'] as grade (grade)}<GradeChip {grade} />{/each}
		</div>
	{/snippet}
</Story>

<Story name="Selected pick, 40px">
	{#snippet template()}
		<div style="display: flex; gap: 12px; align-items: center">
			{#each grades as grade (grade)}<GradeChip {grade} size={40} />{/each}
		</div>
	{/snippet}
</Story>

<Story name="Phone table, 30px">
	{#snippet template()}
		<div style="display: flex; gap: 12px; align-items: center">
			{#each ['A', 'B', 'C', 'D', 'F'] as grade (grade)}<GradeChip {grade} size={30} fontSize={18} />{/each}
		</div>
	{/snippet}
</Story>

<Story name="Inline (board column heads)">
	{#snippet template()}
		<div style="display: flex; gap: 12px; align-items: center">
			{#each grades as grade (grade)}<GradeChip {grade} shape="inline" title="2025 draft grade" />{/each}
		</div>
	{/snippet}
</Story>

<Story name="No grade">
	{#snippet template()}
		<GradeChip grade={null} />
	{/snippet}
</Story>

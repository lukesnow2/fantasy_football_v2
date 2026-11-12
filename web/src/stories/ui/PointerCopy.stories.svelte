<script module>
	import { defineMeta } from '@storybook/addon-svelte-csf';
	import PointerCopy from '$lib/components/ui/PointerCopy.svelte';

	const { Story } = defineMeta({ title: 'UI/PointerCopy', component: PointerCopy, tags: ['autodocs'] });
</script>

<Story name="Hover or Tap (Data Dictionary)">
	{#snippet template()}
		<p style="font-size: 14px">
			<PointerCopy hover="Hover" touch="Tap" /> the ⓘ next to any stat to get its definition.
		</p>
	{/snippet}
</Story>

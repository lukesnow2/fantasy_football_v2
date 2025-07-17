<script lang="ts">
	import { browser } from '$app/environment';
	import { onMount } from 'svelte';
	
	export let data: any = null;
	
	// Extract the actual data from API response
	$: actualData = data?.data ? data.data : data;
	
	let mounted = false;
	
	onMount(() => {
		mounted = true;
	});
</script>

{#if browser && mounted}
	{#await import('$lib/components/D3Overview.svelte') then { default: D3Overview }}
		<D3Overview data={actualData} />
	{/await}
{:else}
	<div class="text-center text-slate-300 py-8">
		<div class="animate-pulse">
			<div class="h-4 bg-slate-700 rounded w-48 mx-auto mb-4"></div>
			<div class="h-96 bg-slate-700/50 rounded"></div>
		</div>
		<p class="mt-4">Loading interactive visualization...</p>
	</div>
{/if} 
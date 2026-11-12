<script lang="ts">
	import { browser } from '$app/environment';
	import { onMount } from 'svelte';

	export let data: any = null;
	/** The era picked above the chart, bound through to D3Overview. */
	export let selectedEra = 'all';

	// Extract the actual data from API response
	$: actualData = data?.data ? data.data : data;

	let mounted = false;

	onMount(() => {
		mounted = true;
	});
</script>

{#if browser && mounted}
	{#await import('$lib/components/D3Overview.svelte')}
		<div class="placeholder" aria-busy="true">
			<div class="block"></div>
			<p>Loading the chart</p>
		</div>
	{:then { default: D3Overview }}
		<D3Overview data={actualData} bind:selectedEra />
	{/await}
{:else}
	<div class="placeholder" aria-busy="true">
		<div class="block"></div>
		<p>Loading the chart</p>
	</div>
{/if}

<style>
	.placeholder {
		margin-top: 20px;
	}

	.block {
		height: 384px;
		border-radius: var(--radius-card);
		background: var(--color-wash);
	}

	p {
		margin: 8px 0 0;
		font-size: 14px;
		color: var(--color-muted);
	}
</style>

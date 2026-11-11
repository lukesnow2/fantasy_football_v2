<script lang="ts">
	/**
	 * A labeled native select (Trade Center and Draft Central, "Season"). The
	 * label is 14px weight 600 ink-2, 8px from the select; the select is 44px
	 * tall with 0 12px of padding, the control radius, a 1px line-strong border
	 * on card fill and 15px weight 600 ink text.
	 *
	 * Below 640px it runs full width: the label on the left and the select
	 * filling the rest, in 16px so iOS Safari doesn't zoom when it opens.
	 *
	 *   <Select label="Season" bind:value={season} onchange={load} options={[...]} />
	 */
	import type { HTMLSelectAttributes } from 'svelte/elements';

	interface Option {
		value: string;
		label: string;
	}

	type Props = {
		label: string;
		options: Option[];
		value?: string;
	} & Omit<HTMLSelectAttributes, 'value' | 'children'>;

	let { label, options, value = $bindable(''), class: className = '', ...rest }: Props = $props();
</script>

<label class="select {className}">
	{label}
	<select bind:value {...rest}>
		{#each options as option (option.value)}
			<option value={option.value}>{option.label}</option>
		{/each}
	</select>
</label>

<style>
	.select {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		font-size: 14px;
		font-weight: 600;
		color: var(--color-ink-2);
	}

	select {
		min-height: 44px;
		padding: 0 12px;
		border-radius: var(--radius-control);
		border: 1px solid var(--color-line-strong);
		background: var(--color-card);
		font-family: var(--font-sans);
		font-size: 15px;
		font-weight: 600;
		color: var(--color-ink);
	}

	select:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	@media (max-width: 639.98px) {
		.select {
			display: flex;
		}

		select {
			flex: 1;
			min-width: 0;
			font-size: 16px;
		}
	}
</style>

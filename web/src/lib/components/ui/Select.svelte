<script lang="ts">
	/**
	 * A labeled native select (Trade Center and Draft Central, "Season"). The
	 * label is 14px weight 600 ink-2, 8px from the select; the select is 44px
	 * tall with 12px of padding (40px on the right, clear of the chevron), the
	 * control radius, a 1px line-strong border on card fill and 15px weight
	 * 600 ink text.
	 *
	 * Below 640px it runs full width: the label on the left and the select
	 * filling the rest, in 16px so iOS Safari doesn't zoom when it opens.
	 *
	 *   <Select label="Season" bind:value={season} onchange={load} options={[...]} />
	 *
	 * `stacked` is the phone picker the Managers and League Table phone boards
	 * draw: the label above in 13px weight 600 ink, 6px over a full-width
	 * select 48px tall with 10px corners and 16px text.
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
		/** The label above a full-width 48px select (the phone pickers). */
		stacked?: boolean;
	} & Omit<HTMLSelectAttributes, 'value' | 'children'>;

	let {
		label,
		options,
		value = $bindable(''),
		stacked = false,
		class: className = '',
		...rest
	}: Props = $props();
</script>

<label class="select {className}" class:stacked>
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

	/* The chevron and the 40px of right padding that clears it come from
	   the base styles. */
	select {
		min-height: 44px;
		padding-block: 0;
		padding-left: 12px;
		border-radius: var(--radius-control);
		border: 1px solid var(--color-line-strong);
		background-color: var(--color-card);
		font-family: var(--font-sans);
		font-size: 15px;
		font-weight: 600;
		color: var(--color-ink);
	}

	.stacked {
		display: flex;
		flex-direction: column;
		align-items: stretch;
		gap: 6px;
		font-size: 13px;
		color: var(--color-ink);
	}

	.stacked select {
		min-height: 48px;
		border-radius: 10px;
		font-size: 16px;
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

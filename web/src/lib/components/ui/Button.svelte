<script lang="ts">
	/**
	 * A button or a link that looks like one (System board, "Buttons").
	 *
	 * - primary: field fill, white text ("Submit proposal")
	 * - secondary: white, a 1px line-strong border, ink text ("Cancel")
	 * - link: field text, weight 600, no box ("Full rankings →")
	 *
	 * Primary and secondary are 44px tall with 0 18px of padding, an 8px radius
	 * and 15px weight 600 text. Pass `href` to render an <a>; everything else
	 * (onclick, type, aria-*) goes through to the element, and `class` adds to
	 * the part's own. A disabled link drops its href, so it can't navigate.
	 */
	import type { Snippet } from 'svelte';
	import type { HTMLAnchorAttributes, HTMLButtonAttributes } from 'svelte/elements';

	type Variant = 'primary' | 'secondary' | 'link';

	type Props = {
		variant?: Variant;
		href?: string;
		children: Snippet;
	} & Omit<HTMLButtonAttributes, 'children'> &
		Omit<HTMLAnchorAttributes, 'children'>;

	let {
		variant = 'primary',
		href,
		children,
		type = 'button',
		class: className = '',
		disabled = false,
		...rest
	}: Props = $props();
</script>

{#if href}
	<a
		{...rest as HTMLAnchorAttributes}
		href={disabled ? undefined : href}
		aria-disabled={disabled || undefined}
		class="btn {variant} {className}">{@render children()}</a
	>
{:else}
	<button {...rest as HTMLButtonAttributes} {type} {disabled} class="btn {variant} {className}"
		>{@render children()}</button
	>
{/if}

<style>
	.btn {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		gap: 6px;
		min-height: 44px;
		padding: 0 18px;
		border: 0;
		border-radius: var(--radius-control);
		font-family: var(--font-sans);
		font-size: 15px;
		font-weight: 600;
		cursor: pointer;
		text-decoration: none;
	}

	.primary {
		background: var(--color-field);
		color: var(--color-white);
	}

	.primary:hover {
		color: var(--color-white);
	}

	.secondary {
		border: 1px solid var(--color-line-strong);
		background: var(--color-card);
		color: var(--color-ink);
	}

	.secondary:hover {
		color: var(--color-ink);
	}

	/* The board's link is a bare anchor: no box, no height, and the
	   `a.field-link` colors, field turning clubhouse. */
	.link {
		display: inline;
		min-height: 0;
		padding: 0;
		border-radius: 0;
		background: transparent;
		color: var(--color-field);
	}

	.link:hover {
		color: var(--color-clubhouse);
	}

	.btn:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.btn:disabled,
	.btn[aria-disabled='true'] {
		cursor: not-allowed;
		opacity: 0.55;
	}
</style>

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
	 * (onclick, type, disabled, aria-*) goes through to the element.
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

	let { variant = 'primary', href, children, type = 'button', ...rest }: Props = $props();
</script>

{#if href}
	<a {href} class="btn {variant}" {...rest as HTMLAnchorAttributes}>{@render children()}</a>
{:else}
	<button {type} class="btn {variant}" {...rest as HTMLButtonAttributes}
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

	/* The board's link is a bare anchor: no box, no height, and the System
	   board's own hover color. */
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

	.btn:disabled {
		cursor: not-allowed;
		opacity: 0.55;
	}
</style>

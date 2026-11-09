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
	 * the part's own. A disabled link drops its href and its onclick, so it
	 * can't navigate or act, and keeps the link role so it's announced as one.
	 */
	import type { Snippet } from 'svelte';
	import type { HTMLAnchorAttributes, HTMLButtonAttributes } from 'svelte/elements';

	type Variant =
		| 'primary'
		| 'secondary'
		| 'link'
		| 'link-pennant'
		| 'band-primary'
		| 'band-outline';
	type Size = 'default' | 'large' | 'tap';

	type Props = {
		variant?: Variant;
		size?: Size;
		href?: string;
		children: Snippet;
	} & Omit<HTMLButtonAttributes, 'children'> &
		Omit<HTMLAnchorAttributes, 'children'>;

	let {
		variant = 'primary',
		size = 'default',
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
		role={disabled ? 'link' : undefined}
		aria-disabled={disabled || undefined}
		onclick={disabled ? undefined : (rest as HTMLAnchorAttributes).onclick}
		class="btn {variant} size-{size} {className}">{@render children()}</a
	>
{:else}
	<button
		{...rest as HTMLButtonAttributes}
		{type}
		{disabled}
		class="btn {variant} size-{size} {className}">{@render children()}</button
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

	.link-pennant {
		display: inline;
		min-height: 0;
		padding: 0;
		border-radius: 0;
		background: transparent;
		color: var(--color-pennant-dark);
	}

	.link-pennant:hover {
		color: var(--color-ink-brown);
	}

	/* A link variant with a 44px touch target, as Home's card links draw it. */
	.link.size-tap,
	.link-pennant.size-tap {
		display: inline-flex;
		min-height: 44px;
	}

	.band-primary {
		background: var(--color-pennant-light);
		color: var(--color-clubhouse);
		font-weight: 700;
	}

	.band-primary:hover {
		color: var(--color-clubhouse);
	}

	.band-outline {
		border: 1px solid var(--color-clubhouse-line);
		background: transparent;
		color: var(--color-white);
	}

	.band-outline:hover {
		background: var(--color-clubhouse-3);
		color: var(--color-white);
	}

	.size-large {
		box-sizing: content-box;
		min-height: 48px;
		padding: 0 22px;
		font-size: 16px;
	}

	.btn:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.band-primary:focus-visible,
	.band-outline:focus-visible {
		outline-color: var(--color-white);
	}

	.btn:disabled,
	.btn[aria-disabled='true'] {
		cursor: not-allowed;
		opacity: 0.55;
	}
</style>

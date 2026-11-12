<script lang="ts">
	/**
	 * A button or a link that looks like one (System board, "Buttons").
	 *
	 * - primary: field fill, white text ("Submit proposal")
	 * - secondary: white, a 1px line-strong border, ink text ("Cancel")
	 * - link: field text, weight 600, no box ("Full rankings →")
	 * - band-white: white fill, clubhouse text, weight 700, for a button on a
	 *   clubhouse card (the Managers hero's "Open full profile →")
	 * - chip: a toggle pill (Draft Central's Show tags), 36px tall (44px below
	 *   640px) with 0 12px 0 8px of padding, 8px between its parts, 13px weight
	 *   600 ink. Pass `aria-pressed`: on is card fill with a line-strong border,
	 *   off is ground fill with a line border at 0.55 opacity
	 * - outline: card fill, a 1px ink border, ink text, weight 700 (the Bet
	 *   Board's "This one's over")
	 * - text: no box, ink-2 text with the button's height and padding (the Bet
	 *   Board's "Cancel"); with `tone="pennant"` it's pennant-dark ("Pull it")
	 *
	 * - filter: a filter chip (League History's record book), a pill with 7px
	 *   12px of padding at 14px weight 600 (44px tall with 0 14px below 640px).
	 *   Pass `aria-pressed`: on is field fill with white text, off is card
	 *   fill, ink, with a 1px line border
	 *
	 * `tone="pennant"` turns primary into a pennant fill with white text ("Record
	 * the ruling") and text into pennant-dark.
	 *
	 * Sizes beyond the default (WP9 adds the last two):
	 * - `large`: 48px with 0 22px of padding at 16px, content-box (Home's band)
	 * - `lg`: 48px with 0 22px of padding at 16px weight 700 ("Post a bet");
	 *   `padX` sets another side padding in px ("Post it", 26)
	 * - `sm`: 40px at 14px (44px below 640px, the phone tap target). Primary and
	 *   outline are 0 16px at weight 700, secondary 0 14px at 600, text 0 12px at
	 *   600, and the pennant text button is 13px ("Take it", "Pass", "Pull it")
	 * - `xs` (WP10): 40px at 13px weight 600, secondary 0 14px (44px below
	 *   640px): League History's "Save image" and "Show table"
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
		| 'band-outline'
		| 'band-white'
		| 'chip'
		| 'outline'
		| 'text'
		| 'filter';
	type Size = 'default' | 'large' | 'tap' | 'lg' | 'sm' | 'xs';
	type Tone = 'default' | 'pennant';

	type Props = {
		variant?: Variant;
		size?: Size;
		tone?: Tone;
		/** Side padding in px, for a board that draws another (lg's "Post it"). */
		padX?: number;
		href?: string;
		children: Snippet;
	} & Omit<HTMLButtonAttributes, 'children'> &
		Omit<HTMLAnchorAttributes, 'children'>;

	let {
		variant = 'primary',
		size = 'default',
		tone = 'default',
		padX,
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
		style:--btn-pad-x={padX == null ? undefined : `${padX}px`}
		class="btn {variant} size-{size} tone-{tone} {className}">{@render children()}</a
	>
{:else}
	<button
		{...rest as HTMLButtonAttributes}
		{type}
		{disabled}
		style:--btn-pad-x={padX == null ? undefined : `${padX}px`}
		class="btn {variant} size-{size} tone-{tone} {className}">{@render children()}</button
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

	.band-white {
		background: var(--color-white);
		color: var(--color-clubhouse);
		font-weight: 700;
	}

	.band-white:hover {
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

	.chip {
		gap: 8px;
		min-height: 36px;
		padding: 0 12px 0 8px;
		border: 1px solid var(--color-line-strong);
		border-radius: 999px;
		background: var(--color-card);
		color: var(--color-ink);
		font-size: 13px;
	}

	.chip:hover {
		color: var(--color-ink);
	}

	.chip[aria-pressed='false'] {
		border-color: var(--color-line);
		background: var(--color-ground);
		opacity: 0.55;
	}

	@media (max-width: 639.98px) {
		.chip {
			min-height: 44px;
		}
	}

	.filter {
		gap: 4px;
		min-height: 0;
		padding: 7px 12px;
		border: 1px solid var(--color-line);
		border-radius: 999px;
		background: var(--color-card);
		color: var(--color-ink);
		font-size: 14px;
	}

	.filter:hover {
		color: var(--color-field);
	}

	.filter[aria-pressed='true'],
	.filter[aria-pressed='true']:hover {
		border-color: var(--color-field);
		background: var(--color-field);
		color: var(--color-white);
	}

	@media (max-width: 639.98px) {
		.filter {
			min-height: 44px;
			padding: 0 14px;
		}
	}

	.size-large {
		box-sizing: content-box;
		min-height: 48px;
		padding: 0 22px;
		font-size: 16px;
	}

	@media (max-width: 639.98px) {
		.size-large {
			box-sizing: border-box;
		}
	}

	.outline {
		border: 1px solid var(--color-ink);
		background: var(--color-card);
		color: var(--color-ink);
		font-weight: 700;
	}

	.outline:hover {
		color: var(--color-ink);
	}

	.text {
		background: transparent;
		color: var(--color-ink-2);
	}

	.text:hover {
		color: var(--color-ink);
	}

	.primary.tone-pennant {
		background: var(--color-pennant);
	}

	.text.tone-pennant {
		color: var(--color-pennant-dark);
	}

	.text.tone-pennant:hover {
		color: var(--color-ink-brown);
	}

	/* <button>s draw border-box on the boards, so these heights include the
	   border. */
	.size-lg {
		min-height: 48px;
		padding: 0 var(--btn-pad-x, 22px);
		font-size: 16px;
		font-weight: 700;
	}

	.size-sm {
		min-height: 40px;
		padding: 0 var(--btn-pad-x, 16px);
		font-size: 14px;
	}

	.size-sm.primary,
	.size-sm.outline {
		font-weight: 700;
	}

	.size-sm.secondary {
		padding: 0 var(--btn-pad-x, 14px);
	}

	.size-sm.text {
		padding: 0 var(--btn-pad-x, 12px);
	}

	.size-sm.text.tone-pennant {
		font-size: 13px;
	}

	.size-xs {
		min-height: 40px;
		padding: 0 var(--btn-pad-x, 14px);
		font-size: 13px;
	}

	@media (max-width: 639.98px) {
		.size-sm,
		.size-xs {
			min-height: 44px;
		}
	}

	.btn:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.band-primary:focus-visible,
	.band-outline:focus-visible,
	.band-white:focus-visible {
		outline-color: var(--color-white);
	}

	.btn:disabled,
	.btn[aria-disabled='true'] {
		cursor: not-allowed;
		opacity: 0.55;
	}
</style>

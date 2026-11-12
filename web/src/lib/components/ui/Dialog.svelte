<script lang="ts">
	/**
	 * A modal dialog (the Constitution's "Propose a change", WP11 S11.5): the
	 * page dimmed under ink at 55%, and a white dialog up to 640px wide with
	 * 16px corners and the dialog shadow, 48px from the top. Its header has a
	 * field kicker over a 28px slab title, a 40px close button on the right and
	 * a line-soft rule under it. The body is the page's own.
	 *
	 *   <Dialog kicker="Article II · Trades" title="Propose a change" onClose={close}>
	 *     <form>...</form>
	 *   </Dialog>
	 *
	 * Mount it to open it. Focus moves to the first control inside, Tab stays
	 * inside, Escape and the close button call `onClose`, and on unmount focus
	 * goes back to whatever had it before. The page behind doesn't scroll.
	 *
	 * Below 640px it's a full-screen sheet with no backdrop showing: card fill,
	 * no corners or shadow, scrolling inside itself, its header stuck to the
	 * top (14px 16px, a 22px title, a 44px close button), as the phone board
	 * draws it.
	 */
	import { onMount, tick, type Snippet } from 'svelte';

	interface Props {
		title: string;
		kicker?: string;
		onClose: () => void;
		/** The label of the close button. */
		closeLabel?: string;
		children: Snippet;
	}

	let { title, kicker, onClose, closeLabel = 'Close', children }: Props = $props();

	const id = $props.id();
	const titleId = `dialog-title-${id}`;
	let box: HTMLDivElement | undefined = $state();

	const FOCUSABLE =
		'a[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

	function focusables(): HTMLElement[] {
		return box ? [...box.querySelectorAll<HTMLElement>(FOCUSABLE)] : [];
	}

	onMount(() => {
		const before = document.activeElement as HTMLElement | null;
		const root = document.documentElement;
		const overflow = root.style.overflow;
		root.style.overflow = 'hidden';

		tick().then(() => {
			// The first control in the body, after the header's close button.
			const all = focusables();
			(all.find((el) => !el.closest('.head')) ?? all[0])?.focus();
		});

		return () => {
			root.style.overflow = overflow;
			if (before && document.contains(before)) before.focus();
		};
	});

	function onkeydown(event: KeyboardEvent) {
		if (event.key === 'Escape') {
			event.stopPropagation();
			onClose();
			return;
		}
		if (event.key !== 'Tab') return;
		const all = focusables();
		if (all.length === 0) return;
		const first = all[0];
		const last = all[all.length - 1];
		const active = document.activeElement;
		if (event.shiftKey && (active === first || !box?.contains(active))) {
			event.preventDefault();
			last.focus();
		} else if (!event.shiftKey && active === last) {
			event.preventDefault();
			first.focus();
		}
	}
</script>

<div class="backdrop">
	<!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
	<div
		bind:this={box}
		class="dialog"
		role="dialog"
		aria-modal="true"
		aria-labelledby={titleId}
		tabindex="-1"
		{onkeydown}
	>
		<div class="head">
			<div class="titles">
				{#if kicker}<div class="kicker">{kicker}</div>{/if}
				<h2 id={titleId}>{title}</h2>
			</div>
			<button type="button" class="close" aria-label={closeLabel} onclick={onClose}>
				<svg
					width="18"
					height="18"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
					stroke-linecap="round"
					aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"></path></svg
				>
			</button>
		</div>
		{@render children()}
	</div>
</div>

<style>
	.backdrop {
		position: fixed;
		inset: 0;
		z-index: 60;
		display: flex;
		justify-content: center;
		align-items: flex-start;
		padding: 48px 24px;
		overflow-y: auto;
		overscroll-behavior: contain;
		background: color-mix(in srgb, var(--color-ink) 55%, transparent);
	}

	.dialog {
		width: 100%;
		max-width: 640px;
		background: var(--color-card);
		color: var(--color-ink);
		border-radius: 16px;
		box-shadow: var(--shadow-dialog);
		overflow: hidden;
	}

	.dialog:focus {
		outline: none;
	}

	.head {
		display: flex;
		justify-content: space-between;
		align-items: flex-start;
		gap: 12px;
		padding: 22px 26px;
		border-bottom: 1px solid var(--color-line-soft);
		background: var(--color-card);
	}

	.kicker {
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-field);
	}

	h2 {
		margin: 4px 0 0;
		font-family: var(--font-slab);
		font-weight: 700;
		font-size: 28px;
	}

	.close {
		flex: none;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 40px;
		height: 40px;
		padding: 0;
		border: 0;
		border-radius: var(--radius-control);
		background: var(--color-ground);
		color: var(--color-ink-2);
		cursor: pointer;
	}

	.close:hover {
		background: var(--color-wash);
	}

	.close:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	@media (max-width: 639.98px) {
		.backdrop {
			display: block;
			padding: 0;
			background: var(--color-card);
		}

		.dialog {
			max-width: none;
			min-height: 100%;
			border-radius: 0;
			box-shadow: none;
			overflow: visible;
		}

		.head {
			position: sticky;
			top: 0;
			z-index: 1;
			padding: 14px 16px;
		}

		.titles {
			padding-top: 2px;
		}

		h2 {
			font-size: 22px;
			line-height: 1.2;
		}

		.close {
			width: 44px;
			height: 44px;
		}
	}
</style>

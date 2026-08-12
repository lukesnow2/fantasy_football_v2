<script lang="ts">
	import { autoUpdate, computePosition, flip, offset, shift } from '@floating-ui/dom';
	import { clickOutside } from '$lib/actions/clickOutside';
	import { EMOJI_DATA_SOURCE } from '$lib/chat/constants';
	import type { EmojiType } from '$lib/chat/types';

	interface Props {
		anchor: HTMLElement | null;
		onSelect: (choice: { emoji: string; emojiType: EmojiType }) => void;
		onClose: () => void;
	}

	let { anchor, onSelect, onClose }: Props = $props();

	let host = $state<HTMLDivElement | null>(null);

	$effect(() => {
		if (!host || !anchor) return;

		const hostEl = host;
		const anchorEl = anchor;
		let disposed = false;
		let stopAutoUpdate: (() => void) | undefined;
		let picker: HTMLElement | null = null;

		(async () => {
			// emoji-picker-element registers a custom element and opens IndexedDB at
			// import time, so it can only load in the browser. `$effect` never runs
			// during SSR, which is the guard — no `typeof window` check needed.
			await import('emoji-picker-element');
			if (disposed) return;

			picker = document.createElement('emoji-picker');
			// Self-hosted dataset. The element defaults to fetching ~440kB of emoji
			// data from jsDelivr at runtime, which is a third-party request on every
			// first open, breaks offline, and would be blocked by any CSP worth
			// having. The copy in /static comes from emoji-picker-element-data.
			picker.setAttribute('data-source', EMOJI_DATA_SOURCE);
			picker.addEventListener('emoji-click', (event: Event) => {
				const detail = (event as CustomEvent).detail;
				if (detail?.emoji?.url) {
					onSelect({ emoji: detail.emoji.name, emojiType: 'custom' });
				} else if (detail?.unicode) {
					onSelect({ emoji: detail.unicode, emojiType: 'unicode' });
				}
			});
			hostEl.appendChild(picker);

			stopAutoUpdate = autoUpdate(anchorEl, hostEl, async () => {
				const { x, y } = await computePosition(anchorEl, hostEl, {
					// `fixed` is the entire point. The message list is an
					// overflow-y-auto scroller, so an absolutely-positioned popover on
					// one of the last messages gets clipped by it and no z-index helps.
					// Fixed positioning escapes the clip; flip and shift keep it onscreen.
					strategy: 'fixed',
					placement: 'bottom-end',
					middleware: [
						offset(6),
						flip({ fallbackPlacements: ['top-end', 'bottom-start', 'top-start'] }),
						shift({ padding: 8 })
					]
				});
				Object.assign(hostEl.style, { left: `${x}px`, top: `${y}px` });
			});
		})();

		return () => {
			disposed = true;
			// autoUpdate re-runs on every scroll and resize, because the anchor moves
			// whenever the list scrolls or a poll inserts a message above it.
			stopAutoUpdate?.();
			picker?.remove();
		};
	});

	function onKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape') {
			event.stopPropagation();
			onClose();
		}
	}
</script>

<svelte:window onkeydown={onKeydown} />

<div
	bind:this={host}
	use:clickOutside={{ onOutside: onClose, ignore: [anchor] }}
	class="fixed z-50 overflow-hidden rounded-xl border border-slate-700 shadow-2xl"
	style="--background:#1e293b; --border-color:#334155; --input-border-color:#475569;
	       --input-font-color:#f1f5f9; --input-placeholder-color:#94a3b8;
	       --category-font-color:#cbd5e1; --indicator-color:#f59e0b;
	       --button-hover-background:#334155; --button-active-background:#475569;
	       --emoji-size:1.375rem; --num-columns:8;"
	role="dialog"
	aria-label="Choose an emoji"
></div>

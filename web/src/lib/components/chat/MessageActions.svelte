<script lang="ts">
	import { autoUpdate, computePosition, flip, offset, shift } from '@floating-ui/dom';
	import { MediaQuery } from 'svelte/reactivity';
	import { clickOutside } from '$lib/actions/clickOutside';
	import { MAX_RECENT_EMOJI, QUICK_EMOJI, RECENT_EMOJI_STORAGE_KEY } from '$lib/chat/constants';
	import type { EmojiType } from '$lib/chat/types';

	interface Props {
		isAuthor: boolean;
		canReply: boolean;
		onReact: (emoji: string, emojiType: EmojiType) => void;
		onOpenPicker: (anchor: HTMLElement) => void;
		onReply: () => void;
		onEdit: () => void;
		onDelete: () => void;
	}

	let { isAuthor, canReply, onReact, onOpenPicker, onReply, onEdit, onDelete }: Props = $props();

	let menuOpen = $state(false);
	let confirmingDelete = $state(false);
	let menuTrigger = $state<HTMLButtonElement | null>(null);
	let menu = $state<HTMLDivElement | null>(null);

	const touch = new MediaQuery('hover: none');

	// The board's labels for the league's three defaults; any other emoji is
	// named by itself.
	const EMOJI_NAMES: Record<string, string> = {
		'👍': 'thumbs up',
		'🔥': 'fire',
		'😂': 'laughing'
	};

	// The three offered emoji start as league defaults and become whatever this
	// person actually reaches for. Read once, lazily, so it survives a reload
	// without a round trip.
	function readRecent(): string[] {
		if (typeof localStorage === 'undefined') return [];
		try {
			const raw = JSON.parse(localStorage.getItem(RECENT_EMOJI_STORAGE_KEY) ?? '[]');
			return Array.isArray(raw) ? raw.filter((e) => typeof e === 'string') : [];
		} catch {
			return [];
		}
	}

	let recent = $state<string[]>(readRecent());
	const quick = $derived(recent.length >= 3 ? recent.slice(0, 3) : QUICK_EMOJI);

	export function rememberEmoji(emoji: string) {
		const next = [emoji, ...recent.filter((e) => e !== emoji)].slice(0, MAX_RECENT_EMOJI);
		recent = next;
		try {
			localStorage.setItem(RECENT_EMOJI_STORAGE_KEY, JSON.stringify(next));
		} catch {
			// Private mode, quota, whatever — the defaults still work.
		}
	}

	function quickReact(emoji: string) {
		rememberEmoji(emoji);
		onReact(emoji, 'unicode');
	}

	function closeMenu() {
		menuOpen = false;
		confirmingDelete = false;
	}

	// Same clipping problem as the emoji picker: this menu lives inside the
	// overflow-y-auto message list, so it has to be positioned fixed to escape it.
	// On a touch screen the toolbar sits inside the row, so the menu flips
	// above it when the row is at the bottom of the feed, as the phone board
	// draws it: the feed, not the window, is the space it has to fit in. On
	// desktop it opens below the ⋯ as the Chat board draws it.
	$effect(() => {
		if (!menuOpen || !menu || !menuTrigger) return;
		const menuEl = menu;
		const triggerEl = menuTrigger;
		const feed = touch.current ? triggerEl.closest<HTMLElement>('[role="log"]') : null;

		return autoUpdate(triggerEl, menuEl, async () => {
			const { x, y } = await computePosition(triggerEl, menuEl, {
				strategy: 'fixed',
				placement: 'bottom-end',
				middleware: [offset(4), flip(feed ? { boundary: feed } : undefined), shift({ padding: 8 })]
			});
			Object.assign(menuEl.style, { left: `${x}px`, top: `${y}px` });
		});
	});
</script>

<!-- Shown on hover *and* focus-within: the old action bar was hover-only, so
     none of this was reachable from the keyboard. On a touch screen the row
     opens it on a tap instead (MessageRow). -->
<div class="toolbar" role="toolbar" aria-label="Message actions">
	{#each quick as emoji (emoji)}
		<button
			type="button"
			class="tool emoji"
			title="React with {EMOJI_NAMES[emoji] ?? emoji}"
			aria-label="React with {EMOJI_NAMES[emoji] ?? emoji}"
			onclick={() => quickReact(emoji)}
		>
			{emoji}
		</button>
	{/each}

	<button
		type="button"
		class="tool"
		title="Add a reaction"
		aria-label="Add a reaction"
		onclick={(event) => onOpenPicker(event.currentTarget as HTMLElement)}
	>
		<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"
			><circle cx="11" cy="13" r="8"></circle><path d="M8 15c.8 1 1.8 1.5 3 1.5s2.2-.5 3-1.5M8.5 11h.01M13.5 11h.01"></path><path d="M19 2v6M16 5h6"></path></svg
		>
	</button>

	{#if canReply}
		<button
			type="button"
			class="tool"
			title="Reply in thread"
			aria-label="Reply in thread"
			onclick={onReply}
		>
			<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"
				><path d="M21 12a8 8 0 0 1-11.5 7.2L4 20l1-4.5A8 8 0 1 1 21 12z"></path></svg
			>
		</button>
	{/if}

	{#if isAuthor}
		<button
			bind:this={menuTrigger}
			type="button"
			class="tool more"
			class:open={menuOpen}
			title="More actions"
			aria-label="More actions"
			aria-expanded={menuOpen}
			aria-haspopup="menu"
			onclick={() => (menuOpen ? closeMenu() : (menuOpen = true))}
		>
			⋯
		</button>

		{#if menuOpen}
			<div
				bind:this={menu}
				use:clickOutside={{ onOutside: closeMenu, ignore: [menuTrigger] }}
				role="menu"
				class="menu"
			>
				<button
					type="button"
					role="menuitem"
					class="item"
					onclick={() => {
						closeMenu();
						onEdit();
					}}
				>
					Edit
				</button>

				<!-- Two-state button rather than window.confirm, which is modal,
				     unstyled, and blocks the whole tab. -->
				<button
					type="button"
					role="menuitem"
					class="item"
					class:confirm={confirmingDelete}
					onclick={() => {
						if (!confirmingDelete) {
							confirmingDelete = true;
							return;
						}
						closeMenu();
						onDelete();
					}}
				>
					{confirmingDelete ? 'Really delete?' : 'Delete'}
				</button>
			</div>
		{/if}
	{/if}
</div>

<style>
	.toolbar {
		display: flex;
		gap: 2px;
		padding: 3px;
		border: 1px solid var(--color-line);
		border-radius: 10px;
		background: var(--color-card);
		box-shadow: var(--shadow-toolbar);
	}

	.tool {
		width: 32px;
		height: 32px;
		padding: 0;
		border: 0;
		border-radius: 8px;
		background: transparent;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		color: var(--color-ink-2);
		font-family: var(--font-sans);
		cursor: pointer;
	}

	.tool:hover {
		background: var(--color-field-wash);
	}

	.tool:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: -2px;
	}

	.emoji {
		font-size: 16px;
		line-height: 1;
	}

	.more {
		font-weight: 700;
	}

	.more.open {
		background: var(--color-wash);
	}

	.menu {
		position: fixed;
		z-index: 50;
		box-sizing: border-box;
		width: 170px;
		padding: 4px;
		border: 1px solid var(--color-line);
		border-radius: 10px;
		background: var(--color-card);
		box-shadow: var(--shadow-popover);
	}

	.item {
		display: block;
		width: 100%;
		padding: 9px 10px;
		border: 0;
		border-radius: 6px;
		background: transparent;
		text-align: left;
		font-family: var(--font-sans);
		font-size: 14px;
		color: var(--color-ink);
		cursor: pointer;
	}

	.item:hover,
	.item:focus-visible {
		background: var(--color-field-wash);
		outline: none;
	}

	.item.confirm {
		background: var(--color-pennant-tint);
		color: var(--color-pennant-dark);
		font-weight: 700;
	}

	@media (hover: none) {
		.tool {
			width: 44px;
			height: 44px;
		}

		.item {
			min-height: 44px;
			padding: 0 10px;
		}
	}
</style>

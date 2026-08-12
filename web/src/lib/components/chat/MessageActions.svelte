<script lang="ts">
	import { autoUpdate, computePosition, flip, offset, shift } from '@floating-ui/dom';
	import { MessageSquare, MoreHorizontal, Pencil, Smile, Trash2 } from 'lucide-svelte';
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
	$effect(() => {
		if (!menuOpen || !menu || !menuTrigger) return;
		const menuEl = menu;
		const triggerEl = menuTrigger;

		return autoUpdate(triggerEl, menuEl, async () => {
			const { x, y } = await computePosition(triggerEl, menuEl, {
				strategy: 'fixed',
				placement: 'bottom-end',
				middleware: [offset(4), flip(), shift({ padding: 8 })]
			});
			Object.assign(menuEl.style, { left: `${x}px`, top: `${y}px` });
		});
	});
</script>

<!-- Shown on hover *and* focus-within: the old action bar was hover-only, so
     none of this was reachable from the keyboard. -->
<div
	class="absolute -top-3 right-2 flex items-center gap-0.5 rounded-lg border border-slate-600 bg-slate-800 p-0.5 opacity-0 shadow-lg transition-opacity group-hover:opacity-100 group-focus-within:opacity-100"
>
	{#each quick as emoji (emoji)}
		<button
			type="button"
			title="React with {emoji}"
			aria-label="React with {emoji}"
			onclick={() => quickReact(emoji)}
			class="rounded px-1.5 py-0.5 text-sm leading-none transition-colors hover:bg-slate-700"
		>
			{emoji}
		</button>
	{/each}

	<button
		type="button"
		title="Add a reaction"
		aria-label="Add a reaction"
		onclick={(event) => onOpenPicker(event.currentTarget as HTMLElement)}
		class="rounded p-1 text-slate-400 transition-colors hover:bg-slate-700 hover:text-slate-200"
	>
		<Smile class="h-4 w-4" />
	</button>

	{#if canReply}
		<button
			type="button"
			title="Reply in thread"
			aria-label="Reply in thread"
			onclick={onReply}
			class="rounded p-1 text-slate-400 transition-colors hover:bg-slate-700 hover:text-slate-200"
		>
			<MessageSquare class="h-4 w-4" />
		</button>
	{/if}

	{#if isAuthor}
		<div class="relative">
			<button
				bind:this={menuTrigger}
				type="button"
				title="More actions"
				aria-label="More actions"
				aria-expanded={menuOpen}
				aria-haspopup="menu"
				onclick={() => (menuOpen = !menuOpen)}
				class="rounded p-1 text-slate-400 transition-colors hover:bg-slate-700 hover:text-slate-200"
			>
				<MoreHorizontal class="h-4 w-4" />
			</button>

			{#if menuOpen}
				<div
					bind:this={menu}
					use:clickOutside={{ onOutside: closeMenu, ignore: [menuTrigger] }}
					role="menu"
					class="fixed z-50 w-40 rounded-lg border border-slate-600 bg-slate-800 py-1 shadow-xl"
				>
					<button
						type="button"
						role="menuitem"
						onclick={() => {
							closeMenu();
							onEdit();
						}}
						class="flex w-full items-center gap-2 px-3 py-1.5 text-left text-xs text-slate-300 hover:bg-slate-700 hover:text-white"
					>
						<Pencil class="h-3.5 w-3.5" /> Edit
					</button>

					<!-- Two-state button rather than window.confirm, which is modal,
					     unstyled, and blocks the whole tab. -->
					<button
						type="button"
						role="menuitem"
						onclick={() => {
							if (!confirmingDelete) {
								confirmingDelete = true;
								return;
							}
							closeMenu();
							onDelete();
						}}
						class="flex w-full items-center gap-2 px-3 py-1.5 text-left text-xs hover:bg-slate-700
							{confirmingDelete ? 'font-medium text-red-300' : 'text-red-400'}"
					>
						<Trash2 class="h-3.5 w-3.5" />
						{confirmingDelete ? 'Really delete?' : 'Delete'}
					</button>
				</div>
			{/if}
		</div>
	{/if}
</div>

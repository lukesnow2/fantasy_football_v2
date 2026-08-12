<script lang="ts">
	import { Send, Smile } from 'lucide-svelte';
	import MentionMenu from './MentionMenu.svelte';
	import EmojiPickerPopover from './EmojiPickerPopover.svelte';
	import { MAX_MESSAGE_LENGTH } from '$lib/chat/constants';
	import type { RosterMember } from '$lib/chat/types';

	interface Props {
		roster: RosterMember[];
		placeholder?: string;
		disabled?: boolean;
		disabledReason?: string | null;
		autofocus?: boolean;
		onSend: (content: string) => void;
		/** Up-arrow on an empty composer edits your last message, Slack-style. */
		onEditLast?: () => void;
	}

	let {
		roster,
		placeholder = 'Drop some trash talk…',
		disabled = false,
		disabledReason = null,
		autofocus = false,
		onSend,
		onEditLast
	}: Props = $props();

	let value = $state('');
	let textarea = $state<HTMLTextAreaElement | null>(null);
	let emojiAnchor = $state<HTMLElement | null>(null);

	// Mention autocomplete state. `mentionStart` is the index of the '@'.
	let mentionStart = $state(-1);
	let mentionQuery = $state('');
	let mentionIndex = $state(0);

	const matches = $derived.by(() => {
		if (mentionStart === -1) return [];
		const query = mentionQuery.toLowerCase();
		const scored = roster
			.map((member) => {
				const name = member.displayName.toLowerCase();
				if (query === '') return { member, rank: 1 };
				if (name.startsWith(query)) return { member, rank: 0 };
				if (name.includes(query)) return { member, rank: 1 };
				return null;
			})
			.filter((entry): entry is { member: RosterMember; rank: number } => entry !== null);

		// Prefix matches first — typing "ga" should offer Gabe before anyone who
		// merely contains "ga".
		scored.sort((a, b) => a.rank - b.rank || a.member.displayName.localeCompare(b.member.displayName));
		return scored.slice(0, 8).map((entry) => entry.member);
	});

	const remaining = $derived(MAX_MESSAGE_LENGTH - value.length);
	const overLimit = $derived(remaining < 0);
	const canSend = $derived(!disabled && !overLimit && value.trim() !== '');

	export function focus() {
		textarea?.focus();
	}

	export function setValue(next: string) {
		value = next;
		queueMicrotask(() => {
			textarea?.focus();
			autosize();
		});
	}

	function autosize() {
		if (!textarea) return;
		textarea.style.height = 'auto';
		// A textarea, not an input — shift+enter for a newline is impossible in an
		// input, which is what the old composer used. Capped at eight rows so a long
		// message can't eat the whole panel.
		const maxHeight = 8 * 20 + 16;
		textarea.style.height = `${Math.min(textarea.scrollHeight, maxHeight)}px`;
	}

	function refreshMentionState() {
		if (!textarea) return;
		const caret = textarea.selectionStart ?? value.length;
		const before = value.slice(0, caret);
		const at = before.lastIndexOf('@');

		if (at === -1) {
			mentionStart = -1;
			return;
		}
		// Only trigger at a word boundary, so an email address doesn't open the menu.
		const charBefore = at === 0 ? '' : before[at - 1];
		if (charBefore !== '' && !/\s/.test(charBefore)) {
			mentionStart = -1;
			return;
		}
		const query = before.slice(at + 1);
		// A space inside the fragment is allowed (names have spaces) but a newline
		// ends the attempt, as does an over-long run with no match.
		if (query.includes('\n') || query.length > 30) {
			mentionStart = -1;
			return;
		}

		mentionStart = at;
		mentionQuery = query;
		mentionIndex = 0;
	}

	function acceptMention(member: RosterMember) {
		if (!textarea || mentionStart === -1) return;
		const caret = textarea.selectionStart ?? value.length;
		const next = `${value.slice(0, mentionStart)}@${member.displayName} ${value.slice(caret)}`;
		const caretAfter = mentionStart + member.displayName.length + 2;
		value = next;
		mentionStart = -1;
		queueMicrotask(() => {
			textarea?.focus();
			textarea?.setSelectionRange(caretAfter, caretAfter);
			autosize();
		});
	}

	function submit() {
		if (!canSend) return;
		onSend(value.trim());
		value = '';
		mentionStart = -1;
		queueMicrotask(autosize);
	}

	function onKeydown(event: KeyboardEvent) {
		if (mentionStart !== -1 && matches.length > 0) {
			if (event.key === 'ArrowDown') {
				event.preventDefault();
				mentionIndex = (mentionIndex + 1) % matches.length;
				return;
			}
			if (event.key === 'ArrowUp') {
				event.preventDefault();
				mentionIndex = (mentionIndex - 1 + matches.length) % matches.length;
				return;
			}
			if (event.key === 'Enter' || event.key === 'Tab') {
				event.preventDefault();
				acceptMention(matches[mentionIndex]);
				return;
			}
			if (event.key === 'Escape') {
				event.preventDefault();
				mentionStart = -1;
				return;
			}
		}

		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			submit();
			return;
		}
		// Cmd/Ctrl+Enter sends too — muscle memory from every other chat app.
		if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
			event.preventDefault();
			submit();
			return;
		}
		if (event.key === 'ArrowUp' && value === '' && onEditLast) {
			event.preventDefault();
			onEditLast();
		}
		// Escape with a draft is deliberately a no-op. Nothing here is worth
		// throwing away someone's half-written message for.
	}

	let pickerOpen = $state(false);

	function insertEmoji(emoji: string) {
		if (!textarea) {
			value += emoji;
			return;
		}
		const caret = textarea.selectionStart ?? value.length;
		value = `${value.slice(0, caret)}${emoji}${value.slice(caret)}`;
		const caretAfter = caret + emoji.length;
		queueMicrotask(() => {
			textarea?.focus();
			textarea?.setSelectionRange(caretAfter, caretAfter);
			autosize();
		});
	}
</script>

<div class="border-t border-slate-700/60 bg-slate-800/60 p-3">
	{#if disabled && disabledReason}
		<p class="mb-2 rounded-lg border border-amber-600/40 bg-amber-900/20 px-3 py-2 text-xs text-amber-200">
			{disabledReason}
		</p>
	{/if}

	<div class="flex items-end gap-2">
		<div class="relative flex-1">
			<textarea
				bind:this={textarea}
				bind:value
				{placeholder}
				{disabled}
				rows="1"
				aria-label="Message"
				oninput={() => {
					autosize();
					refreshMentionState();
				}}
				onclick={refreshMentionState}
				onkeydown={onKeydown}
				onblur={() => (mentionStart = -1)}
				{...autofocus ? { autofocus: true } : {}}
				class="max-h-44 w-full resize-none rounded-lg border border-slate-600 bg-slate-700 px-3 py-2.5 text-sm text-white placeholder-slate-400 focus:border-amber-500 focus:outline-none disabled:opacity-50"
			></textarea>

			<MentionMenu
				anchor={textarea}
				{matches}
				activeIndex={mentionIndex}
				onPick={acceptMention}
			/>
		</div>

		<button
			bind:this={emojiAnchor}
			type="button"
			{disabled}
			aria-label="Insert an emoji"
			title="Insert an emoji"
			onclick={() => (pickerOpen = !pickerOpen)}
			class="rounded-lg border border-slate-600 bg-slate-700 p-2.5 text-slate-300 transition-colors hover:bg-slate-600 hover:text-white disabled:opacity-50"
		>
			<Smile class="h-4 w-4" />
		</button>

		<button
			type="button"
			disabled={!canSend}
			onclick={submit}
			aria-label="Send message"
			class="rounded-lg bg-amber-500 px-4 py-2.5 font-medium text-black transition-colors hover:bg-amber-400 disabled:cursor-not-allowed disabled:opacity-40"
		>
			<Send class="h-4 w-4" />
		</button>
	</div>

	<div class="mt-1.5 flex items-center justify-between text-[11px] text-slate-500">
		<span>Enter to send · Shift+Enter for a new line · @ to mention</span>
		{#if remaining < 200}
			<span class:text-red-400={overLimit} class="tabular-nums">{remaining}</span>
		{/if}
	</div>
</div>

{#if pickerOpen}
	<EmojiPickerPopover
		anchor={emojiAnchor}
		onSelect={({ emoji }) => {
			insertEmoji(emoji);
			pickerOpen = false;
		}}
		onClose={() => (pickerOpen = false)}
	/>
{/if}

<script lang="ts">
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
		/**
		 * `small` is the Thread card's reply box: a 44px box with the buttons
		 * inside it at 36px, no hint line, and no padding or rule of its own
		 * (the card gives it both).
		 */
		size?: 'default' | 'small';
	}

	let {
		roster,
		placeholder = 'Drop some trash talk…',
		disabled = false,
		disabledReason = null,
		autofocus = false,
		onSend,
		onEditLast,
		size = 'default'
	}: Props = $props();

	let value = $state('');
	let textarea = $state<HTMLTextAreaElement | null>(null);
	let emojiAnchor = $state<HTMLElement | null>(null);
	let box = $state<HTMLDivElement | null>(null);

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
		const style = getComputedStyle(textarea);
		const line = parseFloat(style.lineHeight) || 24;
		const pad = parseFloat(style.paddingTop) + parseFloat(style.paddingBottom);
		const maxHeight = 8 * line + pad;
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

<div class="composer {size}">
	{#if disabled && disabledReason}
		<p class="disabled-reason">{disabledReason}</p>
	{/if}

	<MentionMenu anchor={box} {matches} activeIndex={mentionIndex} onPick={acceptMention} />

	<div class="box" bind:this={box}>
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
		></textarea>

		<button
			bind:this={emojiAnchor}
			type="button"
			class="icon emoji"
			{disabled}
			aria-label="Insert an emoji"
			title="Insert an emoji"
			onclick={() => (pickerOpen = !pickerOpen)}
		>
			<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"
				><circle cx="12" cy="12" r="9"></circle><path d="M8.5 14.5c1 1.3 2.2 2 3.5 2s2.5-.7 3.5-2"></path><path
					d="M9 9.5h.01M15 9.5h.01"
					stroke-width="2.6"
				></path></svg
			>
		</button>

		<button type="button" class="icon send" disabled={!canSend} onclick={submit} aria-label="Send message">
			<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"
				><path d="M5 12h14M13 6l6 6-6 6"></path></svg
			>
		</button>
	</div>

	{#if size === 'default' || remaining < 200}
		<div class="foot">
			{#if size === 'default'}<span>Enter to send · Shift+Enter for a new line · @ to mention</span>{/if}
			{#if remaining < 200}
				<span class="remaining" class:over={overLimit}>{remaining}</span>
			{/if}
		</div>
	{/if}
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

<style>
	.composer {
		flex: none;
		padding: 16px 20px 18px;
		border-top: 1px solid var(--color-line);
	}

	.composer.small {
		padding: 0;
		border-top: 0;
	}

	.disabled-reason {
		margin: 0 0 10px;
		padding: 8px 12px;
		border-radius: 10px;
		background: var(--color-pennant-tint);
		font-size: 13px;
		color: var(--color-pennant-dark);
	}

	/* The board draws the focused box with a 1.5px field border; the extra
	   half pixel is an inset shadow so the box doesn't shift on focus. */
	.box {
		display: flex;
		gap: 10px;
		align-items: flex-end;
		padding: 8px 8px 8px 14px;
		border: 1px solid var(--color-line-strong);
		border-radius: 12px;
		background: var(--color-card);
	}

	.box:focus-within {
		border-color: var(--color-field);
		box-shadow: inset 0 0 0 0.5px var(--color-field);
	}

	textarea {
		flex: 1;
		min-width: 0;
		display: block;
		box-sizing: border-box;
		resize: none;
		border: 0;
		outline: none;
		padding: 6px 0;
		background: transparent;
		font-family: var(--font-sans);
		font-size: 16px;
		line-height: 1.5;
		color: var(--color-ink);
	}

	/* The forms plugin draws a ring on focus; the box shows focus instead. */
	textarea:focus {
		box-shadow: none;
		outline: none;
	}

	textarea::placeholder {
		color: var(--color-faint);
	}

	textarea:disabled {
		opacity: 0.5;
	}

	.icon {
		flex: none;
		width: 44px;
		height: 44px;
		padding: 0;
		border: 0;
		border-radius: 10px;
		display: inline-flex;
		align-items: center;
		justify-content: center;
		cursor: pointer;
	}

	.icon:focus-visible {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.emoji {
		background: var(--color-ground);
		color: var(--color-ink-2);
	}

	.emoji:hover {
		background: var(--color-wash);
	}

	.send {
		background: var(--color-field);
		color: var(--color-white);
	}

	.icon:disabled {
		cursor: not-allowed;
		opacity: 0.4;
	}

	.foot {
		margin-top: 8px;
		display: flex;
		justify-content: space-between;
		gap: 12px;
		font-size: 13px;
		line-height: 1.4;
		color: var(--color-muted);
	}

	.remaining {
		margin-left: auto;
		font-variant-numeric: tabular-nums;
	}

	.remaining.over {
		color: var(--color-pennant-dark);
	}

	/* The Thread card's reply box: 44px at rest, 10px radius, 15px, with the
	   two buttons inside it at 36px. */
	.small .box {
		align-items: flex-end;
		gap: 6px;
		padding: 3px 3px 3px 14px;
		border-radius: 10px;
	}

	.small textarea {
		font-size: 15px;
		padding: 7px 0;
	}

	.small .icon {
		width: 36px;
		height: 36px;
		border-radius: 8px;
	}

	.small .emoji {
		background: transparent;
	}

	.small .emoji svg {
		width: 20px;
		height: 20px;
	}

	.small .send svg {
		width: 18px;
		height: 18px;
	}

	@media (max-width: 639.98px) {
		.composer {
			padding: 12px 14px 14px;
		}

		textarea {
			min-height: 44px;
			padding: 10px 0;
		}

		.small textarea {
			font-size: 16px;
		}

		/* At 44px the buttons are tap targets; the box grows to hold them. */
		.small .box {
			padding: 0 0 0 14px;
			gap: 2px;
		}

		.small textarea {
			min-height: 44px;
			padding: 10px 0;
		}

		.small .icon {
			width: 44px;
			height: 44px;
		}
	}
</style>

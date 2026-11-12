<script lang="ts">
	import { tick } from 'svelte';
	import { MediaQuery } from 'svelte/reactivity';
	import { clickOutside } from '$lib/actions/clickOutside';
	import Avatar from '../ui/Avatar.svelte';
	import Button from '../ui/Button.svelte';
	import ChatAvatar from './ChatAvatar.svelte';
	import MessageActions from './MessageActions.svelte';
	import ReactionBar from './ReactionBar.svelte';
	import { formatMessageTime, threadChipLabel } from '$lib/chat/format';
	import { renderMarkdown } from '$lib/chat/markdown';
	import { applyMentionMarkup } from '$lib/chat/mentions';
	import type { ChatMessageView, EmojiType, ReactionGroup, RosterMember } from '$lib/chat/types';

	interface Props {
		message: ChatMessageView;
		reactions: ReactionGroup[];
		roster: RosterMember[];
		meManagerKey: number | null;
		canWrite: boolean;
		/** Continuation of the previous message: no avatar, no name, hover-only time. */
		compact?: boolean;
		/** Replies can't themselves be replied to, so they hide the thread affordances. */
		allowThread?: boolean;
		threadExpanded?: boolean;
		/** The newest distinct repliers, shown on the chip while the thread is open. */
		repliers?: { authorKey: number; name: string }[];
		/** `reply` is the Thread card's smaller row: Avatar 32, a 14px name, a rule under it. */
		size?: 'feed' | 'reply';
		onToggleReaction: (messageId: string, emoji: string, emojiType: EmojiType) => void;
		onOpenPicker: (messageId: string, anchor: HTMLElement) => void;
		onOpenThread?: (messageId: string) => void;
		onEdit: (messageId: string, content: string) => void;
		onDelete: (messageId: string) => void;
		onRetry: (messageId: string) => void;
		onDiscard: (messageId: string) => void;
	}

	let {
		message,
		reactions,
		roster,
		meManagerKey,
		canWrite,
		compact = false,
		allowThread = true,
		threadExpanded = false,
		repliers = [],
		size = 'feed',
		onToggleReaction,
		onOpenPicker,
		onOpenThread,
		onEdit,
		onDelete,
		onRetry,
		onDiscard
	}: Props = $props();

	const isAuthor = $derived(message.authorKey === meManagerKey);
	const isDeleted = $derived(message.deletedAt !== null);
	const mentionsMe = $derived(meManagerKey !== null && message.mentions.includes(meManagerKey));
	const isPending = $derived(message.sendState === 'pending');
	const isFailed = $derived(message.sendState === 'failed');
	const isEdited = $derived(message.editedAt !== null);

	// Mention markup goes on *after* sanitizing. Before, and DOMPurify strips the
	// spans we just added; as a string replace, and it highlights @names inside
	// code fences and href attributes.
	const html = $derived(
		isDeleted ? '' : withEditedMark(applyMentionMarkup(renderMarkdown(message.content), roster, meManagerKey))
	);

	// "(edited)" sits at the end of the last line, as the board draws it. When
	// the message ends in a paragraph the mark goes inside it; after a list or a
	// code block it falls to its own line below. The mark is our own constant,
	// added after sanitizing, so nothing from the message rides along with it.
	function withEditedMark(rendered: string): string {
		if (!isEdited) return rendered;
		const mark = ' <span class="edited">(edited)</span>';
		const trimmed = rendered.trimEnd();
		if (trimmed.endsWith('</p>')) return `${trimmed.slice(0, -4)}${mark}</p>`;
		return `${trimmed}<p>${mark.trim()}</p>`;
	}

	const touch = new MediaQuery('hover: none');
	const showActions = $derived(canWrite && !isDeleted && !isPending);

	let editing = $state(false);
	let draft = $state('');
	let textarea = $state<HTMLTextAreaElement | null>(null);
	// A tap opens the toolbar on a touch screen, where there's no hover.
	let actionsOpen = $state(false);

	export function beginEdit() {
		// $state initialisers don't re-run, so the draft is synced explicitly on
		// entry rather than derived from the message.
		draft = message.content;
		editing = true;
		actionsOpen = false;
		queueMicrotask(() => {
			textarea?.focus();
			textarea?.setSelectionRange(draft.length, draft.length);
		});
	}

	function saveEdit() {
		const next = draft.trim();
		editing = false;
		if (next !== '' && next !== message.content) onEdit(message.messageId, next);
	}

	function cancelEdit() {
		editing = false;
		draft = message.content;
	}

	function onEditKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			saveEdit();
		} else if (event.key === 'Escape') {
			event.preventDefault();
			cancelEdit();
		}
	}

	function onRowTap(event: MouseEvent) {
		if (!touch.current || !showActions || editing) return;
		// A tap on a link, a button or a field does its own thing.
		if ((event.target as HTMLElement).closest('a, button, textarea, input, [role="menu"]')) return;
		actionsOpen = !actionsOpen;
		// The toolbar opens under the text, so on the last row it can land
		// below the fold of the feed.
		if (actionsOpen) {
			void tick().then(() => actionsEl?.scrollIntoView({ block: 'nearest', behavior: 'smooth' }));
		}
	}

	let actionsEl = $state<HTMLDivElement | null>(null);
</script>

<!-- The click handler is only the touch-screen way to open the toolbar; with a
     keyboard the toolbar opens on focus-within, so the row needs no key handler. -->
<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
<div
	class="row {size}"
	class:compact
	class:mention={mentionsMe && !isFailed}
	class:failed={isFailed}
	class:pending={isPending}
	class:thread-open={threadExpanded}
	class:actionable={showActions && !editing}
	class:actions-open={actionsOpen}
	onclick={onRowTap}
	use:clickOutside={{ onOutside: () => (actionsOpen = false) }}
>
	{#if compact}
		<!-- Reserve the avatar gutter so text stays aligned, and reveal the
		     timestamp there on hover, the way Slack does. -->
		<div class="gutter">
			<span class="gutter-time">{formatMessageTime(message.createdAt)}</span>
		</div>
	{:else}
		<ChatAvatar
			managerName={message.authorName}
			displayName={message.authorDisplayName}
			profileImageUrl={message.authorProfileImage}
			size={size === 'reply' ? 32 : 40}
			phoneSize={32}
		/>
	{/if}

	<div class="body">
		{#if !compact}
			<div class="meta">
				<span class="name">{message.authorDisplayName}</span>
				<span class="time">{formatMessageTime(message.createdAt)}</span>
			</div>
		{/if}

		{#if isDeleted}
			<p class="deleted">This message was deleted.</p>
		{:else if editing}
			<div class="edit">
				<textarea
					bind:this={textarea}
					bind:value={draft}
					onkeydown={onEditKeydown}
					rows="2"
					aria-label="Edit message"
				></textarea>
				<div class="edit-actions">
					<Button size="sm" onclick={saveEdit}>Save</Button>
					<Button variant="link" onclick={cancelEdit}>Cancel</Button>
					<span class="hint">Enter to save, Escape to cancel</span>
				</div>
			</div>
		{:else}
			<div class="chat-markdown">
				<!-- Sanitized by DOMPurify in renderMarkdown; see lib/chat/markdown.ts -->
				{@html html}
			</div>
		{/if}

		{#if !isDeleted}
			<ReactionBar
				groups={reactions}
				{meManagerKey}
				{canWrite}
				onToggle={(emoji, emojiType) => onToggleReaction(message.messageId, emoji, emojiType)}
				onOpenPicker={(anchor) => onOpenPicker(message.messageId, anchor)}
			/>
		{/if}

		{#if isFailed}
			<div class="failure">
				<span>{message.failureReason ?? 'Failed to send.'}</span>
				<Button variant="link" onclick={() => onRetry(message.messageId)}>Retry</Button>
				<Button variant="link" onclick={() => onDiscard(message.messageId)}>Discard</Button>
			</div>
		{/if}

		{#if allowThread && message.replyCount > 0 && onOpenThread}
			<button
				type="button"
				class="chip-hit"
				data-thread-chip={message.messageId}
				aria-expanded={threadExpanded}
				onclick={() => onOpenThread(message.messageId)}
			>
				<span class="chip">
					{#if threadExpanded && repliers.length > 0}
						<span class="repliers" aria-hidden="true">
							{#each repliers as replier, i (replier.authorKey)}
								<Avatar
									name={replier.name}
									size={20}
									alt=""
									ring={i > 0 ? 2 : 0}
									ringColor="var(--color-field-tint)"
									class={i > 0 ? 'overlap' : ''}
								/>
							{/each}
						</span>
					{/if}
					{threadChipLabel(message.replyCount, threadExpanded, message.lastReplyAt)}
				</span>
			</button>
		{/if}

		{#if showActions && !editing}
			<div class="actions" bind:this={actionsEl}>
				<MessageActions
					{isAuthor}
					canReply={allowThread && onOpenThread !== undefined}
					onReact={(emoji, emojiType) => {
						actionsOpen = false;
						onToggleReaction(message.messageId, emoji, emojiType);
					}}
					onOpenPicker={(anchor) => onOpenPicker(message.messageId, anchor)}
					onReply={() => {
						actionsOpen = false;
						onOpenThread?.(message.messageId);
					}}
					onEdit={beginEdit}
					onDelete={() => onDelete(message.messageId)}
				/>
			</div>
		{/if}
	</div>
</div>

<style>
	.row {
		position: relative;
		display: flex;
		gap: 12px;
		padding: 10px 8px;
		margin: 0 -8px;
		border-radius: 10px;
	}

	.row.compact {
		padding: 2px 8px 8px;
	}

	/* The toolbar row and an open thread's root take the ground fill. */
	.row.thread-open,
	.row.actionable:hover,
	.row.actionable:focus-within,
	.row.actions-open {
		background: var(--color-ground);
	}

	/* A mention of you keeps its own fill, and a failed send is drawn the same
	   way: the mention fill with a 3px pennant rule on its left. */
	.row.mention,
	.row.failed,
	.row.mention.actionable:hover,
	.row.mention.actionable:focus-within {
		background: var(--color-mention);
		box-shadow: inset 3px 0 0 var(--color-pennant);
	}

	.row.failed {
		box-shadow: inset 3px 0 0 var(--color-pennant);
	}

	.row.pending {
		opacity: 0.6;
	}

	.row.reply {
		gap: 10px;
		padding: 10px 0;
		margin: 0;
		border-radius: 0;
		border-bottom: 1px solid var(--color-line-soft);
	}

	.row.reply.thread-open,
	.row.reply.actionable:hover,
	.row.reply.actionable:focus-within,
	.row.reply.actions-open {
		background: transparent;
	}

	/* The time is a little wider than the gutter ("08:10 AM" at 12px), so it
	   stays on one line and runs into the row's left padding instead of
	   wrapping. */
	.gutter {
		width: 40px;
		flex: none;
		display: flex;
		justify-content: flex-end;
		height: 22.5px;
	}

	.gutter-time {
		white-space: nowrap;
		font-size: 12px;
		line-height: 22.5px;
		color: var(--color-muted);
		font-variant-numeric: tabular-nums;
		opacity: 0;
	}

	.row:hover .gutter-time,
	.row:focus-within .gutter-time {
		opacity: 1;
	}

	.body {
		flex: 1;
		min-width: 0;
	}

	.meta {
		display: flex;
		align-items: baseline;
		gap: 10px;
	}

	.name {
		font-size: 15px;
		font-weight: 700;
		color: var(--color-ink);
	}

	.reply .name {
		font-size: 14px;
	}

	.time {
		font-size: 13px;
		color: var(--color-muted);
		font-variant-numeric: tabular-nums;
	}

	.deleted {
		margin: 4px 0 0;
		font-size: 14px;
		font-style: italic;
		color: var(--color-muted);
	}

	.compact .deleted,
	.compact .chat-markdown {
		margin-top: 0;
	}

	.chat-markdown {
		margin-top: 4px;
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-ink);
		overflow-wrap: anywhere;
	}

	/* Scoped to the rendered markdown, which arrives as {@html} and so is outside
	   Svelte's per-component class scoping — :global is required here. */
	.chat-markdown :global(p) {
		margin: 0;
	}
	.chat-markdown :global(p + p) {
		margin-top: 6px;
	}
	.chat-markdown :global(a) {
		color: var(--color-field);
		text-decoration: underline;
		text-underline-offset: 2px;
	}
	.chat-markdown :global(a:hover) {
		color: var(--color-clubhouse);
	}
	.chat-markdown :global(code) {
		border-radius: var(--radius-tag);
		background: var(--color-ground);
		padding: 1px 4px;
		font-size: 13px;
	}
	.chat-markdown :global(pre) {
		margin: 6px 0;
		overflow-x: auto;
		border-radius: var(--radius-control);
		background: var(--color-ground);
		padding: 8px 12px;
	}
	.chat-markdown :global(pre code) {
		background: none;
		padding: 0;
	}
	.chat-markdown :global(blockquote) {
		margin: 4px 0;
		border-left: 2px solid var(--color-line-strong);
		padding-left: 10px;
		color: var(--color-ink-2);
	}
	.chat-markdown :global(ul),
	.chat-markdown :global(ol) {
		margin: 4px 0;
		padding-left: 20px;
	}
	.chat-markdown :global(ul) {
		list-style: disc;
	}
	.chat-markdown :global(ol) {
		list-style: decimal;
	}
	.chat-markdown :global(.mention) {
		font-weight: 700;
		color: var(--color-field);
	}
	.chat-markdown :global(.mention-me) {
		padding: 1px 6px;
		border-radius: var(--radius-tag);
		background: var(--color-pennant-tint);
		color: var(--color-pennant-dark);
		font-size: 13px;
	}
	.chat-markdown :global(.edited) {
		font-size: 12px;
		color: var(--color-muted);
	}

	.edit {
		margin-top: 4px;
	}

	.edit textarea {
		box-sizing: border-box;
		display: block;
		width: 100%;
		resize: none;
		padding: 8px 12px;
		border: 1px solid var(--color-line-strong);
		border-radius: var(--radius-control);
		background: var(--color-card);
		font-family: var(--font-sans);
		font-size: 15px;
		line-height: 1.5;
		color: var(--color-ink);
	}

	.edit textarea:focus {
		outline: none;
		box-shadow: none;
		border-color: var(--color-field);
	}

	.edit-actions {
		margin-top: 8px;
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		gap: 8px 14px;
	}

	.hint {
		font-size: 13px;
		color: var(--color-muted);
	}

	.failure {
		margin-top: 6px;
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		gap: 4px 12px;
		font-size: 13px;
		color: var(--color-pennant-dark);
	}

	.failure :global(.btn) {
		font-size: 13px;
	}

	.chip-hit {
		margin-top: 10px;
		display: inline-flex;
		align-items: center;
		padding: 0;
		border: 0;
		background: transparent;
		font-family: var(--font-sans);
		cursor: pointer;
	}

	.chip {
		display: inline-flex;
		align-items: center;
		gap: 8px;
		padding: 5px 10px;
		border-radius: 8px;
		background: var(--color-field-tint);
		color: var(--color-clubhouse);
		font-size: 13px;
		font-weight: 700;
	}

	.chip-hit:hover .chip {
		background: var(--color-field-line);
	}

	.chip-hit:focus-visible {
		outline: none;
	}

	.chip-hit:focus-visible .chip {
		outline: 2px solid var(--color-field);
		outline-offset: 2px;
	}

	.repliers {
		display: inline-flex;
		align-items: center;
	}

	/* The second avatar overlaps the first by 12px; its 2px ring is outside its
	   20px, so the overlap counts from its outer edge. */
	.repliers :global(.overlap) {
		margin: -2px 0 -2px -12px;
	}

	/* Floats 16px above the row's top edge, 10px from its right, while the row
	   is hovered or holds focus. */
	.actions {
		position: absolute;
		top: -16px;
		right: 10px;
		z-index: 3;
		display: none;
	}

	.row:hover > .body > .actions,
	.row:focus-within > .body > .actions {
		display: block;
	}

	/* On a touch screen a tap opens it inside the row instead, 8px under the
	   text and right aligned, so it never covers the row above. */
	@media (hover: none) {
		.actions,
		.row:hover > .body > .actions,
		.row:focus-within > .body > .actions {
			position: static;
			display: none;
			margin-top: 8px;
			justify-content: flex-end;
		}

		.row.actions-open > .body > .actions {
			display: flex;
		}

		.row.actionable:hover:not(.actions-open):not(.thread-open):not(.mention),
		.row.actionable:focus-within:not(.actions-open):not(.thread-open):not(.mention) {
			background: transparent;
		}

		.chip-hit {
			margin-top: 3px;
			min-height: 44px;
		}
	}

	@media (max-width: 639.98px) {
		.row {
			gap: 10px;
		}

		.gutter {
			width: 32px;
		}
	}
</style>

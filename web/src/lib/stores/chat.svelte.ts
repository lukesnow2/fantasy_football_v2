import { SvelteMap } from 'svelte/reactivity';
import { nanoid } from 'nanoid';
import {
	ACTIVE_WINDOW_MS,
	FAILURES_BEFORE_RECONNECTING,
	IDLE_WINDOW_MS,
	PAGE_SIZE,
	PENDING_REACTION_TTL_MS,
	POLL_ACTIVE_MS,
	POLL_AFTER_MUTATION_MS,
	POLL_DORMANT_MS,
	POLL_ERROR_BASE_MS,
	POLL_ERROR_MAX_MS,
	POLL_IDLE_MS,
	READ_DEBOUNCE_MS
} from '$lib/chat/constants';
import type {
	ChatBootstrapResponse,
	ChatCursor,
	ChatDeltaResponse,
	ChatMessageDTO,
	ChatMessageView,
	EmojiType,
	ReactionGroup,
	RosterMember
} from '$lib/chat/types';

export type ConnectionState =
	| 'idle'
	| 'connecting'
	| 'live'
	| 'reconnecting'
	| 'offline'
	| 'unauthorized';

export interface ChatStoreOptions {
	channelId: string;
	/** Null when signed out or not roster-linked. Disables all network activity. */
	meManagerKey: number | null;
	/** Injected for tests. */
	fetchImpl?: typeof fetch;
	/** Injected for tests. */
	now?: () => number;
}

type PendingKey = `${string}:${string}`;

interface PendingReaction {
	desired: boolean;
	/** When the toggle's own response landed. Null while still in flight. */
	resolvedAt: number | null;
	createdAt: number;
}

function toView(dto: ChatMessageDTO): ChatMessageView {
	return { ...dto, sendState: 'sent' };
}

/**
 * All chat client state.
 *
 * A class rather than a module of exported `$state`: rune state cannot be
 * exported as a reassignable binding, and an instance per panel means a second
 * channel — or a sign-in mid-session — is a new store rather than a singleton
 * that has to be reset correctly.
 */
export class ChatStore {
	readonly channelId: string;
	readonly meManagerKey: number | null;

	#fetch: typeof fetch;
	#now: () => number;

	// Identity is `messageId`, a string the client mints before the server has
	// ever seen the message. That is what lets the optimistic row and the row
	// that later arrives from a poll be the same row.
	#byId = new SvelteMap<string, ChatMessageView>();
	#order = $state<string[]>([]);

	#replies = new SvelteMap<string, ChatMessageView[]>();
	#threadLoading = new SvelteMap<string, boolean>();

	// Optimistic reactions live in an overlay rather than being written into the
	// message, because delta polls ship *complete* reaction state and would
	// otherwise revert a toggle that is still in flight.
	#pendingReactions = new SvelteMap<PendingKey, PendingReaction>();

	#roster = $state<RosterMember[]>([]);
	#connectionState = $state<ConnectionState>('idle');
	#initialLoadDone = $state(false);
	#loadError = $state<string | null>(null);
	#hasMoreHistory = $state(false);
	#loadingOlder = $state(false);
	#unreadBoundaryKey = $state<number | null>(null);
	#newMessageCount = $state(0);
	#atBottom = $state(true);
	#serverClockOffsetMs = $state(0);

	#cursor: ChatCursor = { messageKey: 0, ts: 0 };
	#lastActivityAt = 0;
	#consecutiveFailures = 0;
	#pollTimer: ReturnType<typeof setTimeout> | null = null;
	#pollController: AbortController | null = null;
	#readTimer: ReturnType<typeof setTimeout> | null = null;
	#lastSentReadKey = 0;
	// `running`, not `disposed`. An `$effect` can re-run and re-invoke start()
	// on the same instance; a one-way disposed flag left the store permanently
	// dead in that case, showing skeletons forever.
	#running = false;
	#teardown: (() => void)[] = [];

	constructor(opts: ChatStoreOptions) {
		this.channelId = opts.channelId;
		this.meManagerKey = opts.meManagerKey;
		this.#fetch = opts.fetchImpl ?? ((...args) => globalThis.fetch(...args));
		this.#now = opts.now ?? (() => Date.now());
	}

	// ---- reads ---------------------------------------------------------------

	get messages(): ChatMessageView[] {
		const out: ChatMessageView[] = [];
		for (const id of this.#order) {
			const message = this.#byId.get(id);
			if (message) out.push(message);
		}
		return out;
	}

	get roster(): RosterMember[] {
		return this.#roster;
	}

	get connectionState(): ConnectionState {
		return this.#connectionState;
	}

	get initialLoadDone(): boolean {
		return this.#initialLoadDone;
	}

	get loadError(): string | null {
		return this.#loadError;
	}

	get hasMoreHistory(): boolean {
		return this.#hasMoreHistory;
	}

	get loadingOlder(): boolean {
		return this.#loadingOlder;
	}

	get unreadBoundaryKey(): number | null {
		return this.#unreadBoundaryKey;
	}

	get newMessageCount(): number {
		return this.#newMessageCount;
	}

	get atBottom(): boolean {
		return this.#atBottom;
	}

	get serverClockOffsetMs(): number {
		return this.#serverClockOffsetMs;
	}

	/** Server truth with any in-flight local toggle applied on top. */
	reactionsFor(messageId: string): ReactionGroup[] {
		const message = this.#byId.get(messageId);
		if (!message) return [];

		const base = message.reactions;
		const overlay: ReactionGroup[] = base.map((group) => ({ ...group }));
		let changed = false;

		for (const [key, pending] of this.#pendingReactions) {
			const [pendingId, emoji] = this.#splitPendingKey(key);
			if (pendingId !== messageId) continue;

			const index = overlay.findIndex((group) => group.emoji === emoji);
			if (pending.desired) {
				if (index === -1) {
					overlay.push({
						emoji,
						emojiType: 'unicode',
						count: 1,
						mine: true,
						users: [{ managerKey: this.meManagerKey ?? 0, name: 'You' }]
					});
					changed = true;
				} else if (!overlay[index].mine) {
					overlay[index] = {
						...overlay[index],
						count: overlay[index].count + 1,
						mine: true,
						users: [...overlay[index].users, { managerKey: this.meManagerKey ?? 0, name: 'You' }]
					};
					changed = true;
				}
			} else if (index !== -1 && overlay[index].mine) {
				const next = {
					...overlay[index],
					count: overlay[index].count - 1,
					mine: false,
					users: overlay[index].users.filter((u) => u.managerKey !== this.meManagerKey)
				};
				if (next.count <= 0) overlay.splice(index, 1);
				else overlay[index] = next;
				changed = true;
			}
		}

		return changed ? overlay : base;
	}

	repliesFor(messageId: string): ChatMessageView[] {
		return this.#replies.get(messageId) ?? [];
	}

	isThreadLoading(messageId: string): boolean {
		return this.#threadLoading.get(messageId) === true;
	}

	// ---- lifecycle -----------------------------------------------------------

	/** Bootstrap then poll. Returns its own teardown, for `$effect`. Re-entrant. */
	start(): () => void {
		if (this.#running) return () => this.stop();
		this.#running = true;

		// Signed out: render the locked panel and make zero requests. The old panel
		// fetched anyway, swallowed the 401 behind `if (response.ok)`, and showed a
		// silently empty box.
		if (this.meManagerKey === null) {
			this.#connectionState = 'unauthorized';
			this.#initialLoadDone = true;
			return () => this.stop();
		}

		if (typeof document !== 'undefined') {
			const onVisibility = () => {
				if (document.visibilityState === 'visible') {
					this.#lastActivityAt = this.#now();
					void this.refresh();
				} else {
					this.#clearPollTimer();
				}
			};
			document.addEventListener('visibilitychange', onVisibility);
			this.#teardown.push(() => document.removeEventListener('visibilitychange', onVisibility));
		}

		if (typeof window !== 'undefined') {
			const onOnline = () => void this.refresh();
			const onOffline = () => {
				this.#clearPollTimer();
				this.#connectionState = 'offline';
			};
			const onPageHide = () => this.#flushRead(true);
			window.addEventListener('online', onOnline);
			window.addEventListener('offline', onOffline);
			window.addEventListener('pagehide', onPageHide);
			this.#teardown.push(() => {
				window.removeEventListener('online', onOnline);
				window.removeEventListener('offline', onOffline);
				window.removeEventListener('pagehide', onPageHide);
			});
		}

		// Already loaded once (a re-entry after a stop): resume polling rather than
		// re-fetching the whole channel.
		if (this.#initialLoadDone && this.#loadError === null) this.#schedulePoll(this.#nextDelay());
		else void this.#bootstrap();

		return () => this.stop();
	}

	stop(): void {
		this.#running = false;
		this.#clearPollTimer();
		this.#pollController?.abort();
		this.#pollController = null;
		if (this.#readTimer !== null) {
			clearTimeout(this.#readTimer);
			this.#readTimer = null;
		}
		for (const fn of this.#teardown) fn();
		this.#teardown = [];
	}

	/** Poll immediately. Used by the retry button, visibility changes and tests. */
	async refresh(): Promise<void> {
		if (this.meManagerKey === null || !this.#running) return;
		if (!this.#initialLoadDone) return void (await this.#bootstrap());
		this.#clearPollTimer();
		await this.#poll();
	}

	// ---- mutations -----------------------------------------------------------

	async send(content: string, opts: { parentMessageKey?: number } = {}): Promise<void> {
		if (this.meManagerKey === null) return;
		const trimmed = content.trim();
		if (trimmed === '') return;

		const messageId = `m_${nanoid()}`;
		const parentMessageKey = opts.parentMessageKey ?? null;
		const optimistic = this.#makeOptimistic(messageId, trimmed, parentMessageKey);

		if (parentMessageKey === null) {
			this.#byId.set(messageId, optimistic);
			this.#order.push(messageId);
			this.#resort();
		} else {
			const parentId = this.#idForKey(parentMessageKey);
			if (parentId) {
				this.#replies.set(parentId, [...(this.#replies.get(parentId) ?? []), optimistic]);
			}
		}

		this.#markActivity();
		await this.#postMessage(messageId, trimmed, parentMessageKey);
	}

	/** Re-POST a failed message under its original id, so a partial send can't duplicate. */
	async retry(messageId: string): Promise<void> {
		const existing = this.#findAnywhere(messageId);
		if (!existing || existing.sendState !== 'failed') return;

		this.#patch(messageId, { sendState: 'pending', failureReason: undefined });
		await this.#postMessage(messageId, existing.content, existing.parentMessageKey);
	}

	discard(messageId: string): void {
		const existing = this.#findAnywhere(messageId);
		if (!existing || existing.sendState !== 'failed') return;

		if (this.#byId.has(messageId)) {
			this.#byId.delete(messageId);
			this.#order = this.#order.filter((id) => id !== messageId);
		}
		for (const [parentId, list] of this.#replies) {
			if (list.some((m) => m.messageId === messageId)) {
				this.#replies.set(
					parentId,
					list.filter((m) => m.messageId !== messageId)
				);
			}
		}
	}

	async edit(messageId: string, content: string): Promise<void> {
		if (this.meManagerKey === null) return;
		const trimmed = content.trim();
		if (trimmed === '') return;

		const previous = this.#findAnywhere(messageId);
		if (!previous || previous.content === trimmed) return;

		this.#patch(messageId, { content: trimmed, editedAt: this.#now() });
		this.#markActivity();

		try {
			const response = await this.#fetch('/api/chat/messages', {
				method: 'PATCH',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ messageId, content: trimmed, channelId: this.channelId })
			});
			if (!response.ok) throw new Error(await this.#errorText(response));
			const { message } = (await response.json()) as { message: ChatMessageDTO };
			this.#applyMessages([message]);
		} catch {
			// Put the original text back rather than leaving a local-only edit that
			// looks saved and isn't.
			this.#patch(messageId, { content: previous.content, editedAt: previous.editedAt });
		}
	}

	async remove(messageId: string): Promise<void> {
		if (this.meManagerKey === null) return;
		const previous = this.#findAnywhere(messageId);
		if (!previous) return;

		this.#patch(messageId, { deletedAt: this.#now() });
		this.#markActivity();

		try {
			const response = await this.#fetch('/api/chat/messages', {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ messageId, channelId: this.channelId })
			});
			if (!response.ok) throw new Error(await this.#errorText(response));
			const { message } = (await response.json()) as { message: ChatMessageDTO };
			this.#applyMessages([message]);
		} catch {
			this.#patch(messageId, { deletedAt: previous.deletedAt });
		}
	}

	async toggleReaction(
		messageId: string,
		emoji: string,
		emojiType: EmojiType = 'unicode'
	): Promise<void> {
		if (this.meManagerKey === null) return;

		const key = this.#pendingKey(messageId, emoji);
		const current = this.reactionsFor(messageId).find((group) => group.emoji === emoji);
		const desired = !(current?.mine ?? false);

		this.#pendingReactions.set(key, { desired, resolvedAt: null, createdAt: this.#now() });
		this.#markActivity();

		try {
			const response = await this.#fetch('/api/chat/reactions', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ messageId, emoji, emojiType, channelId: this.channelId })
			});
			if (!response.ok) throw new Error(await this.#errorText(response));

			const result = (await response.json()) as { reactions: ReactionGroup[] };
			// Authoritative for this message right now, so the common case never
			// waits for the next poll.
			this.#patch(messageId, { reactions: result.reactions });

			const pending = this.#pendingReactions.get(key);
			if (pending) this.#pendingReactions.set(key, { ...pending, resolvedAt: this.#now() });
		} catch {
			this.#pendingReactions.delete(key);
		}
	}

	async loadThread(messageId: string): Promise<void> {
		if (this.meManagerKey === null) return;
		const message = this.#byId.get(messageId);
		if (!message?.messageKey) return;

		this.#threadLoading.set(messageId, true);
		try {
			const response = await this.#fetch(
				`/api/chat/messages?channelId=${encodeURIComponent(this.channelId)}&parent=${message.messageKey}`
			);
			if (!response.ok) return;
			const data = (await response.json()) as { messages: ChatMessageDTO[] };

			// Keep any pending local reply that the server has not confirmed yet,
			// so expanding a thread does not make an in-flight message vanish.
			const pendingLocal = (this.#replies.get(messageId) ?? []).filter(
				(m) => m.sendState !== 'sent' && !data.messages.some((d) => d.messageId === m.messageId)
			);
			this.#replies.set(messageId, [...data.messages.map(toView), ...pendingLocal]);
		} finally {
			this.#threadLoading.set(messageId, false);
		}
	}

	async loadOlder(): Promise<void> {
		if (this.meManagerKey === null || !this.#hasMoreHistory || this.#loadingOlder) return;

		const oldest = this.messages.find((m) => m.messageKey !== null);
		if (!oldest?.messageKey) return;

		this.#loadingOlder = true;
		try {
			const response = await this.#fetch(
				`/api/chat/messages?channelId=${encodeURIComponent(this.channelId)}&before=${oldest.messageKey}&limit=${PAGE_SIZE}`
			);
			if (!response.ok) return;
			const data = (await response.json()) as { messages: ChatMessageDTO[]; hasMore: boolean };
			this.#applyMessages(data.messages);
			this.#hasMoreHistory = data.hasMore;
		} finally {
			this.#loadingOlder = false;
		}
	}

	// ---- read state ----------------------------------------------------------

	markRead(messageKey: number): void {
		if (this.meManagerKey === null || messageKey <= this.#lastSentReadKey) return;
		if (typeof document !== 'undefined' && document.visibilityState !== 'visible') return;
		if (!this.#atBottom) return;

		this.#lastSentReadKey = messageKey;
		if (this.#readTimer !== null) clearTimeout(this.#readTimer);
		this.#readTimer = setTimeout(() => this.#flushRead(false), READ_DEBOUNCE_MS);
	}

	setAtBottom(atBottom: boolean): void {
		this.#atBottom = atBottom;
		if (atBottom) this.#newMessageCount = 0;
	}

	clearNewMessageCount(): void {
		this.#newMessageCount = 0;
	}

	// ---- internals -----------------------------------------------------------

	#pendingKey(messageId: string, emoji: string): PendingKey {
		return `${messageId} ${emoji}` as PendingKey;
	}

	#splitPendingKey(key: PendingKey): [string, string] {
		const index = key.indexOf(' ');
		return [key.slice(0, index), key.slice(index + 1)];
	}

	#makeOptimistic(
		messageId: string,
		content: string,
		parentMessageKey: number | null
	): ChatMessageView {
		const me = this.#roster.find((r) => r.managerKey === this.meManagerKey);
		const now = this.#now();
		return {
			messageKey: null,
			messageId,
			content,
			channelId: this.channelId,
			parentMessageKey,
			messageType: 'message',
			authorKey: this.meManagerKey ?? 0,
			authorName: me?.name ?? null,
			authorDisplayName: me?.displayName ?? 'You',
			authorProfileImage: me?.profileImageUrl ?? null,
			mentions: [],
			createdAt: now,
			updatedAt: now,
			editedAt: null,
			deletedAt: null,
			reactions: [],
			replyCount: 0,
			lastReplyAt: null,
			sendState: 'pending'
		};
	}

	async #postMessage(
		messageId: string,
		content: string,
		parentMessageKey: number | null
	): Promise<void> {
		try {
			const response = await this.#fetch('/api/chat/messages', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					messageId,
					content,
					channelId: this.channelId,
					parentMessageKey
				})
			});

			if (!response.ok) throw new Error(await this.#errorText(response));

			const { message } = (await response.json()) as { message: ChatMessageDTO };
			if (parentMessageKey === null) {
				this.#applyMessages([message]);
			} else {
				this.#applyReply(parentMessageKey, message);
			}
			this.#schedulePoll(POLL_AFTER_MUTATION_MS);
		} catch (err) {
			this.#patch(messageId, {
				sendState: 'failed',
				failureReason: err instanceof Error ? err.message : 'Could not send.'
			});
		}
	}

	#applyReply(parentMessageKey: number, dto: ChatMessageDTO): void {
		const parentId = this.#idForKey(parentMessageKey);
		if (!parentId) return;
		const list = this.#replies.get(parentId) ?? [];
		const index = list.findIndex((m) => m.messageId === dto.messageId);
		const next = [...list];
		if (index === -1) next.push(toView(dto));
		else next[index] = { ...next[index], ...dto, sendState: 'sent', failureReason: undefined };
		this.#replies.set(parentId, next);
	}

	#idForKey(messageKey: number): string | null {
		for (const [id, message] of this.#byId) {
			if (message.messageKey === messageKey) return id;
		}
		return null;
	}

	#findAnywhere(messageId: string): ChatMessageView | null {
		const root = this.#byId.get(messageId);
		if (root) return root;
		for (const list of this.#replies.values()) {
			const found = list.find((m) => m.messageId === messageId);
			if (found) return found;
		}
		return null;
	}

	/** Update one message wherever it lives — root list or a thread. */
	#patch(messageId: string, changes: Partial<ChatMessageView>): void {
		const root = this.#byId.get(messageId);
		if (root) {
			this.#byId.set(messageId, { ...root, ...changes });
			return;
		}
		for (const [parentId, list] of this.#replies) {
			const index = list.findIndex((m) => m.messageId === messageId);
			if (index === -1) continue;
			const next = [...list];
			next[index] = { ...next[index], ...changes };
			this.#replies.set(parentId, next);
			return;
		}
	}

	/**
	 * Merge server messages into local state.
	 *
	 * Keyed on `messageId`, and the server always wins on every field. That single
	 * rule is what converts a pending optimistic row into a confirmed one, lands
	 * edits and tombstones, and makes the sequence "optimistic insert -> POST
	 * response -> the same row again from a poll" idempotent instead of duplicating.
	 */
	#applyMessages(incoming: ChatMessageDTO[]): void {
		if (incoming.length === 0) return;

		let inserted = false;
		let remoteArrivals = 0;

		for (const dto of incoming) {
			const existing = this.#byId.get(dto.messageId);
			if (existing) {
				this.#byId.set(dto.messageId, {
					...existing,
					...dto,
					sendState: 'sent',
					failureReason: undefined
				});
			} else {
				this.#byId.set(dto.messageId, toView(dto));
				this.#order.push(dto.messageId);
				inserted = true;
				if (dto.authorKey !== this.meManagerKey) remoteArrivals++;
			}
		}

		if (inserted) {
			this.#resort();
			if (!this.#atBottom && remoteArrivals > 0) {
				this.#newMessageCount += remoteArrivals;
			}
		}
	}

	/**
	 * Confirmed messages sort by messageKey — monotonic, and exactly the server's
	 * own order. Pending ones have no key yet, so they sit after everything
	 * confirmed, ordered by when they were typed.
	 */
	#resort(): void {
		const rank = (id: string): [number, number] => {
			const message = this.#byId.get(id);
			if (!message) return [2, 0];
			return message.messageKey === null ? [1, message.createdAt] : [0, message.messageKey];
		};

		this.#order = [...this.#order].sort((a, b) => {
			const [groupA, valueA] = rank(a);
			const [groupB, valueB] = rank(b);
			return groupA - groupB || valueA - valueB;
		});
	}

	#markActivity(): void {
		this.#lastActivityAt = this.#now();
		this.#schedulePoll(POLL_AFTER_MUTATION_MS);
	}

	async #bootstrap(): Promise<void> {
		this.#connectionState = 'connecting';
		this.#loadError = null;

		try {
			const response = await this.#fetch(
				`/api/chat/messages?channelId=${encodeURIComponent(this.channelId)}&limit=${PAGE_SIZE}`
			);

			if (response.status === 401 || response.status === 403) {
				this.#connectionState = 'unauthorized';
				this.#initialLoadDone = true;
				return;
			}
			if (!response.ok) throw new Error(await this.#errorText(response));

			const data = (await response.json()) as ChatBootstrapResponse;

			this.#roster = data.roster;
			this.#cursor = data.cursor;
			this.#serverClockOffsetMs = data.serverTime - this.#now();
			this.#hasMoreHistory = data.hasMore;
			// Frozen at bootstrap. Recomputing it would make the divider chase the
			// reader down the list, which is the opposite of what it is for.
			this.#unreadBoundaryKey = data.lastReadMessageKey;
			this.#lastSentReadKey = data.lastReadMessageKey ?? 0;

			this.#applyMessages(data.messages);
			this.#initialLoadDone = true;
			this.#connectionState = 'live';
			this.#consecutiveFailures = 0;
			this.#lastActivityAt = this.#now();
			this.#schedulePoll(this.#nextDelay());
		} catch (err) {
			this.#initialLoadDone = true;
			this.#loadError = err instanceof Error ? err.message : 'Could not load chat.';
			this.#connectionState = 'reconnecting';
			this.#consecutiveFailures++;
			this.#schedulePoll(this.#backoffDelay());
		}
	}

	async #poll(): Promise<void> {
		if (!this.#running || this.meManagerKey === null) return;
		if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return;

		this.#pollController?.abort();
		const controller = new AbortController();
		this.#pollController = controller;

		// Captured *before* the request goes out. Clearing a pending reaction is
		// only safe once a poll that started after the toggle resolved has landed —
		// a poll already in flight carries pre-toggle data and would flicker the
		// pill back.
		const pollStartedAt = this.#now();

		try {
			const params = new URLSearchParams({
				channelId: this.channelId,
				since: String(this.#cursor.messageKey),
				sinceTs: String(this.#cursor.ts),
				window: String(PAGE_SIZE)
			});
			const response = await this.#fetch(`/api/chat/messages?${params}`, {
				signal: controller.signal
			});

			if (response.status === 401 || response.status === 403) {
				this.#connectionState = 'unauthorized';
				return;
			}
			if (!response.ok) throw new Error(await this.#errorText(response));

			const data = (await response.json()) as ChatDeltaResponse;

			this.#applyMessages(data.messages);
			this.#applyReactionState(data.reactions);
			// Always the server's own value. A client clock running fast would
			// permanently skip edits if we computed this locally.
			this.#cursor = data.cursor;
			this.#clearResolvedPendings(pollStartedAt);

			if (data.messages.length > 0) this.#lastActivityAt = this.#now();

			this.#consecutiveFailures = 0;
			this.#connectionState = 'live';
			this.#schedulePoll(this.#nextDelay());
		} catch (err) {
			if ((err as Error)?.name === 'AbortError') return;
			this.#consecutiveFailures++;
			if (this.#consecutiveFailures >= FAILURES_BEFORE_RECONNECTING) {
				this.#connectionState = 'reconnecting';
			}
			this.#schedulePoll(this.#backoffDelay());
		} finally {
			if (this.#pollController === controller) this.#pollController = null;
		}
	}

	#applyReactionState(reactions: Record<string, ReactionGroup[]>): void {
		for (const [key, groups] of Object.entries(reactions)) {
			const messageKey = Number(key);
			const id = this.#idForKey(messageKey);
			if (!id) continue;
			const message = this.#byId.get(id);
			if (message) this.#byId.set(id, { ...message, reactions: groups });
		}
	}

	#clearResolvedPendings(pollStartedAt: number): void {
		const now = this.#now();
		for (const [key, pending] of this.#pendingReactions) {
			const settledBeforeThisPoll =
				pending.resolvedAt !== null && pending.resolvedAt <= pollStartedAt;
			// Safety valve: a toggle whose confirming poll never arrives must not
			// pin the overlay open forever.
			const stale = now - pending.createdAt > PENDING_REACTION_TTL_MS;
			if (settledBeforeThisPoll || stale) this.#pendingReactions.delete(key);
		}
	}

	/** Quiet channels poll slowly; a live conversation polls fast. */
	#nextDelay(): number {
		const quietFor = this.#now() - this.#lastActivityAt;
		if (quietFor < ACTIVE_WINDOW_MS) return POLL_ACTIVE_MS;
		if (quietFor < IDLE_WINDOW_MS) return POLL_IDLE_MS;
		return POLL_DORMANT_MS;
	}

	#backoffDelay(): number {
		const base = Math.min(
			POLL_ERROR_MAX_MS,
			POLL_ERROR_BASE_MS * 2 ** Math.max(0, this.#consecutiveFailures - 1)
		);
		// Jitter so ten tabs that lost the network together don't retry in lockstep.
		return Math.round(base * (0.8 + Math.random() * 0.4));
	}

	#schedulePoll(delay: number): void {
		if (!this.#running || this.meManagerKey === null) return;
		if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return;

		this.#clearPollTimer();
		// setTimeout chained after each response, never setInterval — an interval
		// stacks requests as soon as the network is slower than the period.
		this.#pollTimer = setTimeout(() => {
			this.#pollTimer = null;
			void this.#poll();
		}, delay);
	}

	#clearPollTimer(): void {
		if (this.#pollTimer !== null) {
			clearTimeout(this.#pollTimer);
			this.#pollTimer = null;
		}
	}

	#flushRead(viaBeacon: boolean): void {
		if (this.meManagerKey === null || this.#lastSentReadKey <= 0) return;
		if (this.#readTimer !== null) {
			clearTimeout(this.#readTimer);
			this.#readTimer = null;
		}

		const body = JSON.stringify({
			channelId: this.channelId,
			lastReadMessageKey: this.#lastSentReadKey
		});

		// fetch() during pagehide is not reliably delivered; sendBeacon is.
		if (viaBeacon && typeof navigator !== 'undefined' && navigator.sendBeacon) {
			navigator.sendBeacon('/api/chat/read', new Blob([body], { type: 'application/json' }));
			return;
		}

		void this.#fetch('/api/chat/read', {
			method: 'PUT',
			headers: { 'Content-Type': 'application/json' },
			body
		}).catch(() => {
			// A missed watermark costs a stale "New" divider, nothing more.
		});
	}

	async #errorText(response: Response): Promise<string> {
		try {
			const data = await response.json();
			return typeof data?.error === 'string' ? data.error : `Request failed (${response.status}).`;
		} catch {
			return `Request failed (${response.status}).`;
		}
	}
}

export function createChatStore(opts: ChatStoreOptions): ChatStore {
	return new ChatStore(opts);
}

import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ChatStore } from './chat.svelte';
import type { ChatMessageDTO, ReactionGroup } from '$lib/chat/types';

/**
 * The store's merge and polling behaviour.
 *
 * Named `chat.store.test.ts` rather than `chat.svelte.test.ts` on purpose: the
 * latter is routed to the browser project by vite.config.ts, and none of this
 * needs a DOM. The rune state in `chat.svelte.ts` is proxy-based, so reads after
 * an await reflect writes without an effect flush.
 */

let clock = 1_700_000_000_000;
const now = () => clock;

function dto(overrides: Partial<ChatMessageDTO> = {}): ChatMessageDTO {
	return {
		messageKey: 1,
		messageId: 'm_serveraaaaaaaaaaaaaa',
		content: 'hello',
		channelId: 'general',
		parentMessageKey: null,
		messageType: 'message',
		authorKey: 14,
		authorName: 'Luke S',
		authorDisplayName: 'Luke S',
		authorProfileImage: null,
		mentions: [],
		createdAt: clock,
		updatedAt: clock,
		editedAt: null,
		deletedAt: null,
		reactions: [],
		replyCount: 0,
		lastReplyAt: null,
		...overrides
	};
}

function jsonResponse(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { 'Content-Type': 'application/json' }
	});
}

function bootstrapBody(messages: ChatMessageDTO[] = []) {
	return {
		mode: 'bootstrap',
		channelId: 'general',
		messages,
		hasMore: false,
		roster: [{ managerKey: 14, name: 'Luke S', displayName: 'Luke S', profileImageUrl: null }],
		lastReadMessageKey: null,
		cursor: { messageKey: messages.at(-1)?.messageKey ?? 0, ts: clock },
		serverTime: clock,
		me: { managerKey: 14, displayName: 'Luke S' }
	};
}

function deltaBody(messages: ChatMessageDTO[], reactions: Record<string, ReactionGroup[]> = {}) {
	return {
		mode: 'delta',
		channelId: 'general',
		messages,
		reactions,
		cursor: { messageKey: messages.at(-1)?.messageKey ?? 0, ts: clock + 1000 },
		serverTime: clock
	};
}

/** A fetch stub that records calls and answers from a per-URL queue. */
function makeFetch() {
	const calls: { url: string; method: string; body: any }[] = [];
	const handlers: ((url: string, init?: RequestInit) => Response | null)[] = [];

	const impl = (async (input: RequestInfo | URL, init?: RequestInit) => {
		const url = String(input);
		const method = init?.method ?? 'GET';
		calls.push({ url, method, body: init?.body ? JSON.parse(String(init.body)) : null });

		for (const handler of handlers) {
			const response = handler(url, init);
			if (response) return response;
		}
		return jsonResponse(bootstrapBody());
	}) as unknown as typeof fetch;

	return {
		impl,
		calls,
		on(handler: (url: string, init?: RequestInit) => Response | null) {
			handlers.push(handler);
		}
	};
}

function makeStore(fetchImpl: typeof fetch, meManagerKey: number | null = 14) {
	return new ChatStore({ channelId: 'general', meManagerKey, fetchImpl, now });
}

/**
 * A promise plus the function that resolves it.
 *
 * `let release: (() => void) | null = null` assigned inside an executor is
 * narrowed to `null` by TypeScript, which makes `release?.()` a type error even
 * though it is correct at runtime.
 */
function deferred(): { promise: Promise<void>; resolve: () => void } {
	let resolve!: () => void;
	const promise = new Promise<void>((r) => (resolve = r));
	return { promise, resolve };
}

/** Let the store's in-flight promises settle without waiting on real timers. */
async function settle(times = 4) {
	for (let i = 0; i < times; i++) await Promise.resolve();
	await new Promise((resolve) => setTimeout(resolve, 0));
}

beforeEach(() => {
	clock = 1_700_000_000_000;
});

describe('signed out', () => {
	it('makes no requests at all', async () => {
		const fetcher = makeFetch();
		const store = makeStore(fetcher.impl, null);

		store.start();
		await settle();

		// The old panel fetched regardless and swallowed the 401, leaving a blank box.
		expect(fetcher.calls).toHaveLength(0);
		expect(store.connectionState).toBe('unauthorized');
		expect(store.initialLoadDone).toBe(true);
		store.stop();
	});
});

describe('optimistic send', () => {
	it('shows the message immediately, then confirms it in place', async () => {
		const fetcher = makeFetch();
		fetcher.on((url, init) =>
			init?.method === 'POST'
				? jsonResponse({
						message: dto({
							messageKey: 7,
							messageId: JSON.parse(String(init.body)).messageId,
							content: 'first'
						})
					})
				: null
		);

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();

		const sending = store.send('first');
		// Present before the network has answered.
		expect(store.messages).toHaveLength(1);
		expect(store.messages[0].sendState).toBe('pending');
		expect(store.messages[0].messageKey).toBeNull();
		const optimisticId = store.messages[0].messageId;

		await sending;
		await settle();

		expect(store.messages).toHaveLength(1);
		expect(store.messages[0].messageId).toBe(optimisticId);
		expect(store.messages[0].sendState).toBe('sent');
		expect(store.messages[0].messageKey).toBe(7);
		store.stop();
	});

	it('does not duplicate when a poll returns the message you just sent', async () => {
		// The highest-value test here. Client-minted messageId is what makes the
		// optimistic row and the polled row the same row.
		const fetcher = makeFetch();
		let sentId = '';
		fetcher.on((url, init) => {
			if (init?.method === 'POST') {
				sentId = JSON.parse(String(init.body)).messageId;
				return jsonResponse({ message: dto({ messageKey: 7, messageId: sentId, content: 'first' }) });
			}
			if (url.includes('since=')) {
				return jsonResponse(deltaBody([dto({ messageKey: 7, messageId: sentId, content: 'first' })]));
			}
			return null;
		});

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();

		await store.send('first');
		await settle();
		await store.refresh();
		await settle();

		expect(store.messages).toHaveLength(1);
		expect(store.messages[0].messageKey).toBe(7);
		store.stop();
	});

	it('does not duplicate when the poll lands before the POST resolves', async () => {
		const fetcher = makeFetch();
		let sentId = '';
		const post = deferred();

		fetcher.on((url, init) => {
			if (init?.method === 'POST') {
				sentId = JSON.parse(String(init.body)).messageId;
				return null; // handled below by the deferred handler
			}
			if (url.includes('since=') && sentId) {
				return jsonResponse(deltaBody([dto({ messageKey: 7, messageId: sentId, content: 'first' })]));
			}
			return null;
		});

		const deferredFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
			if (init?.method === 'POST') {
				sentId = JSON.parse(String(init.body)).messageId;
				await post.promise;
				return jsonResponse({ message: dto({ messageKey: 7, messageId: sentId, content: 'first' }) });
			}
			return fetcher.impl(input, init);
		}) as unknown as typeof fetch;

		const store = makeStore(deferredFetch);
		store.start();
		await settle();

		const sending = store.send('first');
		await settle();

		// A poll completes while the POST is still open.
		await store.refresh();
		await settle();
		expect(store.messages).toHaveLength(1);

		post.resolve();
		await sending;
		await settle();

		expect(store.messages).toHaveLength(1);
		expect(store.messages[0].sendState).toBe('sent');
		store.stop();
	});

	it('marks a rejected send as failed, and retries under the same id', async () => {
		const fetcher = makeFetch();
		let failNext = true;
		let seenIds: string[] = [];

		fetcher.on((url, init) => {
			if (init?.method !== 'POST') return null;
			const id = JSON.parse(String(init.body)).messageId;
			seenIds.push(id);
			if (failNext) {
				failNext = false;
				return jsonResponse({ error: 'nope' }, 500);
			}
			return jsonResponse({ message: dto({ messageKey: 7, messageId: id }) });
		});

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();

		await store.send('first');
		await settle();
		expect(store.messages[0].sendState).toBe('failed');

		await store.retry(store.messages[0].messageId);
		await settle();

		expect(store.messages).toHaveLength(1);
		expect(store.messages[0].sendState).toBe('sent');
		// A retry that minted a fresh id would double-post the moment the first
		// request had actually succeeded but the response was lost.
		expect(seenIds[0]).toBe(seenIds[1]);
		store.stop();
	});

	it('discards a failed message on request', async () => {
		const fetcher = makeFetch();
		fetcher.on((url, init) => (init?.method === 'POST' ? jsonResponse({ error: 'no' }, 500) : null));

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();

		await store.send('doomed');
		await settle();
		store.discard(store.messages[0].messageId);

		expect(store.messages).toHaveLength(0);
		store.stop();
	});
});

describe('delta merge', () => {
	it('applies edits and tombstones to the existing row rather than appending', async () => {
		const fetcher = makeFetch();
		let phase: 'edited' | 'deleted' = 'edited';

		fetcher.on((url) => {
			if (!url.includes('since=')) return null;
			return jsonResponse(
				deltaBody([
					phase === 'edited'
						? dto({ messageKey: 1, content: 'edited text', editedAt: clock })
						: dto({ messageKey: 1, content: 'edited text', deletedAt: clock })
				])
			);
		});
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();
		expect(store.messages).toHaveLength(1);

		await store.refresh();
		await settle();
		expect(store.messages).toHaveLength(1);
		expect(store.messages[0].content).toBe('edited text');

		phase = 'deleted';
		await store.refresh();
		await settle();
		expect(store.messages).toHaveLength(1);
		expect(store.messages[0].deletedAt).not.toBeNull();
		store.stop();
	});

	it('advances the cursor using the server clock, never the local one', async () => {
		const fetcher = makeFetch();
		const SERVER_TS = 1_700_000_555_000;

		fetcher.on((url) =>
			url.includes('since=')
				? jsonResponse({
						mode: 'delta',
						channelId: 'general',
						messages: [dto({ messageKey: 9, messageId: 'm_nineaaaaaaaaaaaaaaaa' })],
						reactions: {},
						// A server clock that is deliberately nothing like the local one.
						cursor: { messageKey: 9, ts: SERVER_TS },
						serverTime: SERVER_TS
					})
				: null
		);
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();

		// First poll consumes the bootstrap cursor and installs the server's.
		await store.refresh();
		await settle();

		// Local clock jumps an hour ahead. The next poll must still send the
		// server's own ts — computing it locally would permanently skip edits.
		clock += 60 * 60 * 1000;
		await store.refresh();
		await settle();

		const polls = fetcher.calls.filter((c) => c.url.includes('since='));
		const lastPoll = polls[polls.length - 1];
		expect(lastPoll.url).toContain('since=9');
		expect(lastPoll.url).toContain(`sinceTs=${SERVER_TS}`);
		store.stop();
	});

	it('counts remote arrivals while scrolled away, and not your own', async () => {
		const fetcher = makeFetch();
		let batch: ChatMessageDTO[] = [];
		fetcher.on((url) => (url.includes('since=') ? jsonResponse(deltaBody(batch)) : null));
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();
		store.setAtBottom(false);

		batch = [
			dto({ messageKey: 2, messageId: 'm_otheraaaaaaaaaaaaaaa', authorKey: 18 }),
			dto({ messageKey: 3, messageId: 'm_mineaaaaaaaaaaaaaaaa', authorKey: 14 })
		];
		await store.refresh();
		await settle();

		expect(store.newMessageCount).toBe(1);
		store.setAtBottom(true);
		expect(store.newMessageCount).toBe(0);
		store.stop();
	});
});

describe('optimistic reactions', () => {
	function reactionGroup(mine: boolean, count = 1): ReactionGroup {
		return {
			emoji: '🔥',
			emojiType: 'unicode',
			count,
			mine,
			users: mine ? [{ managerKey: 14, name: 'Luke S' }] : [{ managerKey: 18, name: 'Trevor' }]
		};
	}

	it('flips the pill before the network answers', async () => {
		const fetcher = makeFetch();
		const toggleGate = deferred();

		const slowFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
			if (String(input).includes('/reactions')) {
				await toggleGate.promise;
				return jsonResponse({ messageKey: 1, mine: true, reactions: [reactionGroup(true)] });
			}
			return fetcher.impl(input, init);
		}) as unknown as typeof fetch;

		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(slowFetch);
		store.start();
		await settle();

		const id = store.messages[0].messageId;
		const toggling = store.toggleReaction(id, '🔥');
		await settle(1);

		expect(store.reactionsFor(id)).toEqual([
			expect.objectContaining({ emoji: '🔥', count: 1, mine: true })
		]);

		toggleGate.resolve();
		await toggling;
		store.stop();
	});

	it('is not reverted by a poll that was already in flight when the toggle resolved', async () => {
		const fetcher = makeFetch();
		// Server state deliberately lags: it still reports no reaction.
		fetcher.on((url) =>
			url.includes('since=') ? jsonResponse(deltaBody([], { '1': [] })) : null
		);
		fetcher.on((url) =>
			url.includes('/reactions')
				? jsonResponse({ messageKey: 1, mine: true, reactions: [reactionGroup(true)] })
				: null
		);
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();

		const id = store.messages[0].messageId;
		await store.toggleReaction(id, '🔥');
		await settle();

		// A poll whose request started before the toggle resolved must not clear
		// the pending overlay — it is carrying pre-toggle data.
		clock -= 5_000;
		await store.refresh();
		await settle();

		expect(store.reactionsFor(id).find((g) => g.emoji === '🔥')?.mine).toBe(true);
		store.stop();
	});

	it('adopts server truth once a later poll confirms it', async () => {
		const fetcher = makeFetch();
		fetcher.on((url) =>
			url.includes('since=') ? jsonResponse(deltaBody([], { '1': [reactionGroup(true)] })) : null
		);
		fetcher.on((url) =>
			url.includes('/reactions')
				? jsonResponse({ messageKey: 1, mine: true, reactions: [reactionGroup(true)] })
				: null
		);
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();

		const id = store.messages[0].messageId;
		await store.toggleReaction(id, '🔥');
		await settle();

		clock += 5_000;
		await store.refresh();
		await settle();

		const groups = store.reactionsFor(id);
		expect(groups).toHaveLength(1);
		expect(groups[0].count).toBe(1);
		expect(groups[0].mine).toBe(true);
		store.stop();
	});
});

describe('failure handling', () => {
	it('backs off and reports itself as reconnecting after repeated failures', async () => {
		const fetcher = makeFetch();
		fetcher.on((url) => (url.includes('since=') ? jsonResponse({ error: 'down' }, 500) : null));
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();
		expect(store.connectionState).toBe('live');

		for (let i = 0; i < 3; i++) {
			await store.refresh();
			await settle();
		}

		expect(store.connectionState).toBe('reconnecting');
		store.stop();
	});

	it('treats a 401 on the poll as no longer authorised', async () => {
		const fetcher = makeFetch();
		fetcher.on((url) => (url.includes('since=') ? jsonResponse({ error: 'nope' }, 401) : null));
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();
		await store.refresh();
		await settle();

		expect(store.connectionState).toBe('unauthorized');
		store.stop();
	});
});

describe('lifecycle', () => {
	it('can be stopped and started again without going dead', async () => {
		// The regression this pins: a one-way `disposed` flag meant an $effect
		// re-run left the store permanently showing skeletons.
		const fetcher = makeFetch();
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));

		const store = makeStore(fetcher.impl);
		const teardown = store.start();
		await settle();
		expect(store.initialLoadDone).toBe(true);

		teardown();
		store.start();
		await settle();

		expect(store.initialLoadDone).toBe(true);
		expect(store.messages).toHaveLength(1);
		store.stop();
	});

	it('leaves no timer behind after stop', async () => {
		const fetcher = makeFetch();
		fetcher.on((url) => (url.includes('since=') ? null : jsonResponse(bootstrapBody([dto()]))));
		const clearSpy = vi.spyOn(globalThis, 'clearTimeout');

		const store = makeStore(fetcher.impl);
		store.start();
		await settle();
		store.stop();

		expect(clearSpy).toHaveBeenCalled();
		clearSpy.mockRestore();
	});
});

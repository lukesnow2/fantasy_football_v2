// Best-effort in-process rate limiting for the sign-in surface.
//
// Deliberately dependency-free: there is no Redis/KV in this project, and the
// real access control is the allowlist in members.ts — a magic link is only ever
// issued to an address already sitting in app.league_member. This is a second
// layer with a narrow job: stop one noisy source from flooding a manager's inbox
// with sign-in links, or from spinning the database.
//
// Honest about its limits: counters live in the memory of a single serverless
// instance, so a caller spread across instances (or arriving after a cold start)
// gets a fresh budget. It shaves the volume of a naive flood; it is not a
// guarantee, and nothing security-critical may depend on it.

type Bucket = { count: number; resetAt: number };

const MAX_TRACKED_KEYS = 5_000;

/**
 * Exported, with an injectable clock, so the window-reset branch can be tested.
 *
 * The capping direction is the obvious one to check; the reset is the dangerous
 * one. A regression that never advances resetAt locks every manager out of
 * sign-in permanently after their fifth attempt — a total outage for a league
 * where this is the only way in — and a cap-only test still passes.
 */
export function createLimiter(limit: number, windowMs: number, clock: () => number = Date.now) {
	const buckets = new Map<string, Bucket>();

	return function consume(key: string): boolean {
		const now = clock();

		// Drop expired buckets, and if the map is still oversized (a key-rotating
		// flood) clear it wholesale rather than letting it grow without bound.
		if (buckets.size > MAX_TRACKED_KEYS) {
			for (const [k, b] of buckets) if (b.resetAt <= now) buckets.delete(k);
			if (buckets.size > MAX_TRACKED_KEYS) buckets.clear();
		}

		const bucket = buckets.get(key);
		if (!bucket || bucket.resetAt <= now) {
			buckets.set(key, { count: 1, resetAt: now + windowMs });
			return true;
		}
		if (bucket.count >= limit) return false;
		bucket.count += 1;
		return true;
	};
}

// Per-address send budget. A real person needs one link, and retries a couple of
// times when it lands in spam; 5 in 15 minutes covers that comfortably while
// capping how much mail a single mailbox can be made to receive.
const emailSendLimiter = createLimiter(5, 15 * 60 * 1000);

// Per-IP budget, sized above the email budget so a manager sharing a household
// or office NAT with another manager is not locked out by their neighbour, while
// still capping how fast one host can probe the roster.
const ipSendLimiter = createLimiter(20, 15 * 60 * 1000);

export function consumeLoginEmailBudget(email: string): boolean {
	return emailSendLimiter(email.toLowerCase().trim());
}

export function consumeLoginIpBudget(ip: string): boolean {
	return ipSendLimiter(ip.trim());
}

// Deliberately called from the /login form action rather than from
// hooks.server.ts. Under adapter-vercel a hook can run in a different instance
// from the one that served the last request, which would make a per-instance
// counter there both ineffective and misleading. The form action runs in the
// Node serverless function, where a module-level map at least survives warm
// invocations.

// Chat write budgets, keyed on manager_key rather than IP. Every chat write
// already requires a session, so the manager key is the strongest identifier
// available, and two managers behind one household NAT don't share a budget.
//
// Same honesty caveat as the login limiters: these counters live in one
// serverless instance's memory. They exist to stop a runaway client — or a bored
// manager with a for-loop — from filling the table, not to resist an adversary
// who can spread requests across instances.
//
// Note there is no budget on the chat GET. That endpoint is polled by design;
// limiting it would break the feature it exists to serve.
const chatPostLimiter = createLimiter(20, 60 * 1000);
const chatEditLimiter = createLimiter(40, 60 * 1000);
const chatReactionLimiter = createLimiter(60, 60 * 1000);

export function consumeChatPostBudget(managerKey: number): boolean {
	return chatPostLimiter(`post:${managerKey}`);
}

export function consumeChatEditBudget(managerKey: number): boolean {
	return chatEditLimiter(`edit:${managerKey}`);
}

export function consumeChatReactionBudget(managerKey: number): boolean {
	return chatReactionLimiter(`react:${managerKey}`);
}

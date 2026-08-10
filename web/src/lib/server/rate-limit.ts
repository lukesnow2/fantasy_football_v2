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

function createLimiter(limit: number, windowMs: number) {
	const buckets = new Map<string, Bucket>();

	return function consume(key: string): boolean {
		const now = Date.now();

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

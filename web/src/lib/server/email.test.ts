import { describe, expect, it, vi } from 'vitest';

// The repo's .env is baked into $env/dynamic/private by the SvelteKit plugin
// before vitest runs, so without this the transports would be built from
// whatever key happens to be on the developer's laptop.
vi.mock('$env/dynamic/private', () => ({
	env: {
		EMAIL_PROVIDER: 'console',
		EMAIL_FROM: 'league@example.com',
		EMAIL_FROM_NAME: 'The League',
		NODE_ENV: 'test'
	}
}));

const { ResendEmailService, formatFromHeader, generateMagicLinkEmail, MAX_BATCH } = await import(
	'./email'
);

const FROM = () => ({ address: 'league@example.com', name: 'The League' });

interface StubResponse {
	status: number;
	body?: unknown;
	headers?: Record<string, string>;
}

interface RecordedCall {
	url: string;
	body: unknown;
	headers: Record<string, string>;
}

/** A fetch that answers from a script, and records what it was asked. */
function stubFetch(script: StubResponse[]) {
	const calls: RecordedCall[] = [];
	const order: string[] = [];
	let next = 0;

	const fetchImpl = (async (url: unknown, init: RequestInit) => {
		const index = next++;
		const spec = script[index] ?? { status: 500 };
		const headers = init.headers as Record<string, string>;
		calls.push({ url: String(url), body: JSON.parse(init.body as string), headers });

		order.push(`start:${index}`);
		// Yield, so a caller that fired requests concurrently would interleave.
		await Promise.resolve();
		order.push(`end:${index}`);

		return new Response(spec.body === undefined ? null : JSON.stringify(spec.body), {
			status: spec.status,
			headers: spec.headers
		});
	}) as unknown as typeof fetch;

	return { fetchImpl, calls, order };
}

function transport(script: StubResponse[], resolveFrom = FROM) {
	const { fetchImpl, calls, order } = stubFetch(script);
	const slept: number[] = [];
	const logs: string[] = [];
	const service = new ResendEmailService('test-key', resolveFrom, {
		fetchImpl,
		sleep: async (ms: number) => {
			slept.push(ms);
		},
		newIdempotencyKey: () => 'fixed-key',
		timeoutMs: 100,
		log: (message: string) => logs.push(message)
	});
	return { service, calls, order, slept, logs };
}

const MESSAGE = { to: 'a@b.c', subject: 'Subject', html: '<p>Body</p>', text: 'Body' };

describe('formatFromHeader', () => {
	it('omits the display name when there is none', () => {
		expect(formatFromHeader({ address: 'x@y.z', name: null })).toBe('x@y.z');
	});

	it('quotes a display name containing a comma', () => {
		// Unquoted, "Smith, John <x@y.z>" parses as two addresses and the provider
		// rejects the entire send rather than just the name.
		expect(formatFromHeader({ address: 'x@y.z', name: 'Smith, John' })).toBe(
			'"Smith, John" <x@y.z>'
		);
	});
});

describe('ResendEmailService.sendEmail', () => {
	it('sends the from header and a single recipient', async () => {
		const { service, calls } = transport([{ status: 200, body: { id: 'abc' } }]);
		await expect(service.sendEmail(MESSAGE)).resolves.toBe(true);

		expect(calls[0].body).toMatchObject({
			from: 'The League <league@example.com>',
			to: ['a@b.c'],
			subject: 'Subject'
		});
	});

	it('returns false without throwing when the from address cannot be resolved', async () => {
		// Defect 2, and the contract that protects the login flow. If this threw,
		// a provisioned address would get an error page while an unknown address
		// still redirected quietly — which is the roster-enumeration oracle that
		// login/+page.server.ts is built to avoid.
		const { service, calls, logs } = transport([], () => {
			throw new Error('EMAIL_FROM must be set when EMAIL_PROVIDER=resend.');
		});

		await expect(service.sendEmail(MESSAGE)).resolves.toBe(false);
		expect(calls).toHaveLength(0);
		expect(logs.join()).toContain('EMAIL_FROM');
	});

	it("surfaces Resend's top-level error message", async () => {
		// The shape Resend actually returns. The previous code read only
		// body.error.message, which never matches, so the single most common
		// misconfiguration was reported as a bare "HTTP 403" and the sentence
		// naming the fix was discarded.
		const { service, logs } = transport([
			{
				status: 403,
				body: {
					statusCode: 403,
					name: 'validation_error',
					message: 'The example.com domain is not verified'
				}
			}
		]);

		await expect(service.sendEmail(MESSAGE)).resolves.toBe(false);
		expect(logs.join()).toContain('domain is not verified');
	});

	it('still reads a nested error shape', async () => {
		const { service, logs } = transport([{ status: 400, body: { error: { message: 'legacy' } } }]);
		await expect(service.sendEmail(MESSAGE)).resolves.toBe(false);
		expect(logs.join()).toContain('legacy');
	});

	it('falls back to the status when the body explains nothing', async () => {
		const { service, logs } = transport([{ status: 500 }, { status: 500 }]);
		await expect(service.sendEmail(MESSAGE)).resolves.toBe(false);
		expect(logs.join()).toContain('HTTP 500');
	});

	it('retries once on 429 and succeeds', async () => {
		const { service, calls, slept } = transport([
			{ status: 429 },
			{ status: 200, body: { id: 'abc' } }
		]);

		await expect(service.sendEmail(MESSAGE)).resolves.toBe(true);
		expect(calls).toHaveLength(2);
		expect(slept).toEqual([250]);
	});

	it('retries at most once', async () => {
		// The failure the whole change exists to prevent: an unbounded retry loop
		// hammering a provider that has already said stop.
		const { service, calls } = transport([{ status: 429 }, { status: 429 }]);
		await expect(service.sendEmail(MESSAGE)).resolves.toBe(false);
		expect(calls).toHaveLength(2);
	});

	it('honours a short retry-after', async () => {
		const { service, slept } = transport([
			{ status: 429, headers: { 'retry-after': '1' } },
			{ status: 200, body: { id: 'abc' } }
		]);
		await service.sendEmail(MESSAGE);
		expect(slept).toEqual([1000]);
	});

	it('refuses to retry when told to wait longer than the budget', async () => {
		// Waiting 60s inside a form action is worse than one logged failure, and
		// retrying early would only earn a second refusal.
		const { service, calls, slept } = transport([{ status: 429, headers: { 'retry-after': '60' } }]);
		await expect(service.sendEmail(MESSAGE)).resolves.toBe(false);
		expect(calls).toHaveLength(1);
		expect(slept).toEqual([]);
	});

	it('tolerates an HTTP-date retry-after', async () => {
		// Retry-After may be a date, which Number() turns into NaN.
		// setTimeout(NaN) fires immediately, turning a paced retry into an instant
		// re-flood, so the default delay has to win.
		const { service, slept } = transport([
			{ status: 503, headers: { 'retry-after': 'Wed, 21 Oct 2026 07:28:00 GMT' } },
			{ status: 200, body: { id: 'abc' } }
		]);
		await service.sendEmail(MESSAGE);
		expect(slept).toEqual([250]);
	});

	it('does not retry a 422', async () => {
		// Permanent. Re-sending burns quota and risks a duplicate.
		const { service, calls } = transport([{ status: 422, body: { message: 'bad address' } }]);
		await expect(service.sendEmail(MESSAGE)).resolves.toBe(false);
		expect(calls).toHaveLength(1);
	});

	it('reuses one idempotency key across the retry', async () => {
		// What makes retrying an ambiguous 5xx safe: if the first attempt actually
		// delivered, Resend dedupes the second rather than sending two sign-in
		// links for one request.
		const { service, calls } = transport([{ status: 500 }, { status: 200, body: { id: 'abc' } }]);
		await service.sendEmail(MESSAGE);

		expect(calls[0].headers['Idempotency-Key']).toBe(calls[1].headers['Idempotency-Key']);
		expect(calls[0].headers['Idempotency-Key']).toBeTruthy();
	});

	it('does not retry a timeout', async () => {
		// Two 8s attempts stacked inside a login action is worse than one failure,
		// and a timeout means the request may have been received anyway.
		const fetchImpl = (async () => {
			throw new DOMException('The operation timed out.', 'TimeoutError');
		}) as unknown as typeof fetch;

		let attempts = 0;
		const counting = (async (...args: Parameters<typeof fetch>) => {
			attempts++;
			return fetchImpl(...args);
		}) as unknown as typeof fetch;

		const service = new ResendEmailService('k', FROM, { fetchImpl: counting, log: () => {} });
		await expect(service.sendEmail(MESSAGE)).resolves.toBe(false);
		expect(attempts).toBe(1);
	});
});

describe('ResendEmailService.sendBatch', () => {
	const nine = Array.from({ length: 9 }, (_, i) => ({ ...MESSAGE, to: `m${i}@b.c` }));

	it('sends nine recipients in one request', async () => {
		const { service, calls } = transport([
			{ status: 200, body: { data: nine.map((_, i) => ({ id: `id-${i}` })) } }
		]);

		const result = await service.sendBatch(nine);
		expect(calls).toHaveLength(1);
		expect(calls[0].url).toBe('https://api.resend.com/emails/batch');
		expect(calls[0].body).toHaveLength(9);
		expect(result.succeeded).toBe(9);
		expect(result.failed).toBe(0);
	});

	it('reports partial failure positionally', async () => {
		const three = nine.slice(0, 3);
		const { service } = transport([
			{ status: 200, body: { data: [{ id: 'a' }, null, { id: 'c' }] } }
		]);

		const result = await service.sendBatch(three);
		expect(result.sent).toEqual([true, false, true]);
		expect(result.succeeded).toBe(2);
		expect(result.failed).toBe(1);
	});

	it('fails the whole chunk when the response does not cover every message', async () => {
		// The load-bearing case. A short array means an entry was omitted and we
		// cannot tell which — attributing ids by position would shift every index,
		// naming the wrong managers AND marking a failed address as sent.
		const three = nine.slice(0, 3);
		const { service, logs } = transport([{ status: 200, body: { data: [{ id: 'a' }] } }]);

		const result = await service.sendBatch(three);
		expect(result.sent).toEqual([false, false, false]);
		expect(logs.join()).toContain('covered 1 of 3');
	});

	it('fails everything when the response has no data key', async () => {
		const { service } = transport([{ status: 200, body: {} }]);
		const result = await service.sendBatch(nine.slice(0, 2));
		expect(result.succeeded).toBe(0);
	});

	it('makes no request for an empty list', async () => {
		const { service, calls } = transport([]);
		const result = await service.sendBatch([]);
		expect(calls).toHaveLength(0);
		expect(result).toEqual({ sent: [], succeeded: 0, failed: 0 });
	});

	it('resolves rather than rejecting when fetch throws', async () => {
		const fetchImpl = (async () => {
			throw new Error('network down');
		}) as unknown as typeof fetch;
		const service = new ResendEmailService('k', FROM, { fetchImpl, log: () => {} });

		const result = await service.sendBatch(nine);
		expect(result.sent).toHaveLength(9);
		expect(result.succeeded).toBe(0);
	});

	it('chunks above the batch ceiling and sends the chunks sequentially', async () => {
		// Guards two failures at once: silently dropping message 101, and
		// reintroducing the concurrency this change exists to remove.
		const many = Array.from({ length: MAX_BATCH + 5 }, (_, i) => ({ ...MESSAGE, to: `m${i}@b.c` }));
		const { service, calls, order } = transport([
			{ status: 200, body: { data: Array.from({ length: MAX_BATCH }, (_, i) => ({ id: `a${i}` })) } },
			{ status: 200, body: { data: Array.from({ length: 5 }, (_, i) => ({ id: `b${i}` })) } }
		]);

		const result = await service.sendBatch(many);
		expect(calls).toHaveLength(2);
		expect(calls[0].body).toHaveLength(MAX_BATCH);
		expect(calls[1].body).toHaveLength(5);
		expect(order).toEqual(['start:0', 'end:0', 'start:1', 'end:1']);
		expect(result.succeeded).toBe(MAX_BATCH + 5);
	});

	it('marks only the failing chunk', async () => {
		const many = Array.from({ length: MAX_BATCH + 5 }, (_, i) => ({ ...MESSAGE, to: `m${i}@b.c` }));
		const { service } = transport([
			{ status: 200, body: { data: Array.from({ length: MAX_BATCH }, (_, i) => ({ id: `a${i}` })) } },
			{ status: 429 },
			{ status: 429 }
		]);

		const result = await service.sendBatch(many);
		expect(result.succeeded).toBe(MAX_BATCH);
		expect(result.failed).toBe(5);
	});
});

describe('generateMagicLinkEmail', () => {
	const url = 'https://league.example/login/verify?token=abc';

	it('escapes a display name in the HTML body', async () => {
		const mail = generateMagicLinkEmail(url, 'a@b.c', {
			purpose: 'invite',
			displayName: '<script>alert(1)</script>'
		});

		expect(mail.html).not.toContain('<script');
		expect(mail.html).toContain('&lt;script');
	});

	it('leaves the display name raw in the text body', async () => {
		// The regression the escaping fix invites. Sharing one greeting between
		// both bodies fixes the injection and makes O'Brien read "O&#39;Brien" in
		// plain text in the same edit.
		const mail = generateMagicLinkEmail(url, 'a@b.c', {
			purpose: 'login',
			displayName: "O'Brien"
		});

		expect(mail.text).toContain("Hi O'Brien,");
		expect(mail.html).toContain('O&#39;Brien');
	});

	it('falls back cleanly with no display name', async () => {
		const mail = generateMagicLinkEmail(url, 'a@b.c', { purpose: 'login', displayName: null });
		expect(mail.text).toContain('Hi,');
		expect(mail.html).toContain('Hi,');
		expect(mail.text).not.toContain('undefined');
		expect(mail.html).not.toContain('undefined');
	});

	it('keeps the pasteable link intact in the text body', async () => {
		// The fallback for when the button does not render. An entity-encoded URL
		// here would be pasted broken.
		const multi = 'https://league.example/login/verify?token=abc&redirect=%2Fhome';
		const mail = generateMagicLinkEmail(multi, 'a@b.c', { purpose: 'login' });

		expect(mail.text).toContain(multi);
		// Correct inside an href: attribute values are entity-decoded before the
		// URL is resolved, so this still points at the same place.
		expect(mail.html).toContain('token=abc&amp;redirect=%2Fhome');
	});

	it('distinguishes an invite from a sign-in', async () => {
		const invite = generateMagicLinkEmail(url, 'a@b.c', { purpose: 'invite' });
		const login = generateMagicLinkEmail(url, 'a@b.c', { purpose: 'login' });
		expect(invite.subject).not.toBe(login.subject);
	});
});

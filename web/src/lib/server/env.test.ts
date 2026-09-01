import { describe, expect, it, vi } from 'vitest';

/**
 * env.ts reads $env/dynamic/private at module load and caches the values in
 * consts, so each configuration needs its own module instance.
 */
async function loadEnv(overrides: Record<string, string | undefined>) {
	vi.resetModules();
	vi.doMock('$env/dynamic/private', () => ({ env: overrides }));
	return import('./env');
}

const CONFIGURED = {
	NODE_ENV: 'production',
	EMAIL_PROVIDER: 'resend',
	RESEND_API_KEY: 'key',
	EMAIL_FROM: 'league@example.com',
	ORIGIN: 'https://oakdalepark.xyz',
	DATABASE_URL: 'postgres://x'
};

describe('describeMailConfig', () => {
	it('reports nothing when everything is set', async () => {
		const { describeMailConfig, CANONICAL_ORIGIN } = await loadEnv(CONFIGURED);
		expect(describeMailConfig()).toEqual({
			ok: true,
			problems: [],
			origin: 'https://oakdalepark.xyz',
			canonicalOrigin: CANONICAL_ORIGIN,
			originOk: true
		});
	});

	it('flags an ORIGIN pointing somewhere other than the canonical host', async () => {
		// The defect this was written for: a preview URL left in ORIGIN mails every
		// manager a sign-in link to the wrong host, and nothing else notices —
		// mail sends, no exception is thrown, and the value is write-only in the
		// hosting dashboard so nobody can read back what went out.
		const { describeMailConfig } = await loadEnv({
			...CONFIGURED,
			ORIGIN: 'https://the-league-abc123.vercel.app'
		});
		const { ok, problems } = describeMailConfig();

		expect(ok).toBe(false);
		expect(problems.join()).toContain('vercel.app');
	});

	it('ignores a trailing slash and casing', async () => {
		const { describeMailConfig } = await loadEnv({
			...CONFIGURED,
			ORIGIN: 'https://OakdalePark.xyz/'
		});
		expect(describeMailConfig().ok).toBe(true);
	});

	it('flags whitespace that would land inside the link', async () => {
		// `echo value | vercel env add` appends a newline; the result is
		// "https://host\n/login/verify", which no mail client will open.
		const { describeMailConfig } = await loadEnv({
			...CONFIGURED,
			ORIGIN: 'https://oakdalepark.xyz\n'
		});
		expect(describeMailConfig().problems.join()).toContain('whitespace');
	});

	it('does not second-guess the origin outside production', async () => {
		// localhost is the correct value on a dev machine; warning about it there
		// would train everyone to ignore the banner.
		const { describeMailConfig } = await loadEnv({
			NODE_ENV: 'development',
			EMAIL_PROVIDER: 'console',
			ORIGIN: 'http://localhost:5175'
		});
		const report = describeMailConfig();

		expect(report.ok).toBe(true);
		// The banner has to stay dark here too, or nobody reads it on the one
		// deployment where it matters.
		expect(report.originOk).toBe(true);
	});

	it('strips whitespace and a trailing slash from the origin itself', async () => {
		// Detection is not enough: requireOrigin() feeds this straight into every
		// sign-in link, so a newline left by `echo value | vercel env add` would
		// break the link for the whole league until somebody read the banner.
		const { ORIGIN, requireOrigin } = await loadEnv({
			...CONFIGURED,
			ORIGIN: '  https://oakdalepark.xyz/\n'
		});

		expect(ORIGIN).toBe('https://oakdalepark.xyz');
		expect(`${requireOrigin()}/login/verify`).toBe('https://oakdalepark.xyz/login/verify');
	});

	it('still reports the dirty value so the variable gets fixed', async () => {
		const { describeMailConfig } = await loadEnv({
			...CONFIGURED,
			ORIGIN: 'https://oakdalepark.xyz\n'
		});
		expect(describeMailConfig().problems.join()).toContain('whitespace');
	});

	it('reports originOk false when production points elsewhere', async () => {
		const { describeMailConfig } = await loadEnv({
			...CONFIGURED,
			ORIGIN: 'https://the-league-abc123.vercel.app'
		});
		expect(describeMailConfig().originOk).toBe(false);
	});

	it('flags a provider selected without a from address', async () => {
		// Defect 2. EMAIL_FROM_NAME defaults but EMAIL_FROM does not, so the
		// header became "The League <undefined>" and every message was rejected
		// with nothing but a per-attempt log line to show for it.
		const { describeMailConfig } = await loadEnv({ ...CONFIGURED, EMAIL_FROM: undefined });
		const { ok, problems } = describeMailConfig();

		expect(ok).toBe(false);
		expect(problems.join()).toContain('EMAIL_FROM');
	});

	it('flags a provider selected without its API key', async () => {
		// Worse than no provider: the service quietly falls back to console mode,
		// so sends land in a serverless log and everything reports success.
		const { describeMailConfig } = await loadEnv({ ...CONFIGURED, RESEND_API_KEY: undefined });
		expect(describeMailConfig().problems.join()).toContain('RESEND_API_KEY');
	});

	it('flags console mode in production', async () => {
		const { describeMailConfig } = await loadEnv({ ...CONFIGURED, EMAIL_PROVIDER: 'console' });
		expect(describeMailConfig().problems.join()).toContain('console');
	});

	it('stays quiet for console mode in development', async () => {
		// Why the banner is invisible on a dev machine: console mode is the
		// documented way to test sign-in locally, so it is not a problem there.
		const { describeMailConfig } = await loadEnv({
			NODE_ENV: 'development',
			EMAIL_PROVIDER: 'console'
		});
		expect(describeMailConfig().ok).toBe(true);
	});

	it('does not demand EMAIL_FROM in console mode', async () => {
		const { describeMailConfig } = await loadEnv({
			NODE_ENV: 'development',
			EMAIL_PROVIDER: 'console',
			EMAIL_FROM: undefined
		});
		expect(describeMailConfig().ok).toBe(true);
	});
});

describe('requireEmailFrom', () => {
	it('throws when a provider is configured without an address', async () => {
		const { requireEmailFrom } = await loadEnv({ ...CONFIGURED, EMAIL_FROM: undefined });
		expect(() => requireEmailFrom()).toThrow(/EMAIL_FROM/);
	});

	it('throws in development too, unlike requireOrigin', async () => {
		// Gated on the provider rather than on isProduction: EMAIL_FROM has no
		// correct default in any environment, and the developer pointing a real
		// key at a local run is exactly who needs the named failure.
		const { requireEmailFrom } = await loadEnv({
			NODE_ENV: 'development',
			EMAIL_PROVIDER: 'resend',
			RESEND_API_KEY: 'key'
		});
		expect(() => requireEmailFrom()).toThrow(/EMAIL_FROM/);
	});

	it('stays silent in console mode', async () => {
		const { requireEmailFrom } = await loadEnv({
			NODE_ENV: 'development',
			EMAIL_PROVIDER: 'console'
		});
		expect(requireEmailFrom()).toBe('');
	});
});

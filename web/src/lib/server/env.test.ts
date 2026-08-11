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
	ORIGIN: 'https://league.example',
	DATABASE_URL: 'postgres://x'
};

describe('describeMailConfig', () => {
	it('reports nothing when everything is set', async () => {
		const { describeMailConfig } = await loadEnv(CONFIGURED);
		expect(describeMailConfig()).toEqual({ ok: true, problems: [] });
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

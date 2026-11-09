import { defineConfig } from '@playwright/test';

// PLAYWRIGHT_BASE_URL points the suite at a server that's already running (the
// dev server, say). Without it, Playwright builds and previews the app itself.
const baseURL = process.env.PLAYWRIGHT_BASE_URL;

export default defineConfig({
	webServer: baseURL
		? undefined
		: {
				command: 'npm run build && npm run preview',
				port: 4173
			},
	use: baseURL ? { baseURL } : undefined,
	testDir: 'e2e'
});

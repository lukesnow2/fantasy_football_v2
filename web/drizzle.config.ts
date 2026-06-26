import { defineConfig } from 'drizzle-kit';

// @ts-ignore
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set');

// @ts-ignore
const isLocalDb = /@(localhost|127\.0\.0\.1)[:/]/.test(process.env.DATABASE_URL);

export default defineConfig({
	schema: './src/lib/server/db/schema.ts',
	dialect: 'postgresql',
	// Only manage the `app` schema. The `edw` and `public` schemas are owned by the
	// Python data pipeline — without this filter, drizzle-kit drops the pipeline's
	// public.* raw tables (it defaults to managing only `public`).
	schemaFilter: ['app'],
	dbCredentials: {
		// @ts-ignore
		url: process.env.DATABASE_URL,
		ssl: isLocalDb ? false : 'require'
	},
	verbose: true,
	strict: true
});

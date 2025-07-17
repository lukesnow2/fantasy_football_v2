import { defineConfig } from 'drizzle-kit';

// @ts-ignore
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set');

export default defineConfig({
	schema: './src/lib/server/db/schema.ts',
	dialect: 'postgresql',
	dbCredentials: { 
		// @ts-ignore
		url: process.env.DATABASE_URL
	},
	verbose: true,
	strict: true
});

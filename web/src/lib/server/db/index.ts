import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema';
import { env } from '$env/dynamic/private';

if (!env.DATABASE_URL) throw new Error('DATABASE_URL is not set');

// Local Postgres (dev) does not speak SSL; remote (Neon/prod) requires it.
const isLocalDb = /@(localhost|127\.0\.0\.1)[:/]/.test(env.DATABASE_URL);

// Configure client for remote PostgreSQL with improved resilience for serverless
const client = postgres(env.DATABASE_URL, {
	ssl: isLocalDb ? false : 'require', // Force SSL for remote databases; disable for local dev
	max: 1, // Use only 1 connection for serverless
	idle_timeout: 20, // Close idle connections after 20 seconds  
	connect_timeout: 30, // 30 second connection timeout
	prepare: false, // Disable prepared statements for serverless
	onnotice: () => {}, // Suppress notices
	connection: {
		search_path: 'app,edw,public' // Search app schema first, then edw, then public
	},
	// Add retry logic for connection issues
	max_lifetime: 60 * 30, // 30 minutes
	transform: postgres.camel, // Transform to camelCase
});

export const db = drizzle(client, { schema });

export type Database = typeof db;

/**
 * Either the pooled handle or an open transaction.
 *
 * Functions that must be able to join a caller's transaction take this instead
 * of importing `db` directly — consuming a login token and creating the session
 * it authorises have to commit or roll back together, or a crash between them
 * burns the token without logging anyone in.
 */
export type Tx = Database | Parameters<Parameters<Database['transaction']>[0]>[0];

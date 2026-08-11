import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema';
import { env } from '$env/dynamic/private';

/**
 * Connect lazily, on first query rather than at import.
 *
 * Opening the connection at module load meant a missing DATABASE_URL threw
 * during `vite build` — SvelteKit evaluates server modules while analysing
 * routes, so the *build* needed a live database URL. That breaks any deployment
 * where the variable is scoped to production only, and it is wrong on its own
 * terms: compiling the app should not require a database.
 *
 * The error still fires, just at the first query, where it is actionable.
 */
let instance: ReturnType<typeof drizzle> | null = null;

function connect() {
	if (instance) return instance;

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
		transform: postgres.camel // Transform to camelCase
	});

	instance = drizzle(client, { schema });
	return instance;
}

/**
 * Proxied so `db.select()` and `db.transaction()` keep working unchanged at
 * every call site while the connection itself is deferred.
 */
export const db = new Proxy({} as ReturnType<typeof drizzle>, {
	get(_target, prop, receiver) {
		return Reflect.get(connect(), prop, receiver);
	}
});

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

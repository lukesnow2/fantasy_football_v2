import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema';
import { env } from '$env/dynamic/private';

if (!env.DATABASE_URL) throw new Error('DATABASE_URL is not set');

// Configure client for remote PostgreSQL with improved resilience for serverless
const client = postgres(env.DATABASE_URL, {
	ssl: 'require', // Force SSL for remote databases
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

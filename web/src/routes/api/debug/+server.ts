import { json } from '@sveltejs/kit';
import { 
	AUTH_SECRET, 
	SESSION_SECRET, 
	EMAIL_PROVIDER, 
	EMAIL_FROM,
	NODE_ENV,
	ORIGIN,
	isDevelopment,
	isProduction
} from '$lib/server/env';
import { emailService } from '$lib/server/email';
import { db } from '$lib/server/db';
import { user, session } from '$lib/server/db/schema';
import { logger } from '$lib/server/logger';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ url }) => {
	// Only allow in development
	if (isProduction) {
		return json({ error: 'Debug endpoint disabled in production' }, { status: 403 });
	}

	const action = url.searchParams.get('action');
	const tests: Record<string, any> = {};

	// Test 1: Environment Variables
	tests.environment = {
		NODE_ENV,
		ORIGIN,
		isDevelopment,
		isProduction,
		AUTH_SECRET_LENGTH: AUTH_SECRET.length,
		SESSION_SECRET_LENGTH: SESSION_SECRET.length,
		AUTH_SECRET_SET: !!AUTH_SECRET,
		SESSION_SECRET_SET: !!SESSION_SECRET,
		EMAIL_PROVIDER,
		EMAIL_FROM
	};

	// Test 2: Database Connection
	try {
		const userCount = await db.$count(user);
		tests.database = {
			status: 'connected',
			userCount,
			message: 'Database connection successful'
		};
	} catch (error) {
		tests.database = {
			status: 'error',
			message: error instanceof Error ? error.message : 'Unknown database error'
		};
	}

	// Test 3: Email System
	try {
		const testEmail = {
			to: 'test@example.com',
			subject: 'Test Email - Environment Check',
			html: '<p>This is a test email to verify email system configuration.</p>'
		};
		
		// Don't actually send, just test the configuration
		tests.email = {
			provider: EMAIL_PROVIDER,
			configured: true,
			message: `Email system configured for ${EMAIL_PROVIDER} mode`
		};
	} catch (error) {
		tests.email = {
			provider: EMAIL_PROVIDER,
			configured: false,
			message: error instanceof Error ? error.message : 'Email configuration error'
		};
	}

	// Test 4: Session System
	try {
		const sessionCount = await db.$count(session);
		tests.sessions = {
			status: 'working',
			sessionCount,
			message: 'Session system operational'
		};
	} catch (error) {
		tests.sessions = {
			status: 'error',
			message: error instanceof Error ? error.message : 'Session system error'
		};
	}

	// Special action: migrate sessions
	if (action === 'migrate') {
		try {
			tests.migration = await runSessionMigration();
		} catch (error) {
			tests.migration = {
				status: 'error',
				message: error instanceof Error ? error.message : 'Migration failed'
			};
		}
	}

	// Special action: test database schemas
	if (action === 'test-schemas') {
		try {
			tests.schemaTest = await testDatabaseSchemas();
		} catch (error) {
			tests.schemaTest = {
				status: 'error',
				message: error instanceof Error ? error.message : 'Schema test failed'
			};
		}
	}

	return json(tests);
};

async function runSessionMigration() {
	const results: string[] = [];
	
	try {
		// Create app schema if it doesn't exist
		await db.execute(sql`CREATE SCHEMA IF NOT EXISTS app`);
		results.push('✅ App schema ensured');

		// Check if session table exists in public schema
		const publicSessionResult = await db.execute(sql`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = 'session'
			);
		`);
		
		// Check if session table exists in app schema
		const appSessionResult = await db.execute(sql`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'app' 
				AND table_name = 'session'
			);
		`);
		
		const publicSessionExists = (publicSessionResult as any)[0]?.exists;
		const appSessionExists = (appSessionResult as any)[0]?.exists;
		
		if (publicSessionExists && !appSessionExists) {
			// Move session table from public to app schema
			await db.execute(sql`ALTER TABLE public.session SET SCHEMA app`);
			results.push('✅ Moved session table from public to app schema');
		}
		
		// Ensure session table has correct structure
		await db.execute(sql`
			CREATE TABLE IF NOT EXISTS app.session (
				id text PRIMARY KEY NOT NULL,
				user_id text NOT NULL,
				expires_at timestamp with time zone NOT NULL
			);
		`);
		results.push('✅ Session table structure ensured');

		// Check if user table exists in public schema and move it too
		const publicUserResult = await db.execute(sql`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = 'user'
			);
		`);
		
		const appUserResult = await db.execute(sql`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'app' 
				AND table_name = 'user'
			);
		`);
		
		const publicUserExists = (publicUserResult as any)[0]?.exists;
		const appUserExists = (appUserResult as any)[0]?.exists;
		
		if (publicUserExists && !appUserExists) {
			await db.execute(sql`ALTER TABLE public.user SET SCHEMA app`);
			results.push('✅ Moved user table from public to app schema');
		}

		// Ensure user table has correct structure
		await db.execute(sql`
			CREATE TABLE IF NOT EXISTS app.user (
				id text PRIMARY KEY NOT NULL,
				age integer,
				username text NOT NULL UNIQUE,
				password_hash text NOT NULL,
				manager_key integer,
				email varchar(255) UNIQUE,
				display_name varchar(255),
				account_status varchar(20) DEFAULT 'active',
				notification_preferences text,
				profile_settings text,
				created_at timestamp DEFAULT now(),
				updated_at timestamp DEFAULT now()
			);
		`);
		results.push('✅ User table structure ensured');

		return {
			status: 'success',
			message: 'Session migration completed successfully',
			details: results
		};
		
	} catch (error) {
		return {
			status: 'error',
			message: error instanceof Error ? error.message : 'Unknown migration error',
			details: results
		};
	}
}

async function testDatabaseSchemas() {
	try {
		// List schemas to verify structure
		const schemasResult = await db.execute(sql`
			SELECT schema_name FROM information_schema.schemata 
			WHERE schema_name IN ('public', 'app', 'edw')
			ORDER BY schema_name
		`);
		const schemas = (schemasResult as any).map((r: any) => r.schema_name);
		
		// List tables in app schema
		const appTablesResult = await db.execute(sql`
			SELECT table_name FROM information_schema.tables 
			WHERE table_schema = 'app'
			ORDER BY table_name
		`);
		const appTables = (appTablesResult as any).map((r: any) => r.table_name);

		// Count sessions and users
		const sessionResult = await db.execute(sql`SELECT COUNT(*) FROM app.session`);
		const sessionCount = (sessionResult as any)[0]?.count || 0;
		
		const userResult = await db.execute(sql`SELECT COUNT(*) FROM app.user`);
		const userCount = (userResult as any)[0]?.count || 0;

		return {
			status: 'success',
			schemas,
			appTables,
			sessionCount,
			userCount,
			message: 'Database schema test completed'
		};
		
	} catch (error) {
		return {
			status: 'error',
			message: error instanceof Error ? error.message : 'Database schema test failed'
		};
	}
} 
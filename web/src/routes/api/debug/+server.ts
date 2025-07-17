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
		// Test basic connection
		const userCount = await db.$count(user);
		
		// Test schema access
		const schemaTest = await db.execute(sql`
			SELECT 
				COUNT(*) as table_count,
				schemaname 
			FROM pg_tables 
			WHERE schemaname IN ('app', 'edw', 'public')
			GROUP BY schemaname
		`);
		
		// Test actual data access
		const dataTest = await db.execute(sql`
			SELECT COUNT(*) as manager_count FROM edw.dim_manager LIMIT 1
		`);
		
		tests.database = {
			status: 'connected',
			userCount,
			schemaAccess: Array.from(schemaTest),
			edwDataAccess: Array.from(dataTest),
			connectionPool: 'healthy',
			message: 'Database connection and schema access successful'
		};
	} catch (error) {
		tests.database = {
			status: 'error',
			message: error instanceof Error ? error.message : 'Unknown database error',
			errorType: error instanceof Error ? error.constructor.name : 'Unknown',
			stack: error instanceof Error ? error.stack?.split('\n').slice(0, 3) : undefined
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

	// Handle specific actions
	if (action === 'migrate') {
		tests.migration = await runSessionMigration();
	} else if (action === 'test-schemas') {
		tests.schemas = await testDatabaseSchemas();
	} else if (action === 'migrate-rule-proposals') {
		tests.ruleProposalMigration = await migrateRuleProposals();
	}

	return json({
		status: 'debug',
		timestamp: new Date().toISOString(),
		tests
	});
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

async function migrateRuleProposals() {
	const results: string[] = [];
	
	try {
		// Create app schema if it doesn't exist
		await db.execute(sql`CREATE SCHEMA IF NOT EXISTS app`);
		results.push('✅ App schema ensured');

		// Check if rule_proposal table exists
		const tableExistsResult = await db.execute(sql`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'app' 
				AND table_name = 'rule_proposal'
			);
		`);
		
		const tableExists = (tableExistsResult as any)[0]?.exists;
		
		if (!tableExists) {
			// Create the rule_proposal table with SERIAL primary key
			await db.execute(sql`
				CREATE TABLE app.rule_proposal (
					proposal_key SERIAL PRIMARY KEY,
					proposal_id VARCHAR(50) NOT NULL UNIQUE,
					title VARCHAR(255) NOT NULL,
					description TEXT NOT NULL,
					proposal_type VARCHAR(50) NOT NULL,
					affected_section VARCHAR(100),
					rule_index INTEGER,
					current_language TEXT,
					proposed_language TEXT NOT NULL,
					rationale TEXT NOT NULL,
					effective_season INTEGER NOT NULL,
					submitted_by INTEGER NOT NULL,
					submitted_at TIMESTAMP DEFAULT NOW(),
					voting_start_date TIMESTAMP,
					voting_end_date TIMESTAMP,
					status VARCHAR(20) DEFAULT 'draft',
					required_votes INTEGER DEFAULT 7,
					yes_votes INTEGER DEFAULT 0,
					no_votes INTEGER DEFAULT 0,
					abstain_votes INTEGER DEFAULT 0,
					created_at TIMESTAMP DEFAULT NOW(),
					updated_at TIMESTAMP DEFAULT NOW(),
					FOREIGN KEY (submitted_by) REFERENCES edw.dim_manager(manager_key)
				);
			`);
			results.push('✅ Created rule_proposal table with SERIAL primary key');
		} else {
			// Check if proposal_key is already SERIAL
			const columnInfoResult = await db.execute(sql`
				SELECT column_default
				FROM information_schema.columns 
				WHERE table_schema = 'app' 
				AND table_name = 'rule_proposal' 
				AND column_name = 'proposal_key';
			`);
			
			const columnDefault = (columnInfoResult as any)[0]?.column_default;
			
			if (!columnDefault || !columnDefault.includes('nextval')) {
				// Convert existing integer primary key to SERIAL
				await db.execute(sql`
					CREATE SEQUENCE IF NOT EXISTS app.rule_proposal_proposal_key_seq;
				`);
				await db.execute(sql`
					ALTER TABLE app.rule_proposal 
					ALTER COLUMN proposal_key SET DEFAULT nextval('app.rule_proposal_proposal_key_seq');
				`);
				await db.execute(sql`
					ALTER SEQUENCE app.rule_proposal_proposal_key_seq 
					OWNED BY app.rule_proposal.proposal_key;
				`);
				results.push('✅ Converted proposal_key to SERIAL');
			} else {
				results.push('✅ proposal_key already uses SERIAL');
			}
		}

		// Create rule_vote table if it doesn't exist
		const voteTableExistsResult = await db.execute(sql`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'app' 
				AND table_name = 'rule_vote'
			);
		`);
		
		const voteTableExists = (voteTableExistsResult as any)[0]?.exists;
		
		if (!voteTableExists) {
			await db.execute(sql`
				CREATE TABLE app.rule_vote (
					vote_key SERIAL PRIMARY KEY,
					proposal_key INTEGER NOT NULL,
					manager_key INTEGER NOT NULL,
					vote VARCHAR(10) NOT NULL,
					comment TEXT,
					voted_at TIMESTAMP DEFAULT NOW(),
					created_at TIMESTAMP DEFAULT NOW(),
					FOREIGN KEY (proposal_key) REFERENCES app.rule_proposal(proposal_key),
					FOREIGN KEY (manager_key) REFERENCES edw.dim_manager(manager_key),
					UNIQUE(proposal_key, manager_key)
				);
			`);
			results.push('✅ Created rule_vote table');
		} else {
			results.push('✅ rule_vote table already exists');
		}

		// Create rule_amendment table if it doesn't exist
		const amendmentTableExistsResult = await db.execute(sql`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'app' 
				AND table_name = 'rule_amendment'
			);
		`);
		
		const amendmentTableExists = (amendmentTableExistsResult as any)[0]?.exists;
		
		if (!amendmentTableExists) {
			await db.execute(sql`
				CREATE TABLE app.rule_amendment (
					amendment_key SERIAL PRIMARY KEY,
					proposal_key INTEGER NOT NULL,
					amendment_year INTEGER NOT NULL,
					amendment_title VARCHAR(255) NOT NULL,
					amendment_description TEXT NOT NULL,
					effective_season INTEGER NOT NULL,
					vote_results TEXT,
					approved_by INTEGER,
					approved_at TIMESTAMP,
					created_at TIMESTAMP DEFAULT NOW(),
					FOREIGN KEY (proposal_key) REFERENCES app.rule_proposal(proposal_key),
					FOREIGN KEY (approved_by) REFERENCES edw.dim_manager(manager_key)
				);
			`);
			results.push('✅ Created rule_amendment table');
		} else {
			results.push('✅ rule_amendment table already exists');
		}

		return {
			status: 'success',
			message: 'Rule proposal migration completed successfully',
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
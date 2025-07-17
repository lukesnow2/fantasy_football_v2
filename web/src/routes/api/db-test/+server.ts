import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	const tests = {
		connection: 'pending',
		basicQuery: 'pending',
		schemaAccess: 'pending',
		tableAccess: 'pending',
		permissions: 'pending'
	};

	try {
		// Test 1: Basic connection
		await db.execute(sql`SELECT 1`);
		tests.connection = 'success';

		// Test 2: Database info
		const dbInfo = await db.execute(sql`
			SELECT 
				current_database() as database_name,
				current_user as current_user,
				version() as db_version
		`);
		tests.basicQuery = { status: 'success', data: Array.from(dbInfo) };

		// Test 3: Schema access 
		const schemas = await db.execute(sql`
			SELECT schema_name 
			FROM information_schema.schemata 
			WHERE schema_name IN ('edw', 'app', 'public')
		`);
		tests.schemaAccess = { status: 'success', schemas: Array.from(schemas) };

		// Test 4: Table existence check
		const tables = await db.execute(sql`
			SELECT table_schema, table_name 
			FROM information_schema.tables 
			WHERE table_schema IN ('edw', 'app') 
			ORDER BY table_schema, table_name
		`);
		tests.tableAccess = { status: 'success', tables: Array.from(tables) };

		// Test 5: Permissions check
		const permissions = await db.execute(sql`
			SELECT 
				table_schema,
				table_name,
				privilege_type
			FROM information_schema.table_privileges 
			WHERE grantee = current_user 
			AND table_schema IN ('edw', 'app')
			LIMIT 10
		`);
		tests.permissions = { status: 'success', permissions: Array.from(permissions) };

		return json({
			status: 'success',
			tests,
			timestamp: new Date().toISOString(),
			environment: 'vercel-production'
		});

	} catch (error) {
		return json({
			status: 'error',
			tests,
			error: {
				message: error instanceof Error ? error.message : String(error),
				code: (error as any)?.code,
				detail: (error as any)?.detail,
				hint: (error as any)?.hint
			},
			timestamp: new Date().toISOString(),
			environment: 'vercel-production'
		}, { status: 500 });
	}
}; 
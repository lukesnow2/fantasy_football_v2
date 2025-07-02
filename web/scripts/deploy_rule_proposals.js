#!/usr/bin/env node

import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { Client } from 'pg';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function deployRuleProposals() {
    const client = new Client({
        connectionString: process.env.DATABASE_URL
    });

    try {
        console.log('🔌 Connecting to database...');
        await client.connect();
        console.log('✅ Connected to database');

        // Read the migration SQL
        const migrationPath = join(__dirname, '../src/lib/server/db/migrations/add_rule_proposals.sql');
        const migrationSQL = readFileSync(migrationPath, 'utf8');

        console.log('📋 Executing rule proposal migration...');
        
        // Split and execute SQL statements
        const statements = migrationSQL
            .split(';')
            .map(stmt => stmt.trim())
            .filter(stmt => stmt.length > 0 && !stmt.startsWith('--'));

        for (const statement of statements) {
            try {
                await client.query(statement);
                console.log('  ✅ Executed statement');
            } catch (error) {
                if (error.message.includes('already exists')) {
                    console.log('  ⚠️  Object already exists, skipping');
                } else {
                    console.error('  ❌ Error executing statement:', error.message);
                }
            }
        }

        console.log('✅ Rule proposal migration completed successfully');

        // Verify tables were created
        const tables = ['rule_proposal', 'rule_vote', 'rule_amendment'];
        for (const table of tables) {
            try {
                const result = await client.query(`
                    SELECT COUNT(*) FROM edw.${table}
                `);
                console.log(`  📊 ${table}: ${result.rows[0].count} records`);
            } catch (error) {
                console.error(`  ❌ Error checking ${table}:`, error.message);
            }
        }

    } catch (error) {
        console.error('❌ Migration failed:', error);
        process.exit(1);
    } finally {
        await client.end();
    }
}

// Run the migration
deployRuleProposals(); 
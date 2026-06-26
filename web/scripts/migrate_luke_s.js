#!/usr/bin/env node

/**
 * Migration Script: Enable Passkey Authentication for Luke S
 * 
 * This script migrates Luke S from password-only to hybrid authentication
 * (password + passkey), allowing him to set up his passkey while keeping
 * password as backup.
 */

import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { eq } from 'drizzle-orm';

// Database connection
const connectionString = process.env.DATABASE_URL
const client = postgres(connectionString);
const db = drizzle(client);

async function migrateLukeS() {
    console.log('🔧 Starting Luke S migration to passkey authentication...');
    
    try {
        // Step 1: Verify Luke S's current account
        const lukeSUser = await db
            .select({
                id: 'id',
                username: 'username',
                managerKey: 'manager_key',
                passkeyEnabled: 'passkey_enabled',
                accountStatus: 'account_status'
            })
            .from('app.user')
            .where(eq('username', 'linkin22luke'))
            .limit(1);

        if (lukeSUser.length === 0) {
            throw new Error('Luke S user account not found');
        }

        const luke = lukeSUser[0];
        console.log('✅ Found Luke S account:', {
            id: luke.id,
            username: luke.username,
            managerKey: luke.managerKey,
            passkeyEnabled: luke.passkeyEnabled,
            accountStatus: luke.accountStatus
        });

        // Step 2: Enable passkey authentication (hybrid mode)
        await db
            .update('app.user')
            .set({
                passkey_enabled: true,
                passkey_registered_at: new Date(),
                updated_at: new Date()
            })
            .where(eq('id', luke.id));

        console.log('✅ Successfully enabled passkey authentication for Luke S');
        console.log('📝 Migration complete! Luke S can now:');
        console.log('   - Continue using password authentication');
        console.log('   - Set up passkey authentication via login page');
        console.log('   - Use either method to sign in');
        
    } catch (error) {
        console.error('❌ Migration failed:', error);
        process.exit(1);
    } finally {
        await client.end();
    }
}

// Run migration
migrateLukeS().catch(console.error); 
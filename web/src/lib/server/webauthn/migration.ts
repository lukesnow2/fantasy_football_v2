import { db } from '$lib/server/db';
import { user } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';
import { WebAuthnService } from './service';

export interface MigrationUser {
  id: string;
  username: string;
  email: string | null;
  managerKey: number | null;
  passkeyEnabled: boolean | null;
  passkeyRegisteredAt: Date | null;
}

export interface MigrationResult {
  userId: string;
  success: boolean;
  message: string;
  requiresSetup?: boolean;
}

export class WebAuthnMigration {
  private webauthnService: WebAuthnService;

  constructor() {
    this.webauthnService = new WebAuthnService();
  }

  async migrateExistingUsers(): Promise<MigrationResult[]> {
    console.log('Starting WebAuthn migration for existing users');
    
    try {
      // Get all existing users
      const existingUsers = await db.select({
        id: user.id,
        username: user.username,
        email: user.email,
        managerKey: user.managerKey,
        passkeyEnabled: user.passkeyEnabled,
        passkeyRegisteredAt: user.passkeyRegisteredAt
      }).from(user);

      const results: MigrationResult[] = [];

      for (const existingUser of existingUsers) {
        const result = await this.migrateUser(existingUser);
        results.push(result);
      }

      console.log(`Migration completed. ${results.length} users processed`);
      return results;

    } catch (error) {
      console.error('Migration failed:', error instanceof Error ? error.message : 'Unknown error');
      throw new Error('User migration failed');
    }
  }

  private async migrateUser(userData: MigrationUser): Promise<MigrationResult> {
    try {
      // Preserve existing manager key mapping
      if (userData.managerKey) {
        console.log(`Preserving manager key for user ${userData.id}`);
      }

      // Check if user already has passkeys
      if (userData.passkeyEnabled && userData.passkeyRegisteredAt) {
        return {
          userId: userData.id,
          success: true,
          message: 'User already has passkeys enabled',
          requiresSetup: false
        };
      }

      // Mark user for first-time setup
      await db.update(user)
        .set({
          passkeyEnabled: false,
          passkeyRegisteredAt: null,
          // Preserve manager key mapping
          managerKey: userData.managerKey || null
        })
        .where(eq(user.id, userData.id));

      return {
        userId: userData.id,
        success: true,
        message: 'User marked for passkey setup',
        requiresSetup: true
      };

    } catch (error) {
      console.error(`Failed to migrate user ${userData.id}:`, error instanceof Error ? error.message : 'Unknown error');
      return {
        userId: userData.id,
        success: false,
        message: error instanceof Error ? error.message : 'Migration failed'
      };
    }
  }

  async getMigrationStats(): Promise<{
    totalUsers: number;
    migratedUsers: number;
    pendingSetup: number;
    failedMigrations: number;
  }> {
    try {
      const users = await db.select({
        passkeyEnabled: user.passkeyEnabled,
        passkeyRegisteredAt: user.passkeyRegisteredAt
      }).from(user);

      const totalUsers = users.length;
      const migratedUsers = users.filter(u => u.passkeyEnabled && u.passkeyRegisteredAt).length;
      const pendingSetup = users.filter(u => !u.passkeyEnabled).length;
      const failedMigrations = totalUsers - migratedUsers - pendingSetup;

      return {
        totalUsers,
        migratedUsers,
        pendingSetup,
        failedMigrations
      };

    } catch (error) {
      console.error('Failed to get migration stats:', error instanceof Error ? error.message : 'Unknown error');
      throw new Error('Failed to get migration statistics');
    }
  }
} 
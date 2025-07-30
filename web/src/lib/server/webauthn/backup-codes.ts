import { db } from '$lib/server/db';
import { backupCodes } from '$lib/server/db/schema';
import { eq } from 'drizzle-orm';
import { randomBytes, createHash } from 'crypto';

export interface BackupCode {
  id: string;
  codeHash: string;
  used: boolean;
  usedAt?: Date;
  createdAt: Date;
}

export class BackupCodesManager {
  private static readonly CODE_LENGTH = 10;
  private static readonly CODE_COUNT = 8;
  private static readonly CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

  static generateSecureCodes(count: number = this.CODE_COUNT, length: number = this.CODE_LENGTH): string[] {
    const codes: string[] = [];
    
    for (let i = 0; i < count; i++) {
      let code = '';
      for (let j = 0; j < length; j++) {
        code += this.CHARS.charAt(Math.floor(Math.random() * this.CHARS.length));
      }
      codes.push(code);
    }
    
    return codes;
  }

  static hashCode(code: string): string {
    return createHash('sha256').update(code).digest('hex');
  }

  async generateBackupCodes(userId: string): Promise<string[]> {
    try {
      // Clear existing backup codes for this user
      await db.delete(backupCodes).where(eq(backupCodes.userId, userId));
      
      // Generate new codes
      const codes = BackupCodesManager.generateSecureCodes();
      
      // Insert hashed codes into database
      for (const code of codes) {
        const codeHash = BackupCodesManager.hashCode(code);
        await db.insert(backupCodes).values({
          id: randomBytes(16).toString('hex'),
          userId,
          codeHash,
          used: false
        });
      }

      console.log(`Generated ${codes.length} backup codes for user ${userId}`);
      return codes;

    } catch (error) {
      console.error(`Failed to generate backup codes for user ${userId}:`, error);
      throw new Error('Failed to generate backup codes');
    }
  }

  async verifyBackupCode(userId: string, code: string): Promise<boolean> {
    try {
      const codeHash = BackupCodesManager.hashCode(code);
      
      const backupCode = await db.select().from(backupCodes).where(eq(backupCodes.userId, userId));

      const matchingCode = backupCode.find(code => 
        code.codeHash === codeHash && !code.used
      );

      if (!matchingCode) {
        return false;
      }

      // Mark code as used
      await db.update(backupCodes)
        .set({ 
          used: true,
          usedAt: new Date()
        })
        .where(eq(backupCodes.id, matchingCode.id));

      console.log(`Backup code used for user ${userId}`);
      return true;

    } catch (error) {
      console.error(`Failed to verify backup code for user ${userId}:`, error);
      return false;
    }
  }

  async getBackupCodesStatus(userId: string): Promise<{
    totalCodes: number;
    usedCodes: number;
    availableCodes: number;
    hasCodes: boolean;
  }> {
    try {
      const userCodes = await db.select({
        used: backupCodes.used
      }).from(backupCodes).where(eq(backupCodes.userId, userId));

      const totalCodes = userCodes.length;
      const usedCodes = userCodes.filter(code => code.used).length;
      const availableCodes = totalCodes - usedCodes;

      return {
        totalCodes,
        usedCodes,
        availableCodes,
        hasCodes: totalCodes > 0
      };

    } catch (error) {
      console.error(`Failed to get backup codes status for user ${userId}:`, error);
      return {
        totalCodes: 0,
        usedCodes: 0,
        availableCodes: 0,
        hasCodes: false
      };
    }
  }

  async regenerateBackupCodes(userId: string): Promise<string[]> {
    try {
      // Verify user has passkeys enabled
      const { user } = await import('$lib/server/db/schema');
      const userData = await db.select({
        passkeyEnabled: user.passkeyEnabled
      }).from(user).where(eq(user.id, userId)).limit(1);

      if (!userData.length || !userData[0].passkeyEnabled) {
        throw new Error('User must have passkeys enabled to regenerate backup codes');
      }

      return await this.generateBackupCodes(userId);

    } catch (error) {
      console.error(`Failed to regenerate backup codes for user ${userId}:`, error);
      throw error;
    }
  }

  async deleteBackupCodes(userId: string): Promise<void> {
    try {
      await db.delete(backupCodes).where(eq(backupCodes.userId, userId));
      console.log(`Deleted backup codes for user ${userId}`);

    } catch (error) {
      console.error(`Failed to delete backup codes for user ${userId}:`, error);
      throw new Error('Failed to delete backup codes');
    }
  }
} 
import { db } from '$lib/server/db';
import { webauthnCredentials } from '$lib/server/db/schema';
import { eq, and, isNull } from 'drizzle-orm';
import { randomBytes } from 'crypto';

export interface Credential {
  id: string;
  userId: string;
  credentialId: string;
  publicKey: string;
  signCount: number;
  transports?: string[] | null;
  backupEligible: boolean;
  backupState: boolean;
  createdAt: Date | null;
  lastUsedAt?: Date | null;
  deviceType?: string | null;
  authenticatorType?: string | null;
}

export class CredentialManager {
  static async create(credential: Omit<Credential, 'id' | 'createdAt'>): Promise<Credential> {
    const id = randomBytes(16).toString('hex');
    const now = new Date();
    
    const credentialRecord = {
      ...credential,
      id,
      createdAt: now
    };

    await db.insert(webauthnCredentials).values(credentialRecord);
    
    return credentialRecord;
  }

  static async getByCredentialId(credentialId: string): Promise<Credential | null> {
    const [credential] = await db
      .select()
      .from(webauthnCredentials)
      .where(eq(webauthnCredentials.credentialId, credentialId))
      .limit(1);
    
    return credential || null;
  }

  static async getByUserId(userId: string): Promise<Credential[]> {
    return await db
      .select()
      .from(webauthnCredentials)
      .where(eq(webauthnCredentials.userId, userId));
  }

  static async updateSignCount(credentialId: string, signCount: number): Promise<void> {
    await db
      .update(webauthnCredentials)
      .set({ 
        signCount, 
        lastUsedAt: new Date() 
      })
      .where(eq(webauthnCredentials.credentialId, credentialId));
  }

  static async delete(id: string): Promise<void> {
    await db
      .delete(webauthnCredentials)
      .where(eq(webauthnCredentials.id, id));
  }

  static async getActiveCredentials(userId: string): Promise<Credential[]> {
    return await db
      .select()
      .from(webauthnCredentials)
      .where(eq(webauthnCredentials.userId, userId));
  }

  static async validateCredential(credentialId: string, expectedSignCount: number): Promise<boolean> {
    const credential = await this.getByCredentialId(credentialId);
    if (!credential) return false;
    
    // Prevent replay attacks
    if (credential.signCount >= expectedSignCount) {
      return false;
    }
    
    return true;
  }
} 
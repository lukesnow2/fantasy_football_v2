import { db } from '$lib/server/db';
import { webauthnChallenges } from '$lib/server/db/schema';
import { eq, lt } from 'drizzle-orm';
import { randomBytes } from 'crypto';

export interface Challenge {
  id: string;
  challenge: string;
  userId: string | null;
  type: 'registration' | 'authentication';
  expiresAt: Date;
}

export class ChallengeManager {
  static async create(
    userId: string | undefined,
    type: 'registration' | 'authentication',
    timeoutMs: number = 60000
  ): Promise<Challenge> {
    const challenge = randomBytes(32).toString('base64url');
    const expiresAt = new Date(Date.now() + timeoutMs);
    
    const challengeRecord = {
      id: randomBytes(16).toString('hex'),
      challenge,
      userId: userId || null,
      type,
      expiresAt
    };

    await db.insert(webauthnChallenges).values(challengeRecord);
    
          return {
        id: challengeRecord.id,
        challenge: challengeRecord.challenge,
        userId: challengeRecord.userId,
        type: challengeRecord.type as 'registration' | 'authentication',
        expiresAt: challengeRecord.expiresAt
      };
  }

  static async get(id: string): Promise<Challenge | null> {
    const [challenge] = await db
      .select()
      .from(webauthnChallenges)
      .where(eq(webauthnChallenges.id, id))
      .limit(1);
    
    if (!challenge) return null;
    
    return {
      id: challenge.id,
      challenge: challenge.challenge,
      userId: challenge.userId,
      type: challenge.type as 'registration' | 'authentication',
      expiresAt: challenge.expiresAt
    };
  }

  static async delete(id: string): Promise<void> {
    await db
      .delete(webauthnChallenges)
      .where(eq(webauthnChallenges.id, id));
  }

  static async cleanup(): Promise<number> {
    await db
      .delete(webauthnChallenges)
      .where(lt(webauthnChallenges.expiresAt, new Date()));
    
    return 0; // Drizzle doesn't return row count, but cleanup is successful
  }

  static async validate(id: string): Promise<Challenge | null> {
    const challenge = await this.get(id);
    if (!challenge) return null;
    
    if (challenge.expiresAt < new Date()) {
      await this.delete(id);
      return null;
    }
    
    return challenge;
  }
} 
import { json } from '@sveltejs/kit';
import type { RequestHandler } from '@sveltejs/kit';
import { BackupCodesManager } from '$lib/server/webauthn/backup-codes';

export const POST: RequestHandler = async ({ request }) => {
  try {
    const { userId, code } = await request.json();
    
    if (!userId || !code) {
      return json({
        success: false,
        message: 'User ID and backup code are required'
      }, { status: 400 });
    }

    const manager = new BackupCodesManager();
    const isValid = await manager.verifyBackupCode(userId, code);
    
    if (isValid) {
      return json({
        success: true,
        message: 'Backup code verified successfully',
        verified: true
      });
    } else {
      return json({
        success: false,
        message: 'Invalid or expired backup code',
        verified: false
      }, { status: 401 });
    }

  } catch (error) {
    console.error('Backup code verification error:', error);
    return json({
      success: false,
      message: error instanceof Error ? error.message : 'Failed to verify backup code',
      error: 'Backup code verification failed'
    }, { status: 500 });
  }
}; 
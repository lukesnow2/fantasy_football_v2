import { json } from '@sveltejs/kit';
import type { RequestHandler } from '@sveltejs/kit';
import { BackupCodesManager } from '$lib/server/webauthn/backup-codes';

export const POST: RequestHandler = async ({ request }) => {
  try {
    const { userId } = await request.json();
    
    if (!userId) {
      return json({
        success: false,
        message: 'User ID is required'
      }, { status: 400 });
    }

    const manager = new BackupCodesManager();
    const codes = await manager.generateBackupCodes(userId);
    
    return json({
      success: true,
      message: 'Backup codes generated successfully',
      codes,
      count: codes.length
    });

  } catch (error) {
    console.error('Backup codes generation error:', error);
    return json({
      success: false,
      message: error instanceof Error ? error.message : 'Failed to generate backup codes',
      error: 'Backup codes generation failed'
    }, { status: 500 });
  }
};

export const GET: RequestHandler = async ({ url }) => {
  try {
    const userId = url.searchParams.get('userId');
    
    if (!userId) {
      return json({
        success: false,
        message: 'User ID is required'
      }, { status: 400 });
    }

    const manager = new BackupCodesManager();
    const status = await manager.getBackupCodesStatus(userId);
    
    return json({
      success: true,
      status
    });

  } catch (error) {
    console.error('Backup codes status error:', error);
    return json({
      success: false,
      message: error instanceof Error ? error.message : 'Failed to get backup codes status',
      error: 'Backup codes status failed'
    }, { status: 500 });
  }
}; 
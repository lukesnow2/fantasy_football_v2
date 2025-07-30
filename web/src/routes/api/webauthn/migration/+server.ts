import { json } from '@sveltejs/kit';
import type { RequestHandler } from '@sveltejs/kit';
import { WebAuthnMigration } from '$lib/server/webauthn/migration';

export const POST: RequestHandler = async ({ request }) => {
  try {
    const migration = new WebAuthnMigration();
    const results = await migration.migrateExistingUsers();
    
    return json({
      success: true,
      message: 'Migration completed successfully',
      results,
      totalProcessed: results.length,
      successful: results.filter(r => r.success).length,
      failed: results.filter(r => !r.success).length,
      requiresSetup: results.filter(r => r.requiresSetup).length
    });

  } catch (error) {
    console.error('Migration API error:', error);
    return json({
      success: false,
      message: error instanceof Error ? error.message : 'Migration failed',
      error: 'Migration failed'
    }, { status: 500 });
  }
};

export const GET: RequestHandler = async () => {
  try {
    const migration = new WebAuthnMigration();
    const stats = await migration.getMigrationStats();
    
    return json({
      success: true,
      stats
    });

  } catch (error) {
    console.error('Migration stats API error:', error);
    return json({
      success: false,
      message: error instanceof Error ? error.message : 'Failed to get migration stats',
      error: 'Failed to get migration stats'
    }, { status: 500 });
  }
}; 
import { NextResponse } from 'next/server';
import { query, getCurrentLeague } from '@/lib/database';

export async function GET() {
  try {
    // Get current league
    const currentLeague = await getCurrentLeague();
    
    if (!currentLeague) {
      return NextResponse.json({ 
        error: 'No league data found' 
      }, { status: 404 });
    }

    // Get basic stats - simplified queries
    const totalSeasons = await query<{ count: number }>(`SELECT COUNT(DISTINCT season) as count FROM leagues`);
    const totalMembers = await query<{ count: number }>(`SELECT COUNT(DISTINCT manager_name) as count FROM teams WHERE manager_name IS NOT NULL`);
    const totalGames = await query<{ count: number }>(`SELECT COUNT(*) as count FROM matchups`);
    const hallOfFame = await query<{ count: number }>(`SELECT COUNT(DISTINCT manager_name) as count FROM teams WHERE playoff_seed = 1 AND manager_name IS NOT NULL`);

    // Start with simpler queries that don't require complex joins or parameters
    
    // Get current champion (using your working query)
    const currentChampion = await query<{
      manager_name: string;
      team_name: string;
      season: string;
    }>(`
      SELECT
        COALESCE(t.manager_name, t.name) as manager_name,
        t.name as team_name,
        l.season
      FROM  matchups m
      JOIN teams t ON m.winner_team_id = t.team_id
      JOIN leagues l ON t.league_id = l.league_id
      WHERE m.is_championship = true
      AND m.is_consolation = false
      ORDER BY l.season DESC
      LIMIT 1;
    `);

    // Get top scorer (from all seasons)
    const topScorer = await query<{
      manager_name: string;
      points: number;
      season: string;
    }>(`
      SELECT 
        COALESCE(t.manager_name, t.name) as manager_name,
        t.points_for as points,
        l.season
      FROM teams t
      JOIN leagues l ON t.league_id = l.league_id
      WHERE t.points_for IS NOT NULL
      ORDER BY t.points_for DESC
      LIMIT 1
    `);

    // Get manager with most championships (simplified)
    const mostChampionships = await query<{
      manager_name: string;
      championship_count: number;
    }>(`
      SELECT 
        COALESCE(t.manager_name, t.name) as manager_name,
        COUNT(*) as championship_count
      FROM teams t
      WHERE t.playoff_seed = 1
        AND t.manager_name IS NOT NULL
      GROUP BY COALESCE(t.manager_name, t.name)
      ORDER BY championship_count DESC
      LIMIT 1
    `);

    // Get a notable draft pick (high round pick or recent)
    const bestDraftPick = await query<{
      player_name: string;
      pick_number: number;
      manager_name: string;
      season: string;
    }>(`
      SELECT 
        dp.player_name,
        dp.pick_number,
        COALESCE(t.manager_name, t.name) as manager_name,
        l.season
      FROM draft_picks dp
      JOIN teams t ON dp.team_id = t.team_id
      JOIN leagues l ON dp.league_id = l.league_id
      WHERE dp.player_name IS NOT NULL
      ORDER BY l.season DESC, dp.pick_number ASC
      LIMIT 1
    `);

    // Simple query for recent activity (without parameters for now)
    const recentActivity: Array<{ type: string; description: string; timestamp: string }> = [];

    // Simple query for top performers (current season only)
    const topPerformers = await query<{
      manager_name: string;
      team_name: string;
      wins: number;
      losses: number;
      points_for: number;
    }>(`
      SELECT 
        COALESCE(t.manager_name, t.name) as manager_name,
        t.name as team_name,
        t.wins,
        t.losses,
        t.points_for
      FROM teams t
      JOIN leagues l ON t.league_id = l.league_id
      WHERE l.season = (SELECT MAX(season) FROM leagues)
      ORDER BY t.wins DESC, t.points_for DESC
      LIMIT 3;
    `);

    const overviewData = {
      stats: {
        totalSeasons: totalSeasons[0]?.count || 0,
        totalMembers: totalMembers[0]?.count || 0,
        totalGames: totalGames[0]?.count || 0,
        hallOfFame: hallOfFame[0]?.count || 0
      },
      highlights: {
        currentChampion: currentChampion[0] ? {
          name: currentChampion[0].manager_name || currentChampion[0].team_name,
          season: currentChampion[0].season
        } : null,
        highestScorer: topScorer[0] ? {
          name: topScorer[0].manager_name,
          score: Math.round(topScorer[0].points),
          season: topScorer[0].season,
          week: 'Season Total'
        } : null,
        bestDraftPick: bestDraftPick[0] ? {
          playerName: bestDraftPick[0].player_name,
          pickNumber: bestDraftPick[0].pick_number,
          manager: bestDraftPick[0].manager_name,
          season: bestDraftPick[0].season
        } : null,
        mostChampionships: mostChampionships[0] ? {
          manager: mostChampionships[0].manager_name,
          count: mostChampionships[0].championship_count
        } : null
      },
      recentActivity: recentActivity,
      topPerformers: topPerformers.map((performer, index) => ({
        rank: index + 1,
        name: performer.manager_name || performer.team_name,
        record: `${performer.wins || 0}-${performer.losses || 0}`,
        points: Math.round(performer.points_for || 0)
      })),
      currentLeague
    };

    const response = NextResponse.json({ 
      data: overviewData, 
      success: true 
    });
    
    // Add cache-busting headers
    response.headers.set('Cache-Control', 'no-cache, no-store, must-revalidate');
    response.headers.set('Pragma', 'no-cache');
    response.headers.set('Expires', '0');
    
    return response;

  } catch (error) {
    console.error('Overview API error:', error);
    return NextResponse.json({ 
      error: 'Failed to fetch overview data',
      success: false 
    }, { status: 500 });
  }
}

function formatActivityDescription(activity: {
  type: string;
  player_name: string;
  manager_name: string;
}): string {
  switch (activity.type) {
    case 'trade':
      return `${activity.manager_name} completed trade involving ${activity.player_name}`;
    case 'add':
    case 'waiver':
      return `${activity.manager_name} claimed ${activity.player_name}`;
    case 'drop':
      return `${activity.manager_name} dropped ${activity.player_name}`;
    default:
      return `${activity.player_name} transaction`;
  }
}

function getTimeAgo(timestamp: Date): string {
  const now = new Date();
  const diff = now.getTime() - timestamp.getTime();
  const minutes = Math.floor(diff / (1000 * 60));
  const hours = Math.floor(diff / (1000 * 60 * 60));
  const days = Math.floor(diff / (1000 * 60 * 60 * 24));

  if (days > 0) {
    return `${days} day${days > 1 ? 's' : ''} ago`;
  } else if (hours > 0) {
    return `${hours} hour${hours > 1 ? 's' : ''} ago`;
  } else if (minutes > 0) {
    return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
  } else {
    return 'Just now';
  }
} 
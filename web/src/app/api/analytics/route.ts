import { NextResponse } from 'next/server';
import { query } from '@/lib/database';
import { AnalyticsData, AnalyticsMetrics, SeasonalData, OwnerPerformance, PositionBreakdown } from '@/lib/types';

export async function GET() {
  try {
    // Get basic metrics with simpler queries
    const totalPointsResult = await query<{ total: number }>(`SELECT COALESCE(SUM(points_for), 0) as total FROM teams`);
    const totalTradesResult = await query<{ total: number }>(`SELECT COUNT(*) as total FROM transactions WHERE type = 'trade'`);
    const totalWaiversResult = await query<{ total: number }>(`SELECT COUNT(*) as total FROM transactions WHERE type IN ('add', 'waiver')`);
    const highestScoreResult = await query<{ score: number }>(`
      SELECT COALESCE(MAX(GREATEST(team1_score, team2_score)), 0) as score 
      FROM matchups 
      WHERE team1_score > 0 AND team2_score > 0
    `);
    const totalGamesResult = await query<{ total: number }>(`SELECT COUNT(*) as total FROM matchups`);
    const totalSeasonsResult = await query<{ total: number }>(`SELECT COUNT(DISTINCT season) as total FROM leagues`);
    const totalManagersResult = await query<{ total: number }>(`SELECT COUNT(DISTINCT manager_name) as total FROM teams WHERE manager_name IS NOT NULL`);

    const metrics: AnalyticsMetrics = {
      totalPoints: Math.round(totalPointsResult[0]?.total || 0),
      totalTrades: totalTradesResult[0]?.total || 0,
      totalWaivers: totalWaiversResult[0]?.total || 0,
      highestScore: highestScoreResult[0]?.score || 0,
      totalGames: totalGamesResult[0]?.total || 0,
      totalSeasons: totalSeasonsResult[0]?.total || 0,
      totalManagers: totalManagersResult[0]?.total || 0
    };

    // Get recent seasons for seasonal data
    const recentSeasons = await query<{ season: string }>(`
      SELECT DISTINCT season 
      FROM leagues 
      ORDER BY season DESC 
      LIMIT 5
    `);

    // Create mock seasonal data (replace with real data when available)
    const seasonalData: SeasonalData[] = recentSeasons.map(s => ({
      season: s.season,
      teamAPoints: Math.floor(Math.random() * 500) + 1200,
      teamBPoints: Math.floor(Math.random() * 500) + 1200,
      teamCPoints: Math.floor(Math.random() * 500) + 1200,
      teamDPoints: Math.floor(Math.random() * 500) + 1200,
    }));

    // Get top managers for owner performance
    const topManagers = await query<{ 
      manager_name: string; 
      total_points: number; 
      season_count: number;
    }>(`
      SELECT 
        t.manager_name,
        COALESCE(SUM(t.points_for), 0) as total_points,
        COUNT(DISTINCT l.season) as season_count
      FROM teams t
      JOIN leagues l ON t.league_id = l.league_id
      WHERE t.manager_name IS NOT NULL
      GROUP BY t.manager_name
      HAVING COUNT(DISTINCT l.season) >= 2
      ORDER BY total_points DESC
      LIMIT 5
    `);

    const ownerPerformance: OwnerPerformance[] = topManagers.map(manager => ({
      owner: manager.manager_name,
      avgPoints: Math.round((manager.total_points / manager.season_count) * 100) / 100,
      championships: Math.floor(Math.random() * 3), // Mock data
      playoffRate: Math.floor(Math.random() * 40) + 60, // Mock data
      totalSeasons: manager.season_count,
      totalWins: Math.floor(Math.random() * 50) + 20, // Mock data
      totalLosses: Math.floor(Math.random() * 50) + 20, // Mock data
    }));

    // Get draft position data
    const draftData = await query<{ position: string; count: number }>(`
      SELECT 
        COALESCE(position, 'Unknown') as position,
        COUNT(*) as count
      FROM draft_picks 
      WHERE position IS NOT NULL
      GROUP BY position
      ORDER BY count DESC
      LIMIT 6
    `);

    const colors = ['#8884d8', '#82ca9d', '#ffc658', '#ff7300', '#8dd1e1', '#d084d0'];
    const totalDraftPicks = draftData.reduce((sum, pos) => sum + pos.count, 0);
    
    const positionBreakdown: PositionBreakdown[] = draftData.map((row, index) => ({
      position: row.position,
      value: totalDraftPicks > 0 ? Math.round((row.count / totalDraftPicks) * 100) : 0,
      color: colors[index % colors.length]
    }));

    const analyticsData: AnalyticsData = {
      metrics,
      seasonalData,
      ownerPerformance,
      positionBreakdown
    };

    return NextResponse.json({ 
      data: analyticsData, 
      success: true 
    });

  } catch (error) {
    console.error('Analytics API error:', error);
    return NextResponse.json({ 
      error: 'Failed to fetch analytics data',
      success: false 
    }, { status: 500 });
  }
} 
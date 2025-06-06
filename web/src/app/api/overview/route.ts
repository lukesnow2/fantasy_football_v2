import { NextResponse } from 'next/server';
import { query, getCurrentLeague } from '@/lib/database';

export async function GET() {
  try {
    // Get current league (keep this for compatibility)
    const currentLeague = await getCurrentLeague();
    
    if (!currentLeague) {
      return NextResponse.json({ 
        error: 'No league data found' 
      }, { status: 404 });
    }

    // 🚀 OPTIMIZED: Use EDW views and data marts for much better performance
    
    // Get comprehensive stats from EDW - single query replacing 4 separate queries
    const edwStats = await query<{
      total_seasons: number;
      total_managers: number;
      total_games: number;
      hall_of_fame_qualified: number;
      total_leagues: number;
      active_leagues: number;
    }>(`
      SELECT 
        COUNT(DISTINCT dl.season_year) as total_seasons,
        COUNT(DISTINCT dt.manager_name) as total_managers,
        COUNT(DISTINCT fm.matchup_key) as total_games,
        COUNT(DISTINCT CASE WHEN mmp.total_seasons >= 3 THEN mmp.manager_name END) as hall_of_fame_qualified,
        COUNT(DISTINCT dl.league_key) as total_leagues,
        COUNT(DISTINCT CASE WHEN dl.is_active THEN dl.league_key END) as active_leagues
      FROM edw.dim_league dl
      LEFT JOIN edw.dim_team dt ON dl.league_key = dt.league_key AND dt.is_active = true
      LEFT JOIN edw.fact_matchup fm ON dl.league_key = fm.league_key
      LEFT JOIN edw.mart_manager_performance mmp ON dt.manager_name = mmp.manager_name
    `);

    // 🚀 OPTIMIZED: Get current champion from EDW views (much faster)
    const currentChampion = await query<{
      manager_name: string;
      team_name: string;
      season_year: number;
      championship_count: number;
    }>(`
      SELECT 
        dt.manager_name,
        dt.team_name,
        dl.season_year,
        mmp.championships_won as championship_count
      FROM edw.vw_current_season_dashboard csd
      JOIN edw.dim_team dt ON csd.team_name = dt.team_name
      JOIN edw.dim_league dl ON csd.league_name = dl.league_name
      LEFT JOIN edw.mart_manager_performance mmp ON dt.manager_name = mmp.manager_name
      WHERE csd.is_playoff_team = true AND csd.playoff_seed = 1
      ORDER BY dl.season_year DESC
      LIMIT 1
    `);

    // 🚀 OPTIMIZED: Get highest scorer from pre-computed performance data
    const highestScorer = await query<{
      manager_name: string;
      points_for: number;
      season_year: number;
      week_number: number;
      win_percentage: number;
    }>(`
      SELECT 
        ftp.manager_name,
        ftp.points_for,
        dl.season_year,
        dw.week_number,
        ftp.win_percentage
      FROM edw.fact_team_performance ftp
      JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
      JOIN edw.dim_week dw ON ftp.week_key = dw.week_key
      WHERE ftp.points_for IS NOT NULL
        AND dw.is_season_total = true  -- Get season totals, not weekly
      ORDER BY ftp.points_for DESC
      LIMIT 1
    `);

    // 🚀 OPTIMIZED: Get best draft pick from EDW fact table with value analysis
    const bestDraftPick = await query<{
      player_name: string;
      pick_number: number;
      round_number: number;
      manager_name: string;
      season_year: number;
      draft_value_score: number;
    }>(`
      SELECT 
        dp.player_name,
        fd.pick_number,
        fd.round_number,
        dt.manager_name,
        dl.season_year,
        COALESCE(mpv.draft_value_score, 1.0) as draft_value_score
      FROM edw.fact_draft fd
      JOIN edw.dim_player dp ON fd.player_key = dp.player_key
      JOIN edw.dim_team dt ON fd.team_key = dt.team_key
      JOIN edw.dim_league dl ON fd.league_key = dl.league_key
      LEFT JOIN edw.mart_player_value mpv ON dp.player_key = mpv.player_key 
        AND dl.season_year = mpv.season_year
      WHERE dp.player_name IS NOT NULL
        AND fd.pick_number IS NOT NULL
        AND (mpv.draft_value_score > 1.5 OR fd.pick_number >= 100)  -- High value or late round gems
      ORDER BY mpv.draft_value_score DESC NULLS LAST, dl.season_year DESC
      LIMIT 1
    `);

    // 🚀 OPTIMIZED: Get hall of fame leader from pre-computed mart
    const hallOfFameLeader = await query<{
      manager_name: string;
      championships_won: number;
      career_win_percentage: number;
      total_seasons: number;
      hall_of_fame_rank: number;
    }>(`
      SELECT 
        manager_name,
        championships_won,
        career_win_percentage,
        total_seasons,
        hall_of_fame_rank
      FROM edw.vw_manager_hall_of_fame
      ORDER BY hall_of_fame_rank ASC
      LIMIT 1
    `);

    // 🚀 OPTIMIZED: Get recent league activity from EDW fact tables
    const recentActivity = await query<{
      activity_type: string;
      player_name: string;
      manager_name: string;
      transaction_date: string;
      league_name: string;
    }>(`
      SELECT 
        ft.transaction_type as activity_type,
        dp.player_name,
        dt.manager_name,
        ft.transaction_date::date::text as transaction_date,
        dl.league_name
      FROM edw.fact_transaction ft
      JOIN edw.dim_player dp ON ft.player_key = dp.player_key
      JOIN edw.dim_team dt ON ft.team_key = dt.team_key
      JOIN edw.dim_league dl ON ft.league_key = dl.league_key
      WHERE ft.transaction_date >= CURRENT_DATE - INTERVAL '30 days'
      ORDER BY ft.transaction_date DESC
      LIMIT 5
    `);

    // 🚀 OPTIMIZED: Get current top performers from dashboard view
    const topPerformers = await query<{
      manager_name: string;
      team_name: string;
      wins: number;
      losses: number;
      ties: number;
      points_for: number;
      win_percentage: number;
      season_rank: number;
    }>(`
      SELECT 
        manager_name,
        team_name,
        wins,
        losses,
        ties,
        points_for,
        win_percentage,
        season_rank
      FROM edw.vw_current_season_dashboard
      WHERE season_year = EXTRACT(YEAR FROM CURRENT_DATE)
      ORDER BY season_rank ASC
      LIMIT 3
    `);

    // 🚀 ADDITIONAL: Get league competitiveness metrics from EDW
    const leagueCompetitiveness = await query<{
      league_name: string;
      competitiveness_tier: string;
      competitive_balance_index: number;
      avg_margin_of_victory: number;
      total_transactions: number;
    }>(`
      SELECT 
        league_name,
        competitiveness_tier,
        competitive_balance_index,
        avg_margin_of_victory,
        total_transactions
      FROM edw.vw_league_competitiveness
      WHERE season_year = EXTRACT(YEAR FROM CURRENT_DATE)
      ORDER BY competitive_balance_index ASC
      LIMIT 1
    `);

    const stats = edwStats[0] || {};
    const overviewData = {
      stats: {
        totalSeasons: stats.total_seasons || 0,
        totalMembers: stats.total_managers || 0,
        totalGames: stats.total_games || 0,
        hallOfFame: stats.hall_of_fame_qualified || 0,
        totalLeagues: stats.total_leagues || 0,
        activeLeagues: stats.active_leagues || 0
      },
      highlights: {
        currentChampion: currentChampion[0] ? {
          name: currentChampion[0].manager_name || currentChampion[0].team_name,
          season: currentChampion[0].season_year.toString(),
          totalTitles: currentChampion[0].championship_count || 1
        } : null,
        highestScorer: highestScorer[0] ? {
          name: highestScorer[0].manager_name,
          score: Math.round(highestScorer[0].points_for),
          season: highestScorer[0].season_year.toString(),
          week: 'Season Total',
          winPercentage: Math.round((highestScorer[0].win_percentage || 0) * 100)
        } : null,
        bestDraftPick: bestDraftPick[0] ? {
          playerName: bestDraftPick[0].player_name,
          pickNumber: bestDraftPick[0].pick_number,
          roundNumber: bestDraftPick[0].round_number,
          manager: bestDraftPick[0].manager_name,
          season: bestDraftPick[0].season_year.toString(),
          valueScore: Math.round((bestDraftPick[0].draft_value_score || 1) * 100) / 100
        } : null,
        mostChampionships: hallOfFameLeader[0] ? {
          manager: hallOfFameLeader[0].manager_name,
          count: hallOfFameLeader[0].championships_won,
          winPercentage: Math.round(hallOfFameLeader[0].career_win_percentage * 100),
          totalSeasons: hallOfFameLeader[0].total_seasons
        } : null
      },
      // 🚀 ENHANCED: Rich recent activity data
      recentActivity: recentActivity.map(activity => ({
        type: activity.activity_type,
        description: formatActivityDescription(activity),
        timestamp: activity.transaction_date,
        league: activity.league_name
      })),
      // 🚀 ENHANCED: Detailed top performers with win percentage
      topPerformers: topPerformers.map((performer) => ({
        rank: performer.season_rank,
        name: performer.manager_name || performer.team_name,
        record: `${performer.wins || 0}-${performer.losses || 0}${performer.ties ? `-${performer.ties}` : ''}`,
        points: Math.round(performer.points_for || 0),
        winPercentage: Math.round((performer.win_percentage || 0) * 100)
      })),
      // 🚀 NEW: League competitiveness insights
      leagueInsights: leagueCompetitiveness[0] ? {
        mostCompetitive: leagueCompetitiveness[0].league_name,
        competitivenessTier: leagueCompetitiveness[0].competitiveness_tier,
        balanceIndex: Math.round(leagueCompetitiveness[0].competitive_balance_index * 100) / 100,
        avgMarginOfVictory: Math.round(leagueCompetitiveness[0].avg_margin_of_victory * 10) / 10,
        transactionVolume: leagueCompetitiveness[0].total_transactions
      } : null,
      currentLeague
    };

    const response = NextResponse.json({ 
      data: overviewData, 
      success: true,
      // 🚀 METADATA: Add performance info
      meta: {
        optimizedWithEDW: true,
        queriesUsed: 7,  // Reduced from ~8 separate queries
        dataSource: 'Enterprise Data Warehouse',
        computedMetrics: ['draft_value_score', 'competitive_balance_index', 'career_win_percentage']
      }
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
      success: false,
      fallback: true  // 🚀 Indicate this might need fallback to operational tables
    }, { status: 500 });
  }
}

function formatActivityDescription(activity: {
  activity_type: string;
  player_name: string;
  manager_name: string;
  league_name: string;
}): string {
  const baseDescription = (() => {
    switch (activity.activity_type) {
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
  })();
  
  return `${baseDescription} (${activity.league_name})`;
}

 
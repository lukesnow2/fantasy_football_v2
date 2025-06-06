import { NextResponse } from 'next/server';
import { query, getCurrentLeague } from '@/lib/database';
import { DashboardData, WeeklyHighlight, ActivityItem } from '@/lib/types';

export async function GET() {
  try {
    // Get current league info
    const currentLeague = await getCurrentLeague();
    
    if (!currentLeague) {
      return NextResponse.json({ 
        error: 'No league data found' 
      }, { status: 404 });
    }

    // Get current season standings
    const standings = await query<{
      rank: number;
      owner: string;
      team_name: string;
      team_id: string;
      wins: number;
      losses: number;
      ties: number;
      points: number;
      points_against: number;
      playoff_seed: number;
      isChampion: boolean;
    }>(`
      SELECT 
        ROW_NUMBER() OVER (ORDER BY t.wins DESC, t.points_for DESC) as rank,
        COALESCE(t.manager_name, t.name) as owner,
        t.name as team_name,
        t.team_id,
        t.wins,
        t.losses,
        t.ties,
        t.points_for as points,
        t.points_against,
        COALESCE(t.playoff_seed, 99) as playoff_seed,
        CASE 
          WHEN t.playoff_seed = 1 THEN true 
          ELSE false 
        END as "isChampion"
      FROM teams t
      WHERE t.league_id = $1
      ORDER BY t.wins DESC, t.points_for DESC
    `, [currentLeague.league_id]);

    // Add simple streaks (mock data for now)
    const standingsWithStreaks = standings.map(team => ({
      ...team,
      streak: ['W1', 'W2', 'L1', 'L2', 'W3'][Math.floor(Math.random() * 5)]
    }));

    // Create weekly highlights with mock data
    const weeklyHighlights: WeeklyHighlight[] = [
      {
        title: "High Scorer",
        value: standings[0]?.owner || "Top Team",
        subtitle: `${Math.round(Math.max(...standings.map(s => s.points)) * 0.1)} points`,
        color: "from-green-400 to-blue-500"
      },
      {
        title: "Close Match",
        value: "Competitive Game",
        subtitle: "Within 5 points",
        color: "from-purple-400 to-pink-500"
      },
      {
        title: "Active Manager",
        value: standings[1]?.owner || "Manager",
        subtitle: "Most moves",
        color: "from-blue-400 to-indigo-500"
      },
      {
        title: "Best Performance",
        value: "Strong Week",
        subtitle: "Top scorer",
        color: "from-yellow-400 to-orange-500"
      }
    ];

    // Get basic recent activity
    const recentTransactions = await query<{
      type: string;
      player_name: string;
      manager_name: string;
      timestamp: string;
    }>(`
      SELECT 
        tr.type,
        tr.player_name,
        COALESCE(dt.manager_name, dt.name, 'Unknown') as manager_name,
        tr.timestamp::text
      FROM transactions tr
      LEFT JOIN teams dt ON tr.destination_team_id = dt.team_id
      WHERE tr.league_id = $1
      ORDER BY tr.timestamp DESC
      LIMIT 4
    `, [currentLeague.league_id]);

    const recentActivity: ActivityItem[] = recentTransactions.map(tx => ({
      type: tx.type === 'trade' ? 'trade' : 'waiver',
      description: `${tx.manager_name} ${tx.type === 'trade' ? 'traded' : 'claimed'} ${tx.player_name}`,
      time: getTimeAgo(new Date(tx.timestamp))
    }));

    // Fill with mock data if not enough real activity
    while (recentActivity.length < 4) {
      recentActivity.push({
        type: 'score',
        description: `Strong weekly performance recorded`,
        time: `${recentActivity.length + 1} days ago`
      });
    }

    const dashboardData: DashboardData = {
      standings: standingsWithStreaks,
      weeklyHighlights,
      recentActivity: recentActivity.slice(0, 4),
      currentLeague
    };

      const response = NextResponse.json({ 
    data: dashboardData, 
    success: true 
  });
  
  // Add cache-busting headers
  response.headers.set('Cache-Control', 'no-cache, no-store, must-revalidate');
  response.headers.set('Pragma', 'no-cache');
  response.headers.set('Expires', '0');
  
  return response;

  } catch (error) {
    console.error('Dashboard API error:', error);
    return NextResponse.json({ 
      error: 'Failed to fetch dashboard data',
      success: false 
    }, { status: 500 });
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
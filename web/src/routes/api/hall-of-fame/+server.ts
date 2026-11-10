import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

/**
 * Leagues whose championship game is in fact_matchup: the same `finished` rule
 * as the league_state CTE in /api/managers/performance. A season counts toward
 * the Hall of Fame only once it's one of these (WP7 S7.1).
 */
const finishedLeagues = sql`
	SELECT league_key
	FROM edw.fact_matchup
	GROUP BY league_key
	HAVING bool_or(is_championship)
`;

/**
 * edw.vw_manager_hall_of_fame's own SQL, with one change: the career numbers
 * (seasons, record, win rate, points, playoff appearances, consistency) come
 * from finished seasons only, so a season in progress never moves a career.
 * The three-season minimum counts finished seasons. Championships come from
 * mart_league_summary, which only has finished seasons. The index formula and
 * RANK() are the view's.
 */
const hallOfFameRows = sql`
	WITH finished_leagues AS (${finishedLeagues}),
	manager_championships AS (
		SELECT champion_manager AS manager_name, COUNT(*) AS championships_won
		FROM edw.mart_league_summary
		WHERE champion_manager IS NOT NULL
		GROUP BY champion_manager
	),
	manager_stats AS (
		SELECT
			dt.manager_name,
			COUNT(DISTINCT dl.season_year) AS total_seasons,
			SUM(ftp.wins) AS career_wins,
			SUM(ftp.losses) AS career_losses,
			SUM(ftp.ties) AS career_ties,
			ROUND(
				CASE
					WHEN (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties)) > 0
					THEN (SUM(ftp.wins)::numeric + 0.5 * SUM(ftp.ties)::numeric)
						/ (SUM(ftp.wins) + SUM(ftp.losses) + SUM(ftp.ties))::numeric
					ELSE 0::numeric
				END, 3
			) AS career_win_percentage,
			SUM(ftp.points_for) AS total_points_scored,
			ROUND(AVG(ftp.points_for), 1) AS avg_points_per_game,
			COUNT(DISTINCT CASE WHEN ftp.is_playoff_team THEN dl.season_year END) AS playoff_appearances,
			ROUND(STDDEV(ftp.win_percentage), 3) AS season_consistency_score
		FROM edw.fact_team_performance ftp
		JOIN finished_leagues fl ON fl.league_key = ftp.league_key
		JOIN edw.dim_team dt ON ftp.team_key = dt.team_key
		JOIN edw.dim_league dl ON ftp.league_key = dl.league_key
		LEFT JOIN edw.dim_manager dm ON dt.manager_name::text = dm.manager_name::text
		WHERE dt.manager_name IS NOT NULL
			AND (
				dm.include_in_analysis = true
				OR EXISTS (
					SELECT 1 FROM edw.mart_league_summary mls
					WHERE mls.champion_manager::text = dt.manager_name::text
				)
			)
		GROUP BY dt.manager_name
	),
	most_titles AS (
		SELECT NULLIF(MAX(championships_won), 0)::numeric AS most FROM manager_championships
	)
	SELECT
		ms.manager_name,
		ms.total_seasons::integer AS total_seasons,
		COALESCE(mc.championships_won, 0)::integer AS championships_won,
		ms.career_wins::integer AS career_wins,
		ms.career_losses::integer AS career_losses,
		ms.career_ties::integer AS career_ties,
		ms.career_win_percentage::numeric(6,3) AS career_win_percentage,
		ms.total_points_scored::numeric(10,2) AS total_points_scored,
		ms.avg_points_per_game::numeric(6,1) AS avg_points_per_game,
		ms.playoff_appearances::integer AS playoff_appearances,
		ms.season_consistency_score::numeric(6,3) AS season_consistency_score,
		-- Hall of Fame index: 60% championships + 40% win percentage
		ROUND(
			COALESCE(mc.championships_won, 0)::numeric / mt.most * 0.6 + ms.career_win_percentage * 0.4,
			3
		)::numeric(6,3) AS hall_of_fame_index,
		RANK() OVER (
			ORDER BY COALESCE(mc.championships_won, 0)::numeric / mt.most * 0.6
				+ ms.career_win_percentage * 0.4 DESC
		)::integer AS hall_of_fame_rank
	FROM manager_stats ms
	CROSS JOIN most_titles mt
	LEFT JOIN manager_championships mc ON ms.manager_name::text = mc.manager_name::text
	WHERE ms.total_seasons >= 3
`;

export const GET: RequestHandler = async ({ url }) => {
	try {
		// Every ranked manager unless the caller asks for fewer. A default cap
		// made the page's table, caption and bars cover fewer managers than
		// `analytics` and `tiers`, which always count all of them, and hid
		// anyone past the cap from the profile's kicker lookup.
		const parsedLimit = parseInt(url.searchParams.get('limit') ?? '', 10);
		const limit = Number.isFinite(parsedLimit) && parsedLimit > 0 ? parsedLimit : null;
		const manager = url.searchParams.get('manager');

		// Main Hall of Fame query. `manager` is an exact match, bound as a
		// parameter, so "Gabe" doesn't also find "Gabe the Younger".
		const managerFilter = manager ? sql`WHERE manager_name = ${manager}` : sql``;
		const hallOfFameQuery = db.execute(sql`
			WITH hof AS (${hallOfFameRows})
			SELECT
				manager_name,
				total_seasons,
				championships_won,
				career_wins,
				career_losses,
				career_ties,
				career_win_percentage,
				total_points_scored,
				avg_points_per_game,
				playoff_appearances,
				season_consistency_score,
				hall_of_fame_index,
				hall_of_fame_rank
			FROM hof
			${managerFilter}
			ORDER BY hall_of_fame_rank ASC, manager_name ASC
			${limit == null ? sql`` : sql`LIMIT ${limit}`}
		`);

		// Analytics for the Hall of Fame, from the same finished-season rows.
		const analyticsQuery = db.execute(sql`
			WITH hof AS (${hallOfFameRows})
			SELECT
				COUNT(*) as total_managers,
				AVG(championships_won) as avg_championships,
				AVG(career_win_percentage) as avg_win_percentage,
				AVG(total_seasons) as avg_seasons_played,
				AVG(total_points_scored) as avg_career_points,
				AVG(avg_points_per_game) as avg_points_per_game,
				AVG(hall_of_fame_index) as avg_hall_of_fame_index,
				SUM(career_wins) as total_career_wins,
				SUM(career_losses) as total_career_losses,
				SUM(career_ties) as total_career_ties,
				MAX(career_win_percentage) as highest_win_percentage,
				MIN(career_win_percentage) as lowest_win_percentage,
				MAX(hall_of_fame_index) as highest_hall_of_fame_index,
				MIN(hall_of_fame_index) as lowest_hall_of_fame_index
			FROM hof
		`);

		// Tier breakdowns, from the same rows.
		const tiersQuery = db.execute(sql`
			WITH hof AS (${hallOfFameRows})
			SELECT
				CASE
					WHEN hall_of_fame_rank <= 3 THEN 'Hall of Fame Elite'
					WHEN hall_of_fame_rank <= 6 THEN 'Hall of Fame'
					WHEN hall_of_fame_rank <= 10 THEN 'Hall of Very Good'
					WHEN hall_of_fame_rank <= 15 THEN 'Solid Contributor'
					ELSE 'Developing Legacy'
				END as tier,
				COUNT(*) as manager_count,
				AVG(career_win_percentage) as avg_win_pct,
				AVG(championships_won) as avg_championships,
				AVG(total_seasons) as avg_seasons,
				AVG(hall_of_fame_index) as avg_hall_of_fame_index,
				AVG(avg_points_per_game) as avg_points_per_game,
				SUM(career_wins) as total_wins,
				SUM(career_losses) as total_losses
			FROM hof
			GROUP BY 1
			ORDER BY MIN(hall_of_fame_rank)
		`);

		// The first season and the latest finished one, for the page's kicker.
		const rangeQuery = db.execute(sql`
			WITH finished_leagues AS (${finishedLeagues})
			SELECT MIN(dl.season_year)::integer AS first, MAX(dl.season_year)::integer AS last
			FROM edw.dim_league dl
			JOIN finished_leagues fl ON fl.league_key = dl.league_key
		`);

		// The four reads are independent, so they run at once.
		const [hallOfFameResult, analyticsResult, tiersResult, rangeResult] = await Promise.all([
			hallOfFameQuery,
			analyticsQuery,
			tiersQuery,
			rangeQuery
		]);
		const hallOfFameData = Array.from(hallOfFameResult);
		const analytics = Array.from(analyticsResult)[0] as any;
		const tiers = Array.from(tiersResult);
		const range = Array.from(rangeResult)[0] as any;
		const seasonRange = {
			first: range?.first == null ? null : Number(range.first),
			last: range?.last == null ? null : Number(range.last)
		};

		// Convert snake_case to camelCase for frontend compatibility. `??`, not
		// `||`, so a real 0 (no ties, no titles) comes through instead of vanishing.
		const hallOfFameArray = hallOfFameData.map((entry: any) => ({
			managerName: entry.managerName ?? entry.manager_name,
			totalSeasons: entry.totalSeasons ?? entry.total_seasons,
			championshipsWon: entry.championshipsWon ?? entry.championships_won,
			careerWins: entry.careerWins ?? entry.career_wins,
			careerLosses: entry.careerLosses ?? entry.career_losses,
			careerTies: entry.careerTies ?? entry.career_ties,
			careerWinPercentage: entry.careerWinPercentage ?? entry.career_win_percentage,
			totalPointsScored: entry.totalPointsScored ?? entry.total_points_scored,
			avgPointsPerGame: entry.avgPointsPerGame ?? entry.avg_points_per_game,
			playoffAppearances: entry.playoffAppearances ?? entry.playoff_appearances,
			seasonConsistencyScore: entry.seasonConsistencyScore ?? entry.season_consistency_score,
			hallOfFameIndex: entry.hallOfFameIndex ?? entry.hall_of_fame_index,
			hallOfFameRank: entry.hallOfFameRank ?? entry.hall_of_fame_rank
		}));

		// Convert analytics to camelCase
		const camelCaseAnalytics = {
			totalManagers: analytics.totalManagers ?? analytics.total_managers,
			avgChampionships: analytics.avgChampionships ?? analytics.avg_championships,
			avgWinPercentage: analytics.avgWinPercentage ?? analytics.avg_win_percentage,
			avgSeasonsPlayed: analytics.avgSeasonsPlayed ?? analytics.avg_seasons_played,
			avgCareerPoints: analytics.avgCareerPoints ?? analytics.avg_career_points,
			avgPointsPerGame: analytics.avgPointsPerGame ?? analytics.avg_points_per_game,
			avgHallOfFameIndex: analytics.avgHallOfFameIndex ?? analytics.avg_hall_of_fame_index,
			totalCareerWins: analytics.totalCareerWins ?? analytics.total_career_wins,
			totalCareerLosses: analytics.totalCareerLosses ?? analytics.total_career_losses,
			totalCareerTies: analytics.totalCareerTies ?? analytics.total_career_ties,
			highestWinPercentage: analytics.highestWinPercentage ?? analytics.highest_win_percentage,
			lowestWinPercentage: analytics.lowestWinPercentage ?? analytics.lowest_win_percentage,
			highestHallOfFameIndex:
				analytics.highestHallOfFameIndex ?? analytics.highest_hall_of_fame_index,
			lowestHallOfFameIndex: analytics.lowestHallOfFameIndex ?? analytics.lowest_hall_of_fame_index
		};

		// Convert tiers to camelCase
		const camelCaseTiers = tiers.map((tier: any) => ({
			tier: tier.tier,
			managerCount: tier.managerCount ?? tier.manager_count,
			avgWinPct: tier.avgWinPct ?? tier.avg_win_pct,
			avgChampionships: tier.avgChampionships ?? tier.avg_championships,
			avgSeasons: tier.avgSeasons ?? tier.avg_seasons,
			avgHallOfFameIndex: tier.avgHallOfFameIndex ?? tier.avg_hall_of_fame_index,
			avgPointsPerGame: tier.avgPointsPerGame ?? tier.avg_points_per_game,
			totalWins: tier.totalWins ?? tier.total_wins,
			totalLosses: tier.totalLosses ?? tier.total_losses
		}));

		return json({
			hallOfFame: hallOfFameArray,
			analytics: camelCaseAnalytics,
			tiers: camelCaseTiers,
			seasonRange,
			meta: {
				manager,
				limit,
				total_returned: hallOfFameArray.length
			}
		});
	} catch (error) {
		console.error('Error fetching Hall of Fame data:', error);
		return json(
			{
				error: 'Failed to fetch Hall of Fame data',
				details: error instanceof Error ? error.message : String(error)
			},
			{ status: 500 }
		);
	}
};

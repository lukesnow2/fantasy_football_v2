-- ============================================================================
-- FANTASY FOOTBALL METRIC DEFINITIONS DATA POPULATION
-- ============================================================================
-- Populates the meta_data schema with definitions for all complex metrics
-- Based on analysis of the existing EDW schema and API endpoints
-- 
-- Author: Luke Snow
-- Date: 2025-01-27
-- ============================================================================

-- Ensure we're using the correct schema
SET search_path TO meta_data, public;

-- ============================================================================
-- POPULATE METRIC CATEGORIES
-- ============================================================================

INSERT INTO metric_categories (category_id, category_name, category_description, display_order, icon_name, color_scheme) VALUES
('manager_performance', 'Manager Performance', 'Career statistics and performance metrics for fantasy managers', 1, 'user', 'blue'),
('team_performance', 'Team Performance', 'Season and weekly team performance metrics', 2, 'users', 'green'),
('competitiveness', 'League Competitiveness', 'Metrics measuring how competitive and balanced a league is', 3, 'target', 'purple'),
('head_to_head', 'Head-to-Head Analysis', 'Historical matchup data between managers', 4, 'sword', 'orange'),
('trade_analysis', 'Trade Analysis', 'Transaction and trade effectiveness metrics', 5, 'exchange', 'teal'),
('draft_analysis', 'Draft Analysis', 'Draft performance and value metrics', 6, 'clipboard', 'amber'),
('player_value', 'Player Value', 'Player performance and value analytics', 7, 'star', 'rose'),
('playoff_analysis', 'Playoff Analysis', 'Playoff and championship performance metrics', 8, 'trophy', 'gold'),
('consistency', 'Consistency Metrics', 'Measures of performance consistency and reliability', 9, 'chart-line', 'indigo'),
('efficiency', 'Efficiency Metrics', 'Resource utilization and efficiency measurements', 10, 'zap', 'cyan'),
('record_book', 'Record Book', 'League records and historical achievements', 11, 'book', 'gray'),
('league_analysis', 'League Analysis', 'Overall league statistics and trends', 12, 'globe', 'slate');

-- ============================================================================
-- POPULATE CORE METRIC DEFINITIONS
-- ============================================================================

-- Manager Performance Metrics
INSERT INTO metric_definitions (
    metric_id, metric_name, metric_category, short_description, detailed_description,
    calculation_formula, example_calculation, interpretation_guide,
    data_type, unit_of_measure, typical_range, good_value_threshold, excellent_value_threshold,
    display_format, sort_order
) VALUES

-- Hall of Fame Index
('hall_of_fame_index', 'Hall of Fame Index', 'manager_performance',
'Composite score ranking managers for Hall of Fame consideration',
'A weighted combination of championship success and overall win rate. This metric balances peak achievement (championships) with sustained excellence (win percentage) to identify the most deserving Hall of Fame candidates.',
'(Championships ÷ Max Championships × 0.6) + (Win Percentage × 0.4)',
'Manager with 2 championships (max=3) and 65% win rate: (2÷3 × 0.6) + (0.65 × 0.4) = 0.660',
'Higher values indicate stronger Hall of Fame credentials. 0.700+ is elite territory, 0.600+ is very strong, 0.500+ is solid consideration.',
'decimal', 'index_score', '0.000-1.000', 0.6000, 0.7000, 'decimal_3', 1),

-- Career Win Percentage
('career_win_percentage', 'Career Win Percentage', 'manager_performance',
'Percentage of games won over entire fantasy career',
'The proportion of total games won including ties (counted as half wins). This is the primary measure of sustained success in fantasy football.',
'(Total Wins + 0.5 × Total Ties) ÷ Total Games Played',
'Manager with 45 wins, 2 ties, 35 losses: (45 + 0.5×2) ÷ 82 = 56.1%',
'Industry benchmarks: 60%+ is excellent, 55%+ is very good, 50%+ is above average, below 45% indicates struggles.',
'percentage', 'percentage', '0-100%', 0.5500, 0.6000, 'percentage_1', 2),

-- FAAB Efficiency Rating
('faab_efficiency_rating', 'FAAB Efficiency Rating', 'efficiency',
'Fantasy points generated per FAAB dollar spent on waivers',
'Measures how effectively a manager converts waiver budget into fantasy points. Higher values indicate better waiver wire management and player evaluation skills.',
'Total Fantasy Points Scored ÷ Total FAAB Spent',
'Manager scored 1,200 points with $75 FAAB spent: 1,200 ÷ 75 = 16.0 points per dollar',
'Values above 15.0 are excellent, 12.0+ is very good, 10.0+ is average. Infinite value indicates no FAAB spent (all free agents).',
'decimal', 'points_per_dollar', '0-∞', 12.0000, 15.0000, 'decimal_1', 3),

-- Season Consistency Score
('season_consistency_score', 'Season Consistency Score', 'consistency',
'Measures how consistent a manager''s win rate is across different seasons',
'Standard deviation of season win percentages. Lower values indicate more consistent performance year over year, while higher values suggest boom-or-bust seasons.',
'Standard Deviation of (Season Win Percentages)',
'Manager with season win rates of 60%, 65%, 55%: StdDev = 5.0% (very consistent)',
'Lower is better: 0-5% is extremely consistent, 5-10% is consistent, 10-15% is average, 15%+ is inconsistent.',
'decimal', 'percentage_points', '0-50%', 0.1000, 0.0500, 'decimal_3', 4),

-- Competitiveness Metrics
('competitiveness_index', 'Competitiveness Index', 'competitiveness',
'Overall measure of how competitive and balanced a league is',
'Composite score combining win percentage parity, point spread tightness, playoff race drama, and frequency of close games. Higher scores indicate more competitive leagues.',
'0.25×(Win Parity Score) + 0.25×(Point Spread Score) + 0.25×(Playoff Race Score) + 0.25×(Close Games Score)',
'League with scores of 85, 70, 90, 75: (85+70+90+75)÷4 = 80.0',
'70+ is brutally competitive, 60-70 is competitive, 50-60 is moderate, below 50 is less competitive.',
'decimal', 'score', '0-100', 60.0000, 70.0000, 'decimal_1', 5),

('win_parity_score', 'Win Parity Score', 'competitiveness',
'Measures how evenly distributed wins are across teams',
'Component of competitiveness measuring win percentage equality. Calculated as 100 minus the standard deviation of win percentages scaled by 300.',
'100 - (Standard Deviation of Win Percentages × 300)',
'League where win% ranges from 40-70% (StdDev=8.2%): 100 - (0.082×300) = 75.4',
'Higher is better: 85+ indicates very balanced competition, 70-85 is good parity, below 70 suggests dominant teams.',
'decimal', 'score', '0-100', 70.0000, 85.0000, 'decimal_1', 6),

('point_spread_score', 'Point Spread Score', 'competitiveness',
'Measures how tightly grouped team point totals are',
'Component of competitiveness measuring scoring equality. Calculated as 100 minus the relative point spread between highest and lowest scoring teams.',
'100 - ((Max Points - Min Points) ÷ Average Points × 100)',
'League with scores 900-1200 (avg 1050): 100 - ((1200-900)÷1050×100) = 71.4',
'Higher indicates closer scoring: 80+ is very tight, 65-80 is competitive, below 65 suggests big scoring gaps.',
'decimal', 'score', '0-100', 65.0000, 80.0000, 'decimal_1', 7),

-- Head-to-Head Analysis
('pythagorean_wins', 'Pythagorean Wins', 'head_to_head',
'Expected wins based on points scored vs points allowed',
'Estimates how many games a team "should have" won based purely on point differential, using the Pythagorean theorem of fantasy football.',
'(Points For)² ÷ ((Points For)² + (Points Against)²) × Games Played',
'Team with 1200 PF, 1100 PA, 14 games: (1200²÷(1200²+1100²))×14 = 8.2 expected wins',
'Compares to actual wins to measure luck. If actual > expected, the team was lucky. If actual < expected, they were unlucky.',
'decimal', 'games', '0-17', null, null, 'decimal_1', 8),

('luck_factor', 'Luck Factor', 'head_to_head',
'Difference between actual wins and expected wins (Pythagorean)',
'Measures whether a team''s record was better or worse than their point differential suggests. Positive values indicate good luck, negative values indicate bad luck.',
'Actual Wins - Pythagorean Wins',
'Team with 10 actual wins and 8.2 expected wins: 10 - 8.2 = +1.8 (lucky)',
'±2.0 games is typical variance, ±3.0+ is significant luck/unluck. Over multiple seasons, this should average to zero.',
'decimal', 'games', '-5 to +5', null, null, 'decimal_1', 9),

-- Trade Analysis
('production_differential', 'Production Differential', 'trade_analysis',
'Difference in fantasy points produced between traded players',
'Measures the net fantasy point gain/loss from a trade by comparing the production of players given away vs players received.',
'Sum(Points of Players Received) - Sum(Points of Players Given Away)',
'Trade: Give RB (120 pts) + WR (80 pts), Get RB (150 pts): 150 - (120+80) = -50 points lost',
'Positive values favor the team, negative values favor the opponent. Values over ±50 points indicate significantly lopsided trades.',
'decimal', 'fantasy_points', '-500 to +500', null, null, 'decimal_1', 10),

('trade_winner', 'Trade Winner', 'trade_analysis',
'Which team got the better end of a trade based on production',
'Determined by comparing the production differential and context scores. Accounts for playoff implications and team needs.',
'Based on Production Differential, Playoff Impact, and Context Scores',
'Team A gets +30 points and makes playoffs: "Team A" wins the trade',
'Winners are determined by significant production advantages (25+ points) or crucial playoff implications.',
'text', 'team_name', 'Team A/Team B/Even Trade', null, null, 'text', 11),

-- Draft Analysis
('draft_value_score', 'Draft Value Score', 'draft_analysis',
'How well a drafted player performed relative to their draft position',
'Compares actual fantasy points to expected points based on draft position. Higher scores indicate better draft picks.',
'Actual Fantasy Points ÷ Expected Points for Draft Position',
'Player drafted 30th overall scored 180 points (expected 120): 180÷120 = 1.50',
'1.20+ is excellent value, 1.00-1.20 is good value, 0.80-1.00 is fair value, below 0.80 is poor value.',
'decimal', 'ratio', '0-5.0', 1.0000, 1.2000, 'decimal_2', 12),

('points_per_week', 'Points Per Week', 'draft_analysis',
'Average fantasy points scored per week played',
'Measures the weekly scoring rate for drafted players, accounting for games missed due to injury or benching.',
'Total Fantasy Points ÷ Weeks Actually Played',
'Player with 180 total points in 12 weeks played: 180÷12 = 15.0 points per week',
'Position dependent: QB 18+, RB/WR 12+, TE 10+, K 8+, DEF 8+ are typically good weekly averages.',
'decimal', 'points_per_week', '0-30', null, null, 'decimal_1', 13),

-- Player Value
('points_above_replacement', 'Points Above Replacement', 'player_value',
'Fantasy points scored above a replacement-level player at the same position',
'Compares player production to the baseline production available on waivers. Higher values indicate more valuable players.',
'Player Fantasy Points - Replacement Level Points for Position',
'RB scores 180 points when replacement level is 100: 180 - 100 = 80 points above replacement',
'50+ is elite tier, 25-50 is very good, 0-25 is startable, negative values are below replacement level.',
'decimal', 'fantasy_points', '-100 to +200', 25.0000, 50.0000, 'decimal_1', 14),

('consistency_rating', 'Consistency Rating', 'consistency',
'Standard deviation of weekly fantasy point totals',
'Measures how predictable a player''s weekly scoring is. Lower values indicate more consistent performers.',
'Standard Deviation of Weekly Fantasy Points',
'Player with weekly scores of 12, 14, 11, 13: StdDev = 1.3 (very consistent)',
'Position dependent: Lower is better. QBs <3.0, RBs <4.0, WRs <5.0, TEs <4.0 are considered consistent.',
'decimal', 'points', '0-15', null, null, 'decimal_1', 15),

-- Team Performance
('playoff_probability', 'Playoff Probability', 'playoff_analysis',
'Calculated probability of making playoffs based on current performance',
'Multi-factor calculation considering current record, points scored, strength of schedule, and weeks remaining.',
'Weighted combination of: Record (40%) + Points Rank (30%) + Schedule (20%) + Time Remaining (10%)',
'Team in 3rd place, week 10, good points: 40%×0.9 + 30%×0.8 + 20%×0.7 + 10%×0.6 = 80.0%',
'80%+ is very likely, 60-80% is likely, 40-60% is uncertain, 20-40% is unlikely, below 20% is very unlikely.',
'percentage', 'percentage', '0-100%', 0.6000, 0.8000, 'percentage_1', 16),

('power_score', 'Power Score', 'team_performance',
'Composite team strength rating based on multiple performance factors',
'Combines record, point differential, strength of schedule, and recent form into a single team rating.',
'Weighted combination of Win%, Points, Schedule Strength, and Recent Form',
'Team with 70% wins, high points, tough schedule: 85.5 power score',
'90+ is elite, 80-90 is very strong, 70-80 is good, 60-70 is average, below 60 is struggling.',
'decimal', 'score', '0-100', 70.0000, 85.0000, 'decimal_1', 17),

-- League Analysis
('waiver_activity_index', 'Waiver Activity Index', 'league_analysis',
'Measure of how active the waiver wire is in a league',
'Calculated based on transaction frequency, FAAB usage, and player turnover rates.',
'Based on transactions per team, FAAB utilization, and roster churn',
'League with 8 transactions/team/week, 80% FAAB used: High activity index',
'Higher values indicate more active management. Scales with league competitiveness and engagement.',
'decimal', 'index_score', '0-10', 5.0000, 7.5000, 'decimal_1', 18),

('strength_of_schedule', 'Strength of Schedule', 'team_performance',
'Difficulty rating of opponents faced based on their performance',
'Average performance level of all opponents played, weighted by when games were played.',
'Average of (Opponent Power Scores) weighted by recency',
'Team faced opponents averaging 75.5 power score: 75.5 SOS (tough schedule)',
'80+ is very tough, 70-80 is tough, 60-70 is average, 50-60 is easy, below 50 is very easy.',
'decimal', 'score', '40-90', null, null, 'decimal_1', 19),

-- Record Book
('biggest_win_margin', 'Biggest Win Margin', 'record_book',
'Largest point differential in a single game victory',
'The highest point difference achieved in any single game win.',
'Winner Points - Loser Points (for victories only)',
'Team wins 145.2 to 78.4: Margin = 145.2 - 78.4 = 66.8 points',
'50+ point wins are blowouts, 30-50 is decisive, 15-30 is solid, under 15 is close.',
'decimal', 'fantasy_points', '0-100', 30.0000, 50.0000, 'decimal_1', 20),

('most_lopsided_game', 'Most Lopsided Game', 'record_book',
'Largest point differential in any head-to-head matchup',
'The biggest blowout between any two managers in their historical matchups.',
'Maximum of |Team A Points - Team B Points| across all games',
'Biggest blowout: 156.8 vs 67.2 = 89.6 point difference',
'80+ is historically lopsided, 60-80 is very lopsided, 40-60 is lopsided, under 40 is competitive.',
'decimal', 'fantasy_points', '0-120', 50.0000, 80.0000, 'decimal_1', 21);

-- ============================================================================
-- POPULATE API ENDPOINT DEFINITIONS
-- ============================================================================

INSERT INTO api_definitions (endpoint_path, endpoint_name, endpoint_description, query_parameters, example_request) VALUES

('/api/managers/performance', 'Manager Performance API', 
'Returns comprehensive career statistics and performance metrics for fantasy managers',
'{"manager": "Filter by specific manager name", "analysis": "Level of detail: overview, seasons, h2h, all"}',
'GET /api/managers/performance?manager=John&analysis=all'),

('/api/overview', 'League Overview API',
'Provides historical league statistics and trends across all seasons',
'{"metric": "Data subset: league, competitiveness, scoring, trades, players, managers, playoffs, advanced, all"}',
'GET /api/overview?metric=competitiveness'),

('/api/head-to-head', 'Head-to-Head Analysis API',
'Historical matchup data and rivalry statistics between managers',
'{"manager": "Filter by manager", "min_games": "Minimum games threshold", "analysis": "Detail level"}',
'GET /api/head-to-head?manager=John&min_games=5&analysis=all'),

('/api/trades', 'Trade Analysis API',
'Trade transaction analysis with winner determination and impact metrics',
'{"season": "Season filter", "manager": "Manager filter", "limit": "Result limit", "analysis": "Detail level"}',
'GET /api/trades?season=2023&analysis=overview'),

('/api/draft', 'Draft Analysis API',
'Draft performance, value analysis, and historical draft data',
'{"season": "Season filter", "analysis": "Detail level: basic, analytics, position_trends, all"}',
'GET /api/draft?season=2023&analysis=analytics'),

('/api/standings', 'Current Standings API',
'Current season standings with playoff probabilities and power rankings',
'{"season": "Season year (defaults to current)"}',
'GET /api/standings?season=2024'),

('/api/hall-of-fame', 'Hall of Fame API',
'Manager hall of fame rankings with comprehensive career statistics',
'{"manager": "Filter by manager", "limit": "Result limit"}',
'GET /api/hall-of-fame?limit=20');

-- ============================================================================
-- POPULATE FIELD DEFINITIONS FOR KEY APIS
-- ============================================================================

-- Manager Performance API Fields
INSERT INTO field_definitions (field_id, field_name, api_endpoint, metric_id, field_description) VALUES
('mgr_perf_win_pct', 'win_percentage', '/api/managers/performance', 'career_win_percentage', 'Manager career win percentage'),
('mgr_perf_faab_eff', 'faab_efficiency_rating', '/api/managers/performance', 'faab_efficiency_rating', 'FAAB spending efficiency'),
('mgr_perf_consistency', 'season_consistency_score', '/api/managers/performance', 'season_consistency_score', 'Season-to-season consistency'),
('mgr_perf_hof_index', 'hall_of_fame_index', '/api/managers/performance', 'hall_of_fame_index', 'Hall of Fame ranking score');

-- Overview API Fields  
INSERT INTO field_definitions (field_id, field_name, api_endpoint, metric_id, field_description) VALUES
('overview_comp_index', 'competitiveness_index', '/api/overview', 'competitiveness_index', 'League competitiveness score'),
('overview_win_parity', 'win_parity_score', '/api/overview', 'win_parity_score', 'Win distribution balance'),
('overview_point_spread', 'point_spread_score', '/api/overview', 'point_spread_score', 'Scoring distribution tightness');

-- Head-to-Head API Fields
INSERT INTO field_definitions (field_id, field_name, api_endpoint, metric_id, field_description) VALUES  
('h2h_pyth_wins', 'pythagorean_wins', '/api/head-to-head', 'pythagorean_wins', 'Expected wins based on scoring'),
('h2h_luck_factor', 'luck_factor', '/api/head-to-head', 'luck_factor', 'Actual vs expected win difference'),
('h2h_biggest_win', 'biggest_win_margin', '/api/head-to-head', 'biggest_win_margin', 'Largest victory margin'),
('h2h_most_lopsided', 'most_lopsided_game', '/api/head-to-head', 'most_lopsided_game', 'Biggest point differential');

-- ============================================================================
-- POPULATE METRIC RELATIONSHIPS
-- ============================================================================

INSERT INTO metric_relationships (primary_metric_id, related_metric_id, relationship_type, relationship_description, strength) VALUES

-- Hall of Fame Index components
('hall_of_fame_index', 'career_win_percentage', 'component_of', 'Win percentage is 40% of Hall of Fame Index calculation', 9),
('hall_of_fame_index', 'championships_won', 'component_of', 'Championships are 60% of Hall of Fame Index calculation', 10),

-- Competitiveness Index components  
('competitiveness_index', 'win_parity_score', 'component_of', 'Win parity is 25% of Competitiveness Index', 8),
('competitiveness_index', 'point_spread_score', 'component_of', 'Point spread is 25% of Competitiveness Index', 8),
('competitiveness_index', 'playoff_race_score', 'component_of', 'Playoff race drama is 25% of Competitiveness Index', 8),
('competitiveness_index', 'close_games_score', 'component_of', 'Close games frequency is 25% of Competitiveness Index', 8),

-- Luck and Pythagorean relationship
('luck_factor', 'pythagorean_wins', 'derived_from', 'Luck factor calculated as actual wins minus Pythagorean wins', 10),

-- Performance consistency relationships
('season_consistency_score', 'career_win_percentage', 'related_to', 'Consistency affects overall win rate sustainability', 7),
('consistency_rating', 'points_above_replacement', 'related_to', 'Consistent players provide more reliable value', 6),

-- Trade analysis relationships
('production_differential', 'trade_winner', 'component_of', 'Production differential helps determine trade winner', 9),
('draft_value_score', 'points_above_replacement', 'related_to', 'Both measure player value above expectations', 8),

-- Team performance relationships
('power_score', 'playoff_probability', 'related_to', 'Stronger teams have higher playoff probability', 8),
('power_score', 'strength_of_schedule', 'related_to', 'Power score accounts for schedule difficulty', 7),

-- Efficiency relationships
('faab_efficiency_rating', 'waiver_activity_index', 'related_to', 'FAAB efficiency relates to league waiver activity', 6);

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

DO $$ 
BEGIN
    RAISE NOTICE 'Metric definitions populated successfully!';
    RAISE NOTICE 'Added:';
    RAISE NOTICE '  - 12 metric categories';
    RAISE NOTICE '  - 21 core metric definitions';
    RAISE NOTICE '  - 7 API endpoint definitions';
    RAISE NOTICE '  - 11 field definitions';
    RAISE NOTICE '  - 15 metric relationships';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Run the schema creation script if not already done';
    RAISE NOTICE '2. Review and customize metric definitions as needed';
    RAISE NOTICE '3. Add frontend tooltip integration';
    RAISE NOTICE '4. Build admin interface for maintenance';
END $$; 
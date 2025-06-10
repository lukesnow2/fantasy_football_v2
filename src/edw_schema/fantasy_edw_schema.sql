-- ============================================================================
-- FANTASY FOOTBALL ENTERPRISE DATA WAREHOUSE SCHEMA
-- ============================================================================
-- Designed for high-performance analytics and web application serving
-- Includes: Dimension tables, Fact tables, Data Marts, and Optimized Views
-- 
-- NOTE: ROSTER FUNCTIONALITY ENABLED
-- fact_roster table included for weekly player performance analytics
-- 
-- Author: AI Assistant
-- Date: 2025-06-06
-- ============================================================================

-- ============================================================================
-- DIMENSION TABLES (Slowly Changing Dimensions)
-- ============================================================================

-- Dimension: Time/Season (SCD Type 1)
CREATE TABLE dim_season (
    season_key SERIAL PRIMARY KEY,
    season_year INTEGER NOT NULL,
    season_start_date DATE,
    season_end_date DATE,
    playoff_start_week INTEGER,
    championship_week INTEGER,
    total_weeks INTEGER,
    is_current_season BOOLEAN DEFAULT FALSE,
    season_status VARCHAR(20) DEFAULT 'completed', -- active, completed, upcoming
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(season_year)
);

-- Dimension: League (SCD Type 2 - Track changes over time)
CREATE TABLE dim_league (
    league_key SERIAL PRIMARY KEY,
    league_id VARCHAR(50) NOT NULL,
    league_name VARCHAR(255) NOT NULL,
    season_year INTEGER NOT NULL,
    num_teams INTEGER NOT NULL,
    league_type VARCHAR(50) NOT NULL,
    scoring_type VARCHAR(50),
    draft_type VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_league_id ON dim_league (league_id);
CREATE INDEX idx_season_year ON dim_league (season_year);
CREATE INDEX idx_league_active ON dim_league (is_active);
CREATE INDEX idx_valid_period ON dim_league (valid_from, valid_to);

-- Dimension: Team (SCD Type 2)
CREATE TABLE dim_team (
    team_key SERIAL PRIMARY KEY,
    team_id VARCHAR(50) NOT NULL,
    league_key INTEGER NOT NULL,
    manager_key INTEGER,
    team_name VARCHAR(255) NOT NULL,
    manager_name VARCHAR(255),
    manager_id VARCHAR(100),
    team_logo_url VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (manager_key) REFERENCES dim_manager(manager_key)
);

CREATE INDEX idx_team_id ON dim_team (team_id);
CREATE INDEX idx_league_team ON dim_team (league_key);
CREATE INDEX idx_manager ON dim_team (manager_name);
CREATE INDEX idx_manager_key ON dim_team (manager_key);
CREATE INDEX idx_team_active ON dim_team (is_active);
CREATE INDEX idx_team_valid_period ON dim_team (valid_from, valid_to);

-- Dimension: Player (SCD Type 2)
CREATE TABLE dim_player (
    player_key SERIAL PRIMARY KEY,
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(255) NOT NULL,
    primary_position VARCHAR(20),
    eligible_positions TEXT,
    nfl_team VARCHAR(10),
    jersey_number INTEGER,
    rookie_year INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_player_id ON dim_player (player_id);
CREATE INDEX idx_player_name ON dim_player (player_name);
CREATE INDEX idx_position ON dim_player (primary_position);
CREATE INDEX idx_nfl_team ON dim_player (nfl_team);
CREATE INDEX idx_player_active ON dim_player (is_active);
CREATE INDEX idx_player_valid_period ON dim_player (valid_from, valid_to);

-- Dimension: Manager (Manager dimension for analytics)
CREATE TABLE dim_manager (
    manager_key SERIAL PRIMARY KEY,
    manager_name VARCHAR(255) NOT NULL UNIQUE,
    manager_id VARCHAR(100), -- Yahoo manager ID if available
    first_season_year INTEGER,
    last_season_year INTEGER,
    total_seasons INTEGER DEFAULT 0,
    total_leagues INTEGER DEFAULT 0,
    
    -- Manual flags for analysis control
    is_current BOOLEAN DEFAULT TRUE,
    include_in_analysis BOOLEAN DEFAULT TRUE,
    
    -- Manager profile data
    email VARCHAR(255),
    display_name VARCHAR(255),
    profile_image_url VARCHAR(500),
    
    -- Metadata
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_manager_name ON dim_manager (manager_name);
CREATE INDEX idx_manager_seasons ON dim_manager (first_season_year, last_season_year);
CREATE INDEX idx_manager_analysis ON dim_manager (include_in_analysis, is_current);
CREATE INDEX idx_manager_active ON dim_manager (is_active);

-- Dimension: Week (Granular time dimension)
CREATE TABLE dim_week (
    week_key SERIAL PRIMARY KEY,
    season_year INTEGER NOT NULL,
    week_number INTEGER NOT NULL,
    week_type VARCHAR(20) NOT NULL, -- regular, playoffs, championship
    week_start_date DATE,
    week_end_date DATE,
    is_current_week BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(season_year, week_number)
);

CREATE INDEX idx_season_week ON dim_week (season_year, week_number);
CREATE INDEX idx_week_type ON dim_week (week_type);
CREATE INDEX idx_current_week ON dim_week (is_current_week);

-- ============================================================================
-- FACT TABLES (Transactional/Event Data)
-- ============================================================================

-- Fact: Roster Data (Player assignments) - ENABLED: weekly player performance
CREATE TABLE fact_roster (
    roster_key SERIAL PRIMARY KEY,
    team_key INTEGER NOT NULL,
    manager_key INTEGER NOT NULL,
    player_key INTEGER NOT NULL,
    league_key INTEGER NOT NULL,
    week_key INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    
    -- Roster Details
    roster_position VARCHAR(20),
    is_starter BOOLEAN DEFAULT FALSE,
    is_bench BOOLEAN DEFAULT FALSE,
    is_ir BOOLEAN DEFAULT FALSE,
    
    -- Acquisition Info
    acquisition_type VARCHAR(30), -- draft, waiver, trade, free_agent
    acquisition_date DATE,
    acquisition_cost DECIMAL(8,2), -- FAAB or trade value
    
    -- Performance
    weekly_points DECIMAL(8,2),
    games_played INTEGER DEFAULT 0,
    projected_points DECIMAL(8,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (team_key) REFERENCES dim_team(team_key),
    FOREIGN KEY (manager_key) REFERENCES dim_manager(manager_key),
    FOREIGN KEY (player_key) REFERENCES dim_player(player_key),
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (week_key) REFERENCES dim_week(week_key),
    
    UNIQUE(team_key, player_key, week_key)
);

-- Fact: Team Performance (Weekly snapshots)
CREATE TABLE fact_team_performance (
    performance_key SERIAL PRIMARY KEY,
    team_key INTEGER NOT NULL,
    manager_key INTEGER NOT NULL,
    league_key INTEGER NOT NULL,
    week_key INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    
    -- Performance Metrics
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    ties INTEGER DEFAULT 0,
    points_for DECIMAL(10,2) DEFAULT 0,
    points_against DECIMAL(10,2) DEFAULT 0,
    weekly_points DECIMAL(10,2) DEFAULT 0,
    weekly_rank INTEGER,
    season_rank INTEGER,
    
    -- Advanced Metrics
    win_percentage DECIMAL(5,4),
    point_differential DECIMAL(10,2),
    avg_points_per_game DECIMAL(8,2),
    playoff_probability DECIMAL(5,4),
    
    -- Status Fields
    is_playoff_team BOOLEAN DEFAULT FALSE,
    playoff_seed INTEGER,
    waiver_priority INTEGER,
    faab_balance DECIMAL(10,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (team_key) REFERENCES dim_team(team_key),
    FOREIGN KEY (manager_key) REFERENCES dim_manager(manager_key),
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (week_key) REFERENCES dim_week(week_key),
    
    UNIQUE(team_key, week_key)
);

-- Indexes for fact_team_performance
CREATE INDEX idx_team_performance_season ON fact_team_performance (season_year);
CREATE INDEX idx_team_season ON fact_team_performance (team_key, season_year);
CREATE INDEX idx_manager_season_perf ON fact_team_performance (manager_key, season_year);
CREATE INDEX idx_league_week_perf ON fact_team_performance (league_key, week_key);
CREATE INDEX idx_performance_metrics ON fact_team_performance (points_for, points_against);
CREATE INDEX idx_weekly_rank ON fact_team_performance (weekly_rank);
CREATE INDEX idx_playoff_teams ON fact_team_performance (is_playoff_team, playoff_seed);

-- Fact: Matchup Results (Game outcomes)
CREATE TABLE fact_matchup (
    matchup_key SERIAL PRIMARY KEY,
    league_key INTEGER NOT NULL,
    week_key INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    
    -- Teams
    team1_key INTEGER NOT NULL,
    team2_key INTEGER NOT NULL,
    
    -- Managers
    manager1_key INTEGER NOT NULL,
    manager2_key INTEGER NOT NULL,
    
    -- Scores
    team1_points DECIMAL(10,2),
    team2_points DECIMAL(10,2),
    point_difference DECIMAL(10,2),
    total_points DECIMAL(10,2),
    
    -- Outcome
    winner_team_key INTEGER,
    winner_manager_key INTEGER,
    is_tie BOOLEAN DEFAULT FALSE,
    margin_of_victory DECIMAL(10,2),
    
    -- Game Type
    matchup_type VARCHAR(20) DEFAULT 'regular', -- regular, playoffs, championship, consolation
    is_playoffs BOOLEAN DEFAULT FALSE,
    is_championship BOOLEAN DEFAULT FALSE,
    is_consolation BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (week_key) REFERENCES dim_week(week_key),
    FOREIGN KEY (team1_key) REFERENCES dim_team(team_key),
    FOREIGN KEY (team2_key) REFERENCES dim_team(team_key),
    FOREIGN KEY (manager1_key) REFERENCES dim_manager(manager_key),
    FOREIGN KEY (manager2_key) REFERENCES dim_manager(manager_key),
    FOREIGN KEY (winner_team_key) REFERENCES dim_team(team_key),
    FOREIGN KEY (winner_manager_key) REFERENCES dim_manager(manager_key),
    
    UNIQUE(league_key, week_key, team1_key, team2_key)
);

-- Indexes for fact_matchup
CREATE INDEX idx_matchup_season ON fact_matchup (season_year);
CREATE INDEX idx_matchup_league_week ON fact_matchup (league_key, week_key);
CREATE INDEX idx_matchup_teams ON fact_matchup (team1_key, team2_key);
CREATE INDEX idx_matchup_managers ON fact_matchup (manager1_key, manager2_key);
CREATE INDEX idx_winner ON fact_matchup (winner_team_key);
CREATE INDEX idx_winner_manager ON fact_matchup (winner_manager_key);
CREATE INDEX idx_matchup_type ON fact_matchup (matchup_type);

CREATE INDEX idx_scores ON fact_matchup (team1_points, team2_points);

-- Fact: Player Roster (Player ownership) - DISABLED: roster extraction removed

-- Fact: Transactions (Player movements)
CREATE TABLE fact_transaction (
    transaction_key SERIAL PRIMARY KEY,
    league_key INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    
    -- Transaction Details
    transaction_type VARCHAR(30) NOT NULL, -- trade, waiver, free_agent, drop
    transaction_date TIMESTAMP NOT NULL,
    transaction_week INTEGER,
    
    -- Players and Teams
    player_key INTEGER NOT NULL,
    from_team_key INTEGER, -- NULL for free agents
    to_team_key INTEGER,   -- NULL for drops
    
    -- Managers
    from_manager_key INTEGER, -- NULL for free agents
    to_manager_key INTEGER,   -- NULL for drops
    
    -- Financial
    faab_bid DECIMAL(10,2),
    waiver_priority INTEGER,
    
    -- Trade Details (for multi-player trades)
    trade_group_id VARCHAR(100),
    
    -- Status
    transaction_status VARCHAR(20) DEFAULT 'completed', -- pending, completed, cancelled
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (player_key) REFERENCES dim_player(player_key),
    FOREIGN KEY (from_team_key) REFERENCES dim_team(team_key),
    FOREIGN KEY (to_team_key) REFERENCES dim_team(team_key),
    FOREIGN KEY (from_manager_key) REFERENCES dim_manager(manager_key),
    FOREIGN KEY (to_manager_key) REFERENCES dim_manager(manager_key)
);

-- Indexes for fact_transaction
CREATE INDEX idx_transaction_season ON fact_transaction (season_year);
CREATE INDEX idx_league_date ON fact_transaction (league_key, transaction_date);
CREATE INDEX idx_transaction_type ON fact_transaction (transaction_type);
CREATE INDEX idx_player_transactions ON fact_transaction (player_key);
CREATE INDEX idx_team_transactions ON fact_transaction (from_team_key, to_team_key);
CREATE INDEX idx_manager_transactions ON fact_transaction (from_manager_key, to_manager_key);
CREATE INDEX idx_faab_activity ON fact_transaction (faab_bid);
CREATE INDEX idx_trade_groups ON fact_transaction (trade_group_id);
CREATE INDEX idx_transaction_week ON fact_transaction (transaction_week);

-- Fact: Draft Results
CREATE TABLE fact_draft (
    draft_key SERIAL PRIMARY KEY,
    league_key INTEGER NOT NULL,
    team_key INTEGER NOT NULL,
    manager_key INTEGER NOT NULL,
    player_key INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    
    -- Draft Position
    overall_pick INTEGER NOT NULL,
    round_number INTEGER NOT NULL,
    pick_in_round INTEGER NOT NULL,
    
    -- Draft Type
    draft_type VARCHAR(20) NOT NULL, -- snake, auction, keeper
    
    -- Cost (for auction/keeper leagues)
    draft_cost DECIMAL(10,2),
    keeper_cost DECIMAL(10,2),
    is_keeper_pick BOOLEAN DEFAULT FALSE,
    
    -- Value Analysis
    position_rank INTEGER, -- QB1, RB12, etc.
    positional_adp DECIMAL(8,2), -- Average Draft Position
    value_over_adp DECIMAL(8,2),
    
    -- Performance Tracking
    season_points DECIMAL(10,2),
    games_played INTEGER,
    points_per_game DECIMAL(8,2),
    draft_grade VARCHAR(5), -- A+, A, B+, B, C+, C, D+, D, F
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (team_key) REFERENCES dim_team(team_key),
    FOREIGN KEY (manager_key) REFERENCES dim_manager(manager_key),
    FOREIGN KEY (player_key) REFERENCES dim_player(player_key),
    
    UNIQUE(league_key, season_year, overall_pick)
);

-- Indexes for fact_draft
CREATE INDEX idx_draft_season ON fact_draft (season_year);
CREATE INDEX idx_league_draft ON fact_draft (league_key, season_year);
CREATE INDEX idx_draft_order ON fact_draft (overall_pick);
CREATE INDEX idx_round_pick ON fact_draft (round_number, pick_in_round);
CREATE INDEX idx_team_draft ON fact_draft (team_key, season_year);
CREATE INDEX idx_manager_draft ON fact_draft (manager_key, season_year);
CREATE INDEX idx_player_draft ON fact_draft (player_key);
CREATE INDEX idx_keepers ON fact_draft (is_keeper_pick);
CREATE INDEX idx_auction_costs ON fact_draft (draft_cost);
CREATE INDEX idx_draft_performance ON fact_draft (season_points, points_per_game);

-- Fact: Player Statistics (Season-level player performance)
CREATE TABLE fact_player_statistics (
    stat_key SERIAL PRIMARY KEY,
    league_key INTEGER NOT NULL,
    player_key INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    
    -- Basic Stats
    total_fantasy_points DECIMAL(10,2) NOT NULL DEFAULT 0,
    position_type VARCHAR(10), -- O (Offense), K (Kicker), DEF (Defense)
    
    -- Performance Metrics
    games_played INTEGER,
    points_per_game DECIMAL(8,2),
    consistency_score DECIMAL(8,4), -- Standard deviation of weekly scores
    
    -- Ranking Metrics (within league/position)
    position_rank INTEGER,
    league_rank INTEGER,
    
    -- Value Metrics
    points_above_replacement DECIMAL(10,2),
    draft_value_score DECIMAL(8,4), -- Actual vs expected performance
    
    -- Source Data
    source_stat_id VARCHAR(100), -- Reference to original public.statistics record
    game_code VARCHAR(10) DEFAULT 'nfl',
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (player_key) REFERENCES dim_player(player_key),
    
    UNIQUE(league_key, player_key, season_year)
);

-- Indexes for fact_player_statistics
CREATE INDEX idx_player_stats_season ON fact_player_statistics (season_year);
CREATE INDEX idx_player_stats_league ON fact_player_statistics (league_key, season_year);
CREATE INDEX idx_player_stats_player ON fact_player_statistics (player_key, season_year);
CREATE INDEX idx_player_stats_position ON fact_player_statistics (position_type, season_year);
CREATE INDEX idx_player_stats_points ON fact_player_statistics (total_fantasy_points);
CREATE INDEX idx_player_stats_ranking ON fact_player_statistics (league_key, position_rank);
CREATE INDEX idx_player_stats_performance ON fact_player_statistics (points_per_game, consistency_score);
CREATE INDEX idx_player_stats_source ON fact_player_statistics (source_stat_id);

-- ============================================================================
-- DATA MARTS (Pre-aggregated for Web App Performance)
-- ============================================================================

-- Data Mart: League Summary Statistics
CREATE TABLE mart_league_summary (
    league_key INTEGER PRIMARY KEY,
    league_name VARCHAR(255) NOT NULL,
    season_year INTEGER NOT NULL,
    
    -- Basic Stats
    total_teams INTEGER,
    total_weeks INTEGER,
    total_games INTEGER,
    total_points DECIMAL(12,2),
    
    -- Scoring Stats
    avg_team_points DECIMAL(10,2),
    highest_team_score DECIMAL(10,2),
    lowest_team_score DECIMAL(10,2),
    total_point_differential DECIMAL(12,2),
    
    -- Parity Metrics
    competitive_balance_index DECIMAL(8,4), -- Standard deviation of win %
    avg_margin_of_victory DECIMAL(8,2),
    blowout_games_count INTEGER, -- Games with >30 point difference
    close_games_count INTEGER,   -- Games with <5 point difference
    
    -- Activity Metrics
    total_transactions INTEGER,
    avg_transactions_per_team DECIMAL(8,2),
    total_faab_spent DECIMAL(12,2),
    waiver_activity_index DECIMAL(8,4),
    
    -- Championship Info
    champion_team_key INTEGER,
    champion_final_record VARCHAR(10),
    champion_total_points DECIMAL(10,2),
    
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (champion_team_key) REFERENCES dim_team(team_key)
);

-- Data Mart: Manager Performance (Cross-league, Cross-season)
CREATE TABLE mart_manager_performance (
    manager_id VARCHAR(100) PRIMARY KEY,
    manager_name VARCHAR(255) NOT NULL,
    
    -- Career Stats
    first_season INTEGER,
    last_season INTEGER,
    total_seasons INTEGER,
    total_leagues INTEGER,
    
    -- Win/Loss Record
    total_wins INTEGER DEFAULT 0,
    total_losses INTEGER DEFAULT 0,
    total_ties INTEGER DEFAULT 0,
    career_win_percentage DECIMAL(8,4),
    
    -- Scoring
    total_points_scored DECIMAL(15,2),
    avg_points_per_game DECIMAL(10,2),
    avg_points_per_season DECIMAL(12,2),
    
    -- Championships and Playoffs
    championships_won INTEGER DEFAULT 0,
    championship_appearances INTEGER DEFAULT 0,
    playoff_appearances INTEGER DEFAULT 0,
    playoff_win_percentage DECIMAL(8,4),
    
    -- Draft Performance
    avg_draft_grade DECIMAL(4,2),
    best_draft_year INTEGER,
    worst_draft_year INTEGER,
    
    -- Transaction Activity
    total_transactions INTEGER DEFAULT 0,
    avg_transactions_per_season DECIMAL(8,2),
    faab_efficiency_rating DECIMAL(8,4),
    
    -- Consistency Metrics
    season_consistency_score DECIMAL(8,4), -- Low variance = more consistent
    best_season_record VARCHAR(10),
    worst_season_record VARCHAR(10),
    
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Mart: Player Value Analysis
CREATE TABLE mart_player_value (
    player_key INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    
    -- Basic Info
    total_seasons_rostered INTEGER,
    total_teams_rostered INTEGER,
    total_leagues_played INTEGER,
    
    -- Draft Metrics
    times_drafted INTEGER DEFAULT 0,
    avg_draft_position DECIMAL(8,2),
    earliest_draft_pick INTEGER,
    latest_draft_pick INTEGER,
    avg_auction_value DECIMAL(10,2),
    
    -- Performance
    total_fantasy_points DECIMAL(12,2),
    avg_points_per_game DECIMAL(8,2),
    best_weekly_score DECIMAL(8,2),
    worst_weekly_score DECIMAL(8,2),
    consistency_rating DECIMAL(8,4),
    
    -- Ownership
    avg_ownership_percentage DECIMAL(8,4),
    weeks_as_starter INTEGER,
    starter_percentage DECIMAL(8,4),
    
    -- Value Metrics
    points_above_replacement DECIMAL(10,2),
    draft_value_score DECIMAL(8,4), -- Actual performance vs draft cost
    waiver_pickup_value DECIMAL(8,4),
    
    PRIMARY KEY (player_key, season_year),
    FOREIGN KEY (player_key) REFERENCES dim_player(player_key)
);

CREATE INDEX idx_player_value_season ON mart_player_value (season_year);
CREATE INDEX idx_draft_metrics ON mart_player_value (avg_draft_position, times_drafted);
CREATE INDEX idx_value_performance ON mart_player_value (total_fantasy_points, consistency_rating);
CREATE INDEX idx_value_scores ON mart_player_value (draft_value_score, waiver_pickup_value);

-- Data Mart: Weekly Power Rankings
CREATE TABLE mart_weekly_power_rankings (
    league_key INTEGER NOT NULL,
    week_key INTEGER NOT NULL,
    team_key INTEGER NOT NULL,
    
    -- Rankings
    power_rank INTEGER NOT NULL,
    record_rank INTEGER,
    points_rank INTEGER,
    
    -- Metrics
    power_score DECIMAL(10,4),
    strength_of_schedule DECIMAL(8,4),
    recent_form_score DECIMAL(8,4), -- Performance in last 3 weeks
    projection_score DECIMAL(8,4),
    
    -- Movement
    rank_change INTEGER DEFAULT 0, -- vs previous week
    biggest_win_margin DECIMAL(8,2),
    biggest_loss_margin DECIMAL(8,2),
    
    -- Advanced Stats
    pythagorean_wins DECIMAL(8,2),
    luck_factor DECIMAL(8,4), -- Actual wins vs expected wins
    playoff_odds DECIMAL(8,4),
    
    PRIMARY KEY (league_key, week_key, team_key),
    FOREIGN KEY (league_key) REFERENCES dim_league(league_key),
    FOREIGN KEY (week_key) REFERENCES dim_week(week_key),
    FOREIGN KEY (team_key) REFERENCES dim_team(team_key)
);

CREATE INDEX idx_power_rankings ON mart_weekly_power_rankings (league_key, week_key, power_rank);
CREATE INDEX idx_team_progression ON mart_weekly_power_rankings (team_key, week_key);

-- Data Mart: Manager Head-to-Head Historical Performance
CREATE TABLE mart_manager_h2h (
    h2h_key SERIAL PRIMARY KEY,
    
    -- Manager Identification (alphabetical ordering to avoid duplicates)
    manager_a_name VARCHAR(255) NOT NULL,
    manager_b_name VARCHAR(255) NOT NULL,
    manager_a_id VARCHAR(100) NOT NULL,
    manager_b_id VARCHAR(100) NOT NULL,
    
    -- Game Summary
    total_matchups INTEGER DEFAULT 0,
    first_matchup_date DATE,
    last_matchup_date DATE,
    seasons_played_together INTEGER DEFAULT 0,
    leagues_played_together INTEGER DEFAULT 0,
    
    -- Manager A Performance
    manager_a_wins INTEGER DEFAULT 0,
    manager_a_losses INTEGER DEFAULT 0,
    manager_a_ties INTEGER DEFAULT 0,
    manager_a_win_percentage DECIMAL(8,4) DEFAULT 0,
    manager_a_total_points DECIMAL(12,2) DEFAULT 0,
    manager_a_avg_points DECIMAL(8,2) DEFAULT 0,
    manager_a_highest_score DECIMAL(8,2) DEFAULT 0,
    manager_a_lowest_score DECIMAL(8,2) DEFAULT 0,
    manager_a_pythagorean_wins DECIMAL(8,2) DEFAULT 0,
    manager_a_luck_factor DECIMAL(8,4) DEFAULT 0,
    manager_a_biggest_win_margin DECIMAL(8,2) DEFAULT 0,
    manager_a_current_streak INTEGER DEFAULT 0,
    
    -- Manager B Performance
    manager_b_wins INTEGER DEFAULT 0,
    manager_b_losses INTEGER DEFAULT 0,
    manager_b_ties INTEGER DEFAULT 0,
    manager_b_win_percentage DECIMAL(8,4) DEFAULT 0,
    manager_b_total_points DECIMAL(12,2) DEFAULT 0,
    manager_b_avg_points DECIMAL(8,2) DEFAULT 0,
    manager_b_highest_score DECIMAL(8,2) DEFAULT 0,
    manager_b_lowest_score DECIMAL(8,2) DEFAULT 0,
    manager_b_pythagorean_wins DECIMAL(8,2) DEFAULT 0,
    manager_b_luck_factor DECIMAL(8,4) DEFAULT 0,
    manager_b_biggest_win_margin DECIMAL(8,2) DEFAULT 0,
    manager_b_current_streak INTEGER DEFAULT 0,
    
    -- Head-to-Head Analysis
    series_leader VARCHAR(255),
    series_record VARCHAR(20),
    point_differential DECIMAL(10,2) DEFAULT 0,
    avg_point_differential DECIMAL(8,2) DEFAULT 0,
    most_lopsided_game DECIMAL(8,2) DEFAULT 0,
    closest_game DECIMAL(8,2) DEFAULT 999.99,
    total_points_in_series DECIMAL(12,2) DEFAULT 0,
    avg_total_points_per_game DECIMAL(8,2) DEFAULT 0,
    high_scoring_games INTEGER DEFAULT 0,
    low_scoring_games INTEGER DEFAULT 0,
    
    -- Playoff & Championship Tracking
    playoff_matchups INTEGER DEFAULT 0,
    championship_matchups INTEGER DEFAULT 0,
    semifinal_matchups INTEGER DEFAULT 0,
    manager_a_playoff_wins INTEGER DEFAULT 0,
    manager_a_championship_wins INTEGER DEFAULT 0,
    manager_a_semifinal_wins INTEGER DEFAULT 0,
    manager_b_playoff_wins INTEGER DEFAULT 0,
    manager_b_championship_wins INTEGER DEFAULT 0,
    manager_b_semifinal_wins INTEGER DEFAULT 0,
    
    -- Most Important Game Ever
    most_important_game_type VARCHAR(20),
    most_important_game_date DATE,
    most_important_game_winner VARCHAR(255),
    most_important_game_score VARCHAR(100),
    most_important_game_margin DECIMAL(8,2),
    most_important_game_season INTEGER,
    most_important_game_week INTEGER,
    most_important_game_league VARCHAR(255),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(manager_a_name, manager_b_name),
    CHECK (manager_a_name < manager_b_name) -- Ensure alphabetical ordering
);

-- Indexes for mart_manager_h2h
CREATE INDEX idx_h2h_managers ON mart_manager_h2h (manager_a_name, manager_b_name);
CREATE INDEX idx_h2h_manager_a ON mart_manager_h2h (manager_a_name);
CREATE INDEX idx_h2h_manager_b ON mart_manager_h2h (manager_b_name);
CREATE INDEX idx_h2h_series_leader ON mart_manager_h2h (series_leader);
CREATE INDEX idx_h2h_playoff_games ON mart_manager_h2h (playoff_matchups, championship_matchups);
CREATE INDEX idx_h2h_important_game ON mart_manager_h2h (most_important_game_type, most_important_game_date);
CREATE INDEX idx_h2h_matchup_count ON mart_manager_h2h (total_matchups);

-- ============================================================================
-- OPTIMIZED VIEWS FOR WEB APPLICATION
-- ============================================================================

-- View: Current Season Dashboard
CREATE VIEW vw_current_season_dashboard AS
SELECT 
    dl.league_name,
    dl.season_year,
    dt.team_name,
    dt.manager_name,
    ftp.wins,
    ftp.losses,
    ftp.ties,
    ftp.points_for,
    ftp.points_against,
    ftp.point_differential,
    ftp.win_percentage,
    ftp.season_rank,
    ftp.playoff_probability,
    ftp.is_playoff_team,
    ftp.playoff_seed
FROM fact_team_performance ftp
JOIN dim_team dt ON ftp.team_key = dt.team_key
JOIN dim_league dl ON ftp.league_key = dl.league_key
JOIN dim_week dw ON ftp.week_key = dw.week_key
WHERE dl.season_year = EXTRACT(YEAR FROM CURRENT_DATE)
  AND dw.is_current_week = TRUE
  AND dt.is_active = TRUE
ORDER BY dl.league_name, ftp.season_rank;

-- View: Manager Hall of Fame
CREATE VIEW vw_manager_hall_of_fame AS
SELECT 
    manager_name,
    total_seasons,
    championships_won,
    career_win_percentage,
    total_points_scored,
    avg_points_per_season,
    playoff_appearances,
    season_consistency_score,
    RANK() OVER (ORDER BY championships_won DESC, career_win_percentage DESC) as hall_of_fame_rank
FROM mart_manager_performance
WHERE total_seasons >= 3 -- Minimum 3 seasons to qualify
ORDER BY hall_of_fame_rank;

-- View: League Competitiveness Analysis
CREATE VIEW vw_league_competitiveness AS
SELECT 
    mls.league_name,
    mls.season_year,
    mls.competitive_balance_index,
    mls.avg_margin_of_victory,
    mls.close_games_count,
    mls.blowout_games_count,
    mls.total_transactions,
    mls.waiver_activity_index,
    CASE 
        WHEN mls.competitive_balance_index < 0.15 THEN 'Highly Competitive'
        WHEN mls.competitive_balance_index < 0.25 THEN 'Competitive'
        WHEN mls.competitive_balance_index < 0.35 THEN 'Moderately Competitive'
        ELSE 'Low Competition'
    END as competitiveness_tier
FROM mart_league_summary mls
ORDER BY mls.competitive_balance_index ASC;

-- View: Player Breakout Analysis
CREATE VIEW vw_player_breakout_analysis AS
SELECT 
    dp.player_name,
    dp.primary_position,
    mpv.season_year,
    mpv.avg_draft_position,
    mpv.total_fantasy_points,
    mpv.draft_value_score,
    mpv.waiver_pickup_value,
    CASE 
        WHEN mpv.avg_draft_position > 100 AND mpv.draft_value_score > 2.0 THEN 'Major Breakout'
        WHEN mpv.avg_draft_position > 50 AND mpv.draft_value_score > 1.5 THEN 'Solid Breakout'
        WHEN mpv.waiver_pickup_value > 1.5 THEN 'Waiver Wire Gem'
        ELSE 'Standard Performance'
    END as breakout_type
FROM mart_player_value mpv
JOIN dim_player dp ON mpv.player_key = dp.player_key
WHERE mpv.draft_value_score > 1.3 OR mpv.waiver_pickup_value > 1.3
ORDER BY mpv.draft_value_score DESC;

-- View: Trade Analysis
CREATE VIEW vw_trade_analysis AS
SELECT 
    dl.league_name,
    dl.season_year,
    ft.transaction_date,
    dp.player_name,
    dt1.team_name as from_team,
    dt1.manager_name as from_manager,
    dt2.team_name as to_team,
    dt2.manager_name as to_manager,
    ft.trade_group_id,
    -- Could add win-win analysis, value comparison, etc.
    COUNT(*) OVER (PARTITION BY ft.trade_group_id) as players_in_trade
FROM fact_transaction ft
JOIN dim_league dl ON ft.league_key = dl.league_key
JOIN dim_player dp ON ft.player_key = dp.player_key
JOIN dim_team dt1 ON ft.from_team_key = dt1.team_key
JOIN dim_team dt2 ON ft.to_team_key = dt2.team_key
WHERE ft.transaction_type = 'trade'
ORDER BY ft.transaction_date DESC;

-- ============================================================================
-- ADDITIONAL INDEXES FOR PERFORMANCE OPTIMIZATION
-- ============================================================================

-- Composite indexes for common query patterns
CREATE INDEX idx_team_performance_weekly ON fact_team_performance (team_key, season_year, week_key);
CREATE INDEX idx_matchup_outcomes ON fact_matchup (league_key, season_year, winner_team_key);
CREATE INDEX idx_roster_starters ON fact_roster (team_key, week_key, is_starter);
CREATE INDEX idx_transaction_timeline ON fact_transaction (league_key, transaction_date, transaction_type);
CREATE INDEX idx_draft_value ON fact_draft (league_key, season_year, draft_cost, season_points);

-- ============================================================================
-- DATA QUALITY CONSTRAINTS
-- ============================================================================

-- Business rule constraints
ALTER TABLE fact_team_performance ADD CONSTRAINT chk_win_percentage 
    CHECK (win_percentage >= 0 AND win_percentage <= 1);

ALTER TABLE fact_matchup ADD CONSTRAINT chk_point_difference 
    CHECK (point_difference = ABS(team1_points - team2_points));

ALTER TABLE fact_roster ADD CONSTRAINT chk_weekly_points 
    CHECK (weekly_points >= 0);

ALTER TABLE fact_draft ADD CONSTRAINT chk_draft_position 
    CHECK (overall_pick > 0 AND round_number > 0 AND pick_in_round > 0);

-- ============================================================================
-- COMMENTS AND DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE dim_season IS 'Season dimension with complete season metadata';
COMMENT ON TABLE dim_league IS 'League dimension with SCD Type 2 change tracking';
COMMENT ON TABLE dim_team IS 'Team dimension with manager and league relationships';
COMMENT ON TABLE dim_player IS 'Player dimension with position and team info';
COMMENT ON TABLE dim_week IS 'Week dimension for granular time analysis';

COMMENT ON TABLE fact_team_performance IS 'Weekly team performance metrics and rankings';
COMMENT ON TABLE fact_matchup IS 'Head-to-head matchup results and statistics';
COMMENT ON TABLE fact_roster IS 'Player ownership and roster decisions';
COMMENT ON TABLE fact_transaction IS 'All player movement transactions';
COMMENT ON TABLE fact_draft IS 'Draft results with value analysis';
COMMENT ON TABLE fact_player_statistics IS 'Season-level player fantasy statistics and performance metrics';

COMMENT ON TABLE mart_league_summary IS 'Pre-aggregated league statistics for dashboards';
COMMENT ON TABLE mart_manager_performance IS 'Career manager statistics across all leagues';
COMMENT ON TABLE mart_player_value IS 'Player performance and value metrics by season';
COMMENT ON TABLE mart_weekly_power_rankings IS 'Advanced team rankings and projections';
COMMENT ON TABLE mart_manager_h2h IS 'Head-to-head historical performance between manager pairs';

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================ 
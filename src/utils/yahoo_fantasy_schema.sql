-- Yahoo Fantasy Football Database Schema
-- Generated for comprehensive data storage

-- Main leagues table
CREATE TABLE leagues (
    league_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    season VARCHAR(4) NOT NULL,
    game_code VARCHAR(10) NOT NULL,
    game_id INTEGER NOT NULL,
    num_teams INTEGER NOT NULL,
    current_week INTEGER,
    start_week INTEGER,
    end_week INTEGER,
    league_type VARCHAR(50),
    draft_status VARCHAR(50),
    is_pro_league BOOLEAN DEFAULT FALSE,
    is_cash_league BOOLEAN DEFAULT FALSE,
    url VARCHAR(500),
    logo_url VARCHAR(500),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_season (season),
    INDEX idx_game_id (game_id),
    INDEX idx_league_type (league_type)
);

-- Teams table
CREATE TABLE teams (
    team_id VARCHAR(50) PRIMARY KEY,
    league_id VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    manager_name VARCHAR(255),
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    ties INTEGER DEFAULT 0,
    points_for DECIMAL(10,2) DEFAULT 0.00,
    points_against DECIMAL(10,2) DEFAULT 0.00,
    playoff_seed INTEGER,
    waiver_priority INTEGER,
    faab_balance DECIMAL(10,2),
    team_logo_url VARCHAR(500),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_id) REFERENCES leagues(league_id) ON DELETE CASCADE,
    INDEX idx_league_teams (league_id),
    INDEX idx_manager (manager_name),
    INDEX idx_wins_losses (wins, losses)
);

-- Rosters table - ENABLED for roster data storage
CREATE TABLE rosters (
    roster_id VARCHAR(100) PRIMARY KEY,
    team_id VARCHAR(50) NOT NULL,
    league_id VARCHAR(50) NOT NULL,
    week INTEGER NOT NULL,
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(255) NOT NULL,
    position VARCHAR(20),
    eligible_positions TEXT,
    status VARCHAR(50),
    is_starter BOOLEAN DEFAULT FALSE,
    acquisition_date DATE,
    acquisition_type VARCHAR(50),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (team_id) REFERENCES teams(team_id) ON DELETE CASCADE,
    FOREIGN KEY (league_id) REFERENCES leagues(league_id) ON DELETE CASCADE,
    INDEX idx_team_week (team_id, week),
    INDEX idx_player (player_id),
    INDEX idx_league_week (league_id, week),
    INDEX idx_position (position),
    INDEX idx_starters (is_starter),
    INDEX idx_status (status),
    INDEX idx_acquisition (acquisition_type)
);

-- Matchups/Schedule table
CREATE TABLE matchups (
    matchup_id VARCHAR(100) PRIMARY KEY,
    league_id VARCHAR(50) NOT NULL,
    week INTEGER NOT NULL,
    team1_id VARCHAR(50) NOT NULL,
    team2_id VARCHAR(50) NOT NULL,
    team1_score DECIMAL(10,2) DEFAULT 0.00,
    team2_score DECIMAL(10,2) DEFAULT 0.00,
    winner_team_id VARCHAR(50),
    is_playoffs BOOLEAN DEFAULT FALSE,
    is_championship BOOLEAN DEFAULT FALSE,
    is_consolation BOOLEAN DEFAULT FALSE,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_id) REFERENCES leagues(league_id) ON DELETE CASCADE,
    FOREIGN KEY (team1_id) REFERENCES teams(team_id) ON DELETE CASCADE,
    FOREIGN KEY (team2_id) REFERENCES teams(team_id) ON DELETE CASCADE,
    FOREIGN KEY (winner_team_id) REFERENCES teams(team_id) ON DELETE SET NULL,
    INDEX idx_league_week (league_id, week),
    INDEX idx_team1_matchups (team1_id),
    INDEX idx_team2_matchups (team2_id),
    INDEX idx_playoffs (is_playoffs),
    INDEX idx_championship (is_championship)
);

-- Transactions table (trades, adds, drops, waivers)
CREATE TABLE transactions (
    transaction_id VARCHAR(100) PRIMARY KEY,
    league_id VARCHAR(50) NOT NULL,
    type VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(255) NOT NULL,
    source_team_id VARCHAR(50),
    destination_team_id VARCHAR(50),
    faab_bid DECIMAL(10,2),
    status VARCHAR(50),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_id) REFERENCES leagues(league_id) ON DELETE CASCADE,
    FOREIGN KEY (source_team_id) REFERENCES teams(team_id) ON DELETE SET NULL,
    FOREIGN KEY (destination_team_id) REFERENCES teams(team_id) ON DELETE SET NULL,
    INDEX idx_league_transactions (league_id),
    INDEX idx_transaction_type (type),
    INDEX idx_player_transactions (player_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_faab_bids (faab_bid)
);

-- Draft picks table (draft history and results)
CREATE TABLE draft_picks (
    draft_pick_id VARCHAR(100) PRIMARY KEY,
    league_id VARCHAR(50) NOT NULL,
    pick_number INTEGER NOT NULL,
    round_number INTEGER NOT NULL,
    team_id VARCHAR(50) NOT NULL,
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(255) NOT NULL,
    position VARCHAR(20),
    cost DECIMAL(10,2),
    is_keeper BOOLEAN DEFAULT FALSE,
    is_auction_draft BOOLEAN DEFAULT FALSE,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_id) REFERENCES leagues(league_id) ON DELETE CASCADE,
    FOREIGN KEY (team_id) REFERENCES teams(team_id) ON DELETE CASCADE,
    INDEX idx_league_draft (league_id),
    INDEX idx_draft_round (league_id, round_number),
    INDEX idx_draft_pick_order (league_id, pick_number),
    INDEX idx_player_draft (player_id),
    INDEX idx_team_draft (team_id),
    INDEX idx_position_draft (position),
    INDEX idx_auction_draft (is_auction_draft),
    INDEX idx_keeper_picks (is_keeper)
);

-- Player statistics table (real fantasy points performance data)
CREATE TABLE statistics (
    stat_id VARCHAR(100) PRIMARY KEY,
    league_id VARCHAR(50) NOT NULL,
    player_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(255) NOT NULL,
    position_type VARCHAR(20) NOT NULL,
    season_year INTEGER NOT NULL,
    total_fantasy_points DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    game_code VARCHAR(10) NOT NULL DEFAULT 'nfl',
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (league_id) REFERENCES leagues(league_id) ON DELETE CASCADE,
    INDEX idx_league_stats (league_id),
    INDEX idx_player_stats (player_id),
    INDEX idx_season_stats (season_year),
    INDEX idx_position_stats (position_type),
    INDEX idx_fantasy_points (total_fantasy_points),
    INDEX idx_league_season (league_id, season_year),
    INDEX idx_player_season (player_id, season_year),
    UNIQUE KEY unique_player_league_season (player_id, league_id, season_year)
);



-- Views for common queries

-- League summary view
CREATE VIEW league_summary AS
SELECT 
    l.league_id,
    l.name as league_name,
    l.season,
    l.num_teams,
    COUNT(DISTINCT t.team_id) as actual_teams,
    COUNT(DISTINCT m.matchup_id) as total_matchups,
    COUNT(DISTINCT tr.transaction_id) as total_transactions,
    AVG(t.points_for) as avg_points_for,
    MAX(t.points_for) as highest_points,
    MIN(t.points_for) as lowest_points
FROM leagues l
LEFT JOIN teams t ON l.league_id = t.league_id
LEFT JOIN matchups m ON l.league_id = m.league_id  
LEFT JOIN transactions tr ON l.league_id = tr.league_id
GROUP BY l.league_id, l.name, l.season, l.num_teams;

-- Team performance view
CREATE VIEW team_performance AS
SELECT 
    t.team_id,
    t.name as team_name,
    t.manager_name,
    l.name as league_name,
    l.season,
    t.wins,
    t.losses,
    t.ties,
    t.points_for,
    t.points_against,
    (t.points_for - t.points_against) as point_differential,
    ROUND(t.wins * 100.0 / NULLIF(t.wins + t.losses + t.ties, 0), 2) as win_percentage,
    t.playoff_seed
FROM teams t
JOIN leagues l ON t.league_id = l.league_id;

-- Manager history view
CREATE VIEW manager_history AS
SELECT 
    t.manager_name,
    COUNT(DISTINCT l.league_id) as leagues_participated,
    COUNT(DISTINCT l.season) as seasons_played,
    MIN(l.season) as first_season,
    MAX(l.season) as last_season,
    SUM(t.wins) as total_wins,
    SUM(t.losses) as total_losses,
    SUM(t.ties) as total_ties,
    AVG(t.points_for) as avg_points_per_season,
    COUNT(CASE WHEN t.playoff_seed = 1 THEN 1 END) as championships_won
FROM teams t
JOIN leagues l ON t.league_id = l.league_id
WHERE t.manager_name IS NOT NULL AND t.manager_name != ''
GROUP BY t.manager_name;

-- Weekly matchup results view  
CREATE VIEW weekly_results AS
SELECT 
    m.league_id,
    l.name as league_name,
    l.season,
    m.week,
    t1.name as team1_name,
    t1.manager_name as team1_manager,
    m.team1_score,
    t2.name as team2_name, 
    t2.manager_name as team2_manager,
    m.team2_score,
    CASE 
        WHEN m.winner_team_id = m.team1_id THEN t1.name
        WHEN m.winner_team_id = m.team2_id THEN t2.name
        ELSE 'Tie'
    END as winner,
    ABS(m.team1_score - m.team2_score) as point_difference,
    m.is_playoffs,
    m.is_championship
FROM matchups m
JOIN leagues l ON m.league_id = l.league_id
JOIN teams t1 ON m.team1_id = t1.team_id
JOIN teams t2 ON m.team2_id = t2.team_id;

-- Transaction activity view
CREATE VIEW transaction_activity AS
SELECT 
    tr.league_id,
    l.name as league_name,
    l.season,
    tr.type as transaction_type,
    COUNT(*) as transaction_count,
    AVG(tr.faab_bid) as avg_faab_bid,
    MAX(tr.faab_bid) as max_faab_bid
FROM transactions tr
JOIN leagues l ON tr.league_id = l.league_id
GROUP BY tr.league_id, l.name, l.season, tr.type;

-- Draft analysis view
CREATE VIEW draft_analysis AS
SELECT 
    dp.league_id,
    l.name as league_name,
    l.season,
    dp.round_number,
    dp.position,
    COUNT(*) as picks_count,
    AVG(dp.cost) as avg_cost,
    MIN(dp.cost) as min_cost,
    MAX(dp.cost) as max_cost,
    COUNT(CASE WHEN dp.is_keeper THEN 1 END) as keeper_picks,
    dp.is_auction_draft
FROM draft_picks dp
JOIN leagues l ON dp.league_id = l.league_id
GROUP BY dp.league_id, l.name, l.season, dp.round_number, dp.position, dp.is_auction_draft;

-- Team draft summary view
CREATE VIEW team_draft_summary AS
SELECT 
    dp.team_id,
    t.name as team_name,
    t.manager_name,
    l.name as league_name,
    l.season,
    COUNT(*) as total_picks,
    COUNT(CASE WHEN dp.position = 'QB' THEN 1 END) as qb_picks,
    COUNT(CASE WHEN dp.position = 'RB' THEN 1 END) as rb_picks,
    COUNT(CASE WHEN dp.position = 'WR' THEN 1 END) as wr_picks,
    COUNT(CASE WHEN dp.position = 'TE' THEN 1 END) as te_picks,
    COUNT(CASE WHEN dp.position IN ('K', 'DEF') THEN 1 END) as special_picks,
    SUM(dp.cost) as total_draft_cost,
    AVG(dp.cost) as avg_pick_cost,
    COUNT(CASE WHEN dp.is_keeper THEN 1 END) as keeper_count
FROM draft_picks dp
JOIN teams t ON dp.team_id = t.team_id
JOIN leagues l ON dp.league_id = l.league_id
GROUP BY dp.team_id, t.name, t.manager_name, l.name, l.season;

-- Player draft history view
CREATE VIEW player_draft_history AS
SELECT 
    dp.player_id,
    dp.player_name,
    dp.position,
    COUNT(*) as times_drafted,
    AVG(dp.pick_number) as avg_pick_position,
    MIN(dp.pick_number) as highest_pick,
    MAX(dp.pick_number) as lowest_pick,
    AVG(dp.round_number) as avg_round,
    MIN(l.season) as first_drafted_season,
    MAX(l.season) as last_drafted_season,
    AVG(dp.cost) as avg_auction_cost,
    MAX(dp.cost) as max_auction_cost
FROM draft_picks dp
JOIN leagues l ON dp.league_id = l.league_id
GROUP BY dp.player_id, dp.player_name, dp.position;

-- Comments and documentation
-- This schema supports:
-- 1. Complete league hierarchy (leagues -> teams -> rosters)
-- 2. Full season tracking (matchups by week)
-- 3. Transaction history (trades, waivers, adds/drops)
-- 4. Draft history and analysis (snake and auction drafts)
-- 5. Manager performance analysis across seasons
-- 6. Statistical analysis capabilities
-- 7. Playoff and championship tracking
-- 8. FAAB (Free Agent Auction Budget) tracking
-- 9. Keeper league support
-- 10. Extensible for additional statistics

-- Indexes are optimized for:
-- - League-based queries (most common)
-- - Weekly data analysis
-- - Manager performance lookups
-- - Transaction history searches
-- - Draft analysis and player tracking
-- - Cross-season comparisons

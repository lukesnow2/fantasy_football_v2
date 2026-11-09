-- Describe playoff odds as the simulation they are (WP4 S4.3).
--
-- The Data Dictionary's playoff_probability entry still described the
-- retired ladder model ("Record (40%) + Points Rank (30%) + Schedule (20%)
-- + Time Remaining (10%)"). This Season's "Playoff odds" StatTip reads this
-- row, so its short description and calculation now say what the model does,
-- in the This Season board's "How these odds work" wording.
--
-- Idempotent: safe to re-run.

BEGIN;

UPDATE meta_data.metric_definitions
SET short_description = 'The share of 10,000 simulated seasons in which a team makes the playoffs.',
    calculation_formula = 'Each team gets a projected score, pulled toward the league average early in the year. The rest of the season is played 10,000 times with that score drifting week to week, and seeds go by wins, then points for. A team''s odds are the share of those runs where it makes the field.',
    updated_at = CURRENT_TIMESTAMP
WHERE metric_id = 'playoff_probability';

COMMIT;

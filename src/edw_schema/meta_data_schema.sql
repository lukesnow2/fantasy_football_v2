-- ============================================================================
-- FANTASY FOOTBALL DATA DICTIONARY & METRIC DEFINITIONS SCHEMA
-- ============================================================================
-- Stores comprehensive definitions, formulas, and metadata for all metrics
-- Used to provide contextual help and documentation to users
-- 
-- Author: Luke Snow
-- Date: 2025-01-27
-- ============================================================================

-- Create dedicated schema for metadata
CREATE SCHEMA IF NOT EXISTS meta_data;

-- ============================================================================
-- CORE DEFINITION TABLES
-- ============================================================================

-- Main metric definitions table
CREATE TABLE meta_data.metric_definitions (
    metric_id VARCHAR(100) PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_category VARCHAR(50) NOT NULL, 
    short_description VARCHAR(500) NOT NULL,
    detailed_description TEXT,
    calculation_formula TEXT,
    example_calculation TEXT,
    interpretation_guide TEXT,
    limitations TEXT, -- What the metric does not capture and where it misleads
    data_type VARCHAR(20) NOT NULL, -- 'percentage', 'decimal', 'integer', 'rating', 'score'
    unit_of_measure VARCHAR(50), -- 'points', 'games', 'percentage', 'dollars', etc.
    typical_range VARCHAR(100), -- '0-100', '0.000-1.000', 'varies', etc.
    good_value_threshold DECIMAL(10,4), -- What's considered a "good" value
    excellent_value_threshold DECIMAL(10,4), -- What's considered an "excellent" value
    display_format VARCHAR(50), -- 'decimal_1', 'percentage_1', 'integer', 'currency'
    sort_order INTEGER DEFAULT 999, -- For ordering within categories
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    is_active BOOLEAN DEFAULT TRUE,
    
    CONSTRAINT valid_data_type CHECK (data_type IN ('percentage', 'decimal', 'integer', 'rating', 'score', 'text', 'date')),
    CONSTRAINT valid_category CHECK (metric_category IN (
        'manager_performance', 'team_performance', 'league_analysis', 'competitiveness', 
        'trade_analysis', 'draft_analysis', 'player_value', 'head_to_head', 
        'playoff_analysis', 'consistency', 'efficiency', 'record_book'
    ))
);

-- API endpoint definitions
CREATE TABLE meta_data.api_definitions (
    endpoint_path VARCHAR(255) PRIMARY KEY,
    endpoint_name VARCHAR(255) NOT NULL,
    endpoint_description TEXT,
    method VARCHAR(10) DEFAULT 'GET',
    response_structure JSONB, -- Store sample response structure
    query_parameters JSONB, -- Available query parameters
    example_request TEXT,
    example_response TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Field-level definitions for API responses
CREATE TABLE meta_data.field_definitions (
    field_id VARCHAR(100) PRIMARY KEY,
    field_name VARCHAR(100) NOT NULL,
    api_endpoint VARCHAR(255),
    metric_id VARCHAR(100), -- Links to metric_definitions
    field_description TEXT,
    is_required BOOLEAN DEFAULT FALSE,
    default_value TEXT,
    validation_rules TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (api_endpoint) REFERENCES meta_data.api_definitions(endpoint_path) ON DELETE CASCADE,
    FOREIGN KEY (metric_id) REFERENCES meta_data.metric_definitions(metric_id) ON DELETE SET NULL
);

-- Category definitions for organizing metrics
CREATE TABLE meta_data.metric_categories (
    category_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL,
    category_description TEXT,
    display_order INTEGER DEFAULT 999,
    icon_name VARCHAR(50), -- For UI icons
    color_scheme VARCHAR(20), -- For UI theming
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Metric relationships (for showing related metrics)
CREATE TABLE meta_data.metric_relationships (
    relationship_id SERIAL PRIMARY KEY,
    primary_metric_id VARCHAR(100) NOT NULL,
    related_metric_id VARCHAR(100) NOT NULL,
    relationship_type VARCHAR(50) NOT NULL, -- 'component_of', 'related_to', 'inverse_of', 'derived_from'
    relationship_description TEXT,
    strength INTEGER DEFAULT 5, -- 1-10 scale for how related they are
    
    FOREIGN KEY (primary_metric_id) REFERENCES meta_data.metric_definitions(metric_id) ON DELETE CASCADE,
    FOREIGN KEY (related_metric_id) REFERENCES meta_data.metric_definitions(metric_id) ON DELETE CASCADE,
    UNIQUE(primary_metric_id, related_metric_id, relationship_type)
);

-- Historical definition changes (for version control)
CREATE TABLE meta_data.definition_history (
    history_id SERIAL PRIMARY KEY,
    metric_id VARCHAR(100) NOT NULL,
    field_name VARCHAR(100) NOT NULL,
    old_value TEXT,
    new_value TEXT,
    change_reason TEXT,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (metric_id) REFERENCES meta_data.metric_definitions(metric_id) ON DELETE CASCADE
);

-- User feedback on definitions (for continuous improvement)
CREATE TABLE meta_data.definition_feedback (
    feedback_id SERIAL PRIMARY KEY,
    metric_id VARCHAR(100) NOT NULL,
    feedback_type VARCHAR(20) NOT NULL, -- 'unclear', 'incorrect', 'suggestion', 'helpful'
    feedback_text TEXT,
    user_identifier VARCHAR(100), -- Could be session ID, user ID, etc.
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_reviewed BOOLEAN DEFAULT FALSE,
    reviewer_notes TEXT,
    
    FOREIGN KEY (metric_id) REFERENCES meta_data.metric_definitions(metric_id) ON DELETE CASCADE,
    CONSTRAINT valid_feedback_type CHECK (feedback_type IN ('unclear', 'incorrect', 'suggestion', 'helpful', 'outdated'))
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Primary lookup indexes
CREATE INDEX idx_metric_definitions_category ON meta_data.metric_definitions(metric_category);
CREATE INDEX idx_metric_definitions_active ON meta_data.metric_definitions(is_active);
CREATE INDEX idx_metric_definitions_name ON meta_data.metric_definitions(metric_name);

-- API definitions indexes
CREATE INDEX idx_api_definitions_active ON meta_data.api_definitions(is_active);

-- Field definitions indexes
CREATE INDEX idx_field_definitions_endpoint ON meta_data.field_definitions(api_endpoint);
CREATE INDEX idx_field_definitions_metric ON meta_data.field_definitions(metric_id);

-- Relationships indexes
CREATE INDEX idx_metric_relationships_primary ON meta_data.metric_relationships(primary_metric_id);
CREATE INDEX idx_metric_relationships_related ON meta_data.metric_relationships(related_metric_id);
CREATE INDEX idx_metric_relationships_type ON meta_data.metric_relationships(relationship_type);

-- History and feedback indexes
CREATE INDEX idx_definition_history_metric ON meta_data.definition_history(metric_id);
CREATE INDEX idx_definition_history_date ON meta_data.definition_history(changed_at);
CREATE INDEX idx_definition_feedback_metric ON meta_data.definition_feedback(metric_id);
CREATE INDEX idx_definition_feedback_unreviewed ON meta_data.definition_feedback(is_reviewed);

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION meta_data.update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for automatic timestamp updates
CREATE TRIGGER update_metric_definitions_modtime 
    BEFORE UPDATE ON meta_data.metric_definitions 
    FOR EACH ROW EXECUTE FUNCTION meta_data.update_modified_column();

CREATE TRIGGER update_api_definitions_modtime 
    BEFORE UPDATE ON meta_data.api_definitions 
    FOR EACH ROW EXECUTE FUNCTION meta_data.update_modified_column();

-- Function to log definition changes
CREATE OR REPLACE FUNCTION meta_data.log_definition_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Log changes to key fields
    IF OLD.short_description != NEW.short_description THEN
        INSERT INTO meta_data.definition_history (metric_id, field_name, old_value, new_value, changed_by)
        VALUES (NEW.metric_id, 'short_description', OLD.short_description, NEW.short_description, COALESCE(current_setting('app.current_user', true), 'system'));
    END IF;
    
    IF OLD.calculation_formula != NEW.calculation_formula THEN
        INSERT INTO meta_data.definition_history (metric_id, field_name, old_value, new_value, changed_by)
        VALUES (NEW.metric_id, 'calculation_formula', OLD.calculation_formula, NEW.calculation_formula, COALESCE(current_setting('app.current_user', true), 'system'));
    END IF;
    
    IF OLD.detailed_description != NEW.detailed_description THEN
        INSERT INTO meta_data.definition_history (metric_id, field_name, old_value, new_value, changed_by)
        VALUES (NEW.metric_id, 'detailed_description', OLD.detailed_description, NEW.detailed_description, COALESCE(current_setting('app.current_user', true), 'system'));
    END IF;
    
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for logging changes
CREATE TRIGGER log_metric_definition_changes 
    AFTER UPDATE ON meta_data.metric_definitions 
    FOR EACH ROW EXECUTE FUNCTION meta_data.log_definition_changes();

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View for active metric definitions with category info
CREATE VIEW meta_data.vw_active_metrics AS
SELECT 
    md.metric_id,
    md.metric_name,
    md.metric_category,
    mc.category_name,
    mc.category_description,
    md.short_description,
    md.detailed_description,
    md.calculation_formula,
    md.example_calculation,
    md.interpretation_guide,
    md.limitations,
    md.data_type,
    md.unit_of_measure,
    md.typical_range,
    md.good_value_threshold,
    md.excellent_value_threshold,
    md.display_format,
    md.sort_order,
    mc.display_order as category_order,
    mc.icon_name as category_icon,
    mc.color_scheme as category_color
FROM meta_data.metric_definitions md
LEFT JOIN meta_data.metric_categories mc ON md.metric_category = mc.category_id
WHERE md.is_active = TRUE
ORDER BY mc.display_order, md.sort_order, md.metric_name;

-- View for API documentation
CREATE VIEW meta_data.vw_api_documentation AS
SELECT 
    ad.endpoint_path,
    ad.endpoint_name,
    ad.endpoint_description,
    ad.method,
    ad.query_parameters,
    ad.example_request,
    ad.example_response,
    COALESCE(
        json_agg(
            json_build_object(
                'field_name', fd.field_name,
                'description', fd.field_description,
                'metric_id', fd.metric_id,
                'metric_name', md.metric_name,
                'is_required', fd.is_required,
                'default_value', fd.default_value
            ) ORDER BY fd.field_name
        ) FILTER (WHERE fd.field_id IS NOT NULL),
        '[]'::json
    ) as field_definitions
FROM meta_data.api_definitions ad
LEFT JOIN meta_data.field_definitions fd ON ad.endpoint_path = fd.api_endpoint
LEFT JOIN meta_data.metric_definitions md ON fd.metric_id = md.metric_id
WHERE ad.is_active = TRUE
GROUP BY ad.endpoint_path, ad.endpoint_name, ad.endpoint_description, ad.method, 
         ad.query_parameters, ad.example_request, ad.example_response
ORDER BY ad.endpoint_path;

-- View for metric relationships
CREATE VIEW meta_data.vw_metric_relationships AS
SELECT 
    mr.primary_metric_id,
    pm.metric_name as primary_metric_name,
    mr.related_metric_id,
    rm.metric_name as related_metric_name,
    mr.relationship_type,
    mr.relationship_description,
    mr.strength,
    pm.metric_category as primary_category,
    rm.metric_category as related_category
FROM meta_data.metric_relationships mr
JOIN meta_data.metric_definitions pm ON mr.primary_metric_id = pm.metric_id
JOIN meta_data.metric_definitions rm ON mr.related_metric_id = rm.metric_id
WHERE pm.is_active = TRUE AND rm.is_active = TRUE
ORDER BY mr.strength DESC, pm.metric_name;

-- ============================================================================
-- GRANTS AND PERMISSIONS
-- ============================================================================

-- Grant appropriate permissions (adjust based on your user roles)
-- GRANT USAGE ON SCHEMA meta_data TO application_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA meta_data TO application_user;
-- GRANT INSERT, UPDATE ON meta_data.definition_feedback TO application_user;

-- For admin users who can manage definitions
-- GRANT ALL PRIVILEGES ON SCHEMA meta_data TO admin_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA meta_data TO admin_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA meta_data TO admin_user;

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON SCHEMA meta_data IS 'Schema for storing metric definitions and data dictionary information';
COMMENT ON TABLE meta_data.metric_definitions IS 'Core table storing definitions for all metrics displayed to users';
COMMENT ON TABLE meta_data.api_definitions IS 'Documentation for API endpoints and their structure';
COMMENT ON TABLE meta_data.field_definitions IS 'Field-level documentation for API responses';
COMMENT ON TABLE meta_data.metric_categories IS 'Categories for organizing metrics in the UI';
COMMENT ON TABLE meta_data.metric_relationships IS 'Relationships between metrics for showing related information';
COMMENT ON TABLE meta_data.definition_history IS 'Audit trail for changes to metric definitions';
COMMENT ON TABLE meta_data.definition_feedback IS 'User feedback on metric definitions for continuous improvement';

COMMENT ON COLUMN meta_data.metric_definitions.metric_id IS 'Unique identifier matching field names in API responses';
COMMENT ON COLUMN meta_data.metric_definitions.calculation_formula IS 'Human-readable formula showing how the metric is calculated';
COMMENT ON COLUMN meta_data.metric_definitions.example_calculation IS 'Concrete example with actual numbers';
COMMENT ON COLUMN meta_data.metric_definitions.interpretation_guide IS 'How to interpret values (what makes a good vs bad score)';
COMMENT ON COLUMN meta_data.metric_definitions.display_format IS 'How to format the value for display (decimal places, percentage, etc)';

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

DO $$ 
BEGIN
    RAISE NOTICE 'Meta-data schema created successfully!';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Populate metric_categories table';
    RAISE NOTICE '2. Add metric definitions for your key metrics';
    RAISE NOTICE '3. Document your API endpoints';
    RAISE NOTICE '4. Set up appropriate permissions';
END $$; 
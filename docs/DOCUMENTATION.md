# Yahoo Fantasy API Data Pipeline Documentation

## Overview

This project provides a comprehensive data extraction and analysis pipeline for Yahoo Fantasy Sports data. It includes OAuth authentication, data extraction, database storage, and deployment capabilities for fantasy sports analytics.

## Project Structure

```
the-league/
├── DOCUMENTATION.md              # This file - comprehensive project documentation
├── README.md                     # Basic project setup and usage guide
├── requirements.txt              # Python dependencies
├── .gitignore                    # Git ignore patterns
├── config.template.json          # Template for API credentials configuration
├── env.template                  # Template for environment variables
├── oauth2.json                   # OAuth token storage (auto-generated)
│
├── Core Scripts/
├── yahoo_fantasy_oauth.py        # OAuth authentication script
├── yahoo_fantasy_oauth_v2.py     # Enhanced OAuth authentication with token management
├── comprehensive_data_extractor.py # Main data extraction engine
├── run_final_complete_extraction.py # Production data extraction runner
├── query_database.py             # Database query interface
├── deploy_to_heroku_postgres.py  # Heroku deployment script
├── get_heroku_db_url.py          # Heroku database URL retrieval
├── database_explorer.py          # Flask web-based database explorer
├── deploy_database_explorer.py   # Database explorer Heroku deployment
├── Procfile                      # Heroku process configuration
│
├── utils/                        # Utility modules
├── database_schema.py            # Database schema definitions
├── database_loader.py            # Database loading utilities
├── yahoo_fantasy_schema.sql      # SQL schema for PostgreSQL
│
├── debug/                        # Debug and development files
├── data_extraction.log           # Extraction process logs
├── yahoo_fantasy_automation.log  # Automation logs
├── Various debug scripts         # Development and debugging tools
├── Sample JSON files             # API response examples
├── data_quality_analysis.json    # Data quality assessment results
│
├── tests/                        # Test files
├── test_complete_extraction.py   # Complete extraction tests
├── test_comprehensive_extractor.py # Comprehensive extractor tests
├── test_fixed_extraction.py      # Fixed extraction tests
├── test_v2.py                    # Version 2 tests
│
├── archive_data/                 # Historical extraction results
└── Various archived JSON files   # Previous extraction outputs
```

## Core Components

### 1. Authentication Layer

#### `yahoo_fantasy_oauth.py`
- **Purpose**: Basic OAuth 2.0 authentication with Yahoo Fantasy API
- **Features**: 
  - Interactive authentication flow
  - Token storage and management
  - Basic league access verification
- **Usage**: Entry point for manual authentication testing

#### `yahoo_fantasy_oauth_v2.py`
- **Purpose**: Enhanced OAuth authentication with advanced token management
- **Features**:
  - Automatic token refresh
  - Robust error handling
  - Session persistence
  - League discovery and validation
- **Usage**: Production-ready authentication component

### 2. Data Extraction Engine

#### `comprehensive_data_extractor.py`
- **Purpose**: Core data extraction engine that fetches all fantasy data
- **Capabilities**:
  - **League Data**: Basic league information, settings, standings
  - **Team Data**: Team details, rosters, stats
  - **Player Data**: Player information, statistics, projections
  - **Matchup Data**: Weekly matchups, scores, results
  - **Transaction Data**: Trades, adds, drops, waivers
  - **Draft Data**: Draft results, pick orders, player selections
- **Features**:
  - Rate limiting and API throttling
  - Comprehensive error handling
  - Progress tracking and logging
  - Data validation and cleansing
  - Modular extraction by data type

#### `run_final_complete_extraction.py`
- **Purpose**: Production runner for complete data extraction
- **Features**:
  - Configuration management
  - Extraction orchestration
  - Output formatting (JSON)
  - Timestamp and metadata tracking
  - Error recovery and reporting

### 3. Database Layer

#### `utils/database_schema.py`
- **Purpose**: Defines database schema and ORM models
- **Components**:
  - SQLAlchemy ORM models for all data types
  - Table relationships and constraints
  - Data type definitions
  - Index specifications

#### `utils/database_loader.py`
- **Purpose**: Handles database operations and data loading
- **Features**:
  - Data insertion and updates
  - Duplicate handling
  - Batch processing
  - Transaction management
  - Data integrity validation

#### `utils/yahoo_fantasy_schema.sql`
- **Purpose**: Raw SQL schema for PostgreSQL database
- **Features**:
  - Complete table definitions
  - Primary and foreign key constraints
  - Indexes for performance optimization
  - Data type specifications

### 4. Database Interface

#### `query_database.py`
- **Purpose**: Interactive database query interface
- **Features**:
  - Pre-built common queries
  - Interactive query execution
  - Result formatting and display
  - Connection management

### 5. Deployment Layer

#### `deploy_to_heroku_postgres.py`
- **Purpose**: Automated deployment to Heroku with PostgreSQL
- **Features**:
  - Schema creation and migration
  - Data loading and validation
  - Environment configuration
  - Deployment verification

#### `get_heroku_db_url.py`
- **Purpose**: Heroku database connection management
- **Features**:
  - Database URL retrieval
  - Connection testing
  - Environment variable management

#### `database_explorer.py`
- **Purpose**: Web-based database exploration and query interface
- **Features**:
  - Interactive SQL query execution
  - Pre-built analytics queries
  - Table browsing and data viewing
  - Database schema exploration
  - Safety checks for destructive operations
  - Health monitoring endpoints

#### `deploy_database_explorer.py`
- **Purpose**: Deploy database explorer web app to Heroku
- **Features**:
  - Automated Heroku app creation
  - PostgreSQL addon setup
  - Environment configuration
  - Git deployment automation

## Data Pipeline Flow

### 1. Authentication Flow
```
User → yahoo_fantasy_oauth_v2.py → Yahoo API → OAuth Tokens → oauth2.json
```

### 2. Data Extraction Flow
```
run_final_complete_extraction.py → comprehensive_data_extractor.py → Yahoo API
                                                                    ↓
JSON Output ← Data Processing ← Rate Limited API Calls ← Authentication
```

### 3. Database Loading Flow
```
JSON Data → database_loader.py → PostgreSQL Database
                ↓
        database_schema.py (ORM Models)
```

### 4. Deployment Flow
```
Local Data → deploy_to_heroku_postgres.py → Heroku PostgreSQL → Production Database
```

## Data Types Extracted

### League Information
- Basic league details (name, type, season)
- League settings and scoring rules
- Current standings and statistics

### Team Data
- Team information and ownership
- Current roster compositions
- Team statistics and performance metrics

### Player Data
- Player profiles and basic information
- Season statistics and performance data
- Ownership percentages and availability

### Matchup Data
- Weekly matchup schedules
- Scoring results and outcomes
- Historical matchup performance

### Transaction Data
- Trade history and details
- Waiver wire activity (adds/drops)
- Free agent acquisitions

### Draft Data
- Draft order and results
- Pick-by-pick draft history
- Player draft positions and timing

## Configuration Files

### `config.template.json`
Template for Yahoo API credentials:
```json
{
    "yahoo_app_id": "your_app_id",
    "yahoo_client_key": "your_client_key", 
    "yahoo_client_secret": "your_client_secret"
}
```

### `env.template`
Template for environment variables including database connections

## Usage Workflows

### Initial Setup
1. Copy `config.template.json` to `config.json` and add credentials
2. Install dependencies: `pip install -r requirements.txt`
3. Run authentication: `python yahoo_fantasy_oauth_v2.py`

### Data Extraction
1. Run complete extraction: `python run_final_complete_extraction.py`
2. Output saved as timestamped JSON file
3. Review logs for any issues or errors

### Database Setup and Loading
1. Set up PostgreSQL database (local or Heroku)
2. Run schema creation: Use `yahoo_fantasy_schema.sql`
3. Load data: `python -c "from utils.database_loader import load_data; load_data('data_file.json')"`

### Production Deployment
1. Configure Heroku environment
2. Run: `python deploy_to_heroku_postgres.py`
3. Verify deployment with `python get_heroku_db_url.py`

### Database Explorer Deployment
1. Deploy web interface: `python deploy_database_explorer.py`
2. Access via web browser at the provided URL
3. Use predefined queries or create custom ones

### Data Analysis
1. **Web Interface**: Use the deployed database explorer for interactive queries
2. **Command Line**: Connect locally with `python query_database.py`
3. **Direct Database**: Connect with any PostgreSQL client using Heroku credentials

## Development and Debug Tools

### Debug Directory
- Contains various debugging scripts for troubleshooting
- Sample API response files for testing
- Data quality analysis tools
- Extraction logs and automation logs

### Testing
- Comprehensive test suite in `tests/` directory
- Unit tests for individual components
- Integration tests for full workflows
- Performance and reliability tests

## Logging and Monitoring

### Log Files
- `debug/data_extraction.log`: Detailed extraction process logs
- `debug/yahoo_fantasy_automation.log`: Automation and scheduling logs

### Data Quality
- `debug/data_quality_analysis.json`: Comprehensive data quality metrics
- Built-in validation and consistency checking

## API Rate Limiting

The system implements sophisticated rate limiting to respect Yahoo's API constraints:
- Configurable request delays
- Automatic backoff on rate limit errors
- Request queuing and batching
- Error recovery and retry logic

## Security Considerations

- OAuth tokens stored securely in `oauth2.json`
- API credentials managed through config files (not committed to git)
- Database connections use environment variables
- Sensitive files included in `.gitignore`

## Performance Optimization

- Efficient data extraction with minimal API calls
- Database indexing for common query patterns
- Batch processing for large data sets
- Connection pooling and management

## Future Enhancements

- Real-time data streaming capabilities
- Advanced analytics and machine learning features
- Web dashboard for data visualization
- Automated scheduling and monitoring
- Multi-league and multi-sport support

## Support and Troubleshooting

### Common Issues
1. **Authentication Errors**: Check credentials in `config.json`
2. **Rate Limiting**: Increase delays in extraction configuration
3. **Database Connection**: Verify environment variables and permissions
4. **Missing Data**: Review extraction logs for API errors

### Debug Resources
- Enable verbose logging in extraction scripts
- Use debug scripts to test individual components
- Review sample JSON files for expected data formats
- Check data quality analysis for validation issues

## Contributing

When contributing to this project:
1. Follow existing code structure and patterns
2. Add appropriate logging and error handling
3. Update documentation for new features
4. Include tests for new functionality
5. Maintain backward compatibility where possible 
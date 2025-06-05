#!/usr/bin/env python3
"""
Database Loader for Yahoo Fantasy Data
Loads extracted JSON data into a relational database
"""

import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YahooFantasyDatabaseLoader:
    """Database loader for Yahoo Fantasy data"""
    
    def __init__(self, data_file: str):
        """
        Initialize database loader
        
        Args:
            data_file (str): Path to extracted JSON data file
        """
        self.data_file = data_file
        self.data = None
        self.connection = None
        
    def load_data(self) -> bool:
        """Load extracted data from JSON file"""
        try:
            logger.info(f"📂 Loading data from {self.data_file}...")
            
            with open(self.data_file, 'r') as f:
                self.data = json.load(f)
            
            logger.info("✅ Data loaded successfully!")
            logger.info(f"📊 Data Summary:")
            for table, records in self.data.items():
                logger.info(f"  - {table}: {len(records)} records")
            
            return True
            
        except FileNotFoundError:
            logger.error(f"❌ Data file not found: {self.data_file}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in data file: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return False
    
    def validate_data(self) -> bool:
        """Validate data structure and integrity"""
        if not self.data:
            logger.error("❌ No data loaded")
            return False
        
        logger.info("🔍 Validating data structure...")
        
        expected_tables = ['leagues', 'teams', 'rosters', 'matchups', 'transactions']
        
        for table in expected_tables:
            if table not in self.data:
                logger.warning(f"⚠️  Missing table: {table}")
            else:
                logger.info(f"✅ Found {table}: {len(self.data[table])} records")
        
        # Validate league data
        if 'leagues' in self.data and self.data['leagues']:
            sample_league = self.data['leagues'][0]
            required_fields = ['league_id', 'name', 'season', 'num_teams']
            missing_fields = [field for field in required_fields if field not in sample_league]
            
            if missing_fields:
                logger.error(f"❌ Missing required league fields: {missing_fields}")
                return False
            
            logger.info("✅ League data structure validated")
        
        # Validate team data
        if 'teams' in self.data and self.data['teams']:
            sample_team = self.data['teams'][0]
            required_fields = ['team_id', 'league_id', 'name']
            missing_fields = [field for field in required_fields if field not in sample_team]
            
            if missing_fields:
                logger.error(f"❌ Missing required team fields: {missing_fields}")
                return False
            
            logger.info("✅ Team data structure validated")
        
        logger.info("✅ Data validation completed successfully!")
        return True
    
    def export_to_csv(self, output_dir: str = "csv_export") -> bool:
        """Export data to CSV files for database import"""
        try:
            import os
            
            logger.info(f"📁 Exporting data to CSV files in {output_dir}/...")
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Export each table to CSV
            for table_name, records in self.data.items():
                if not records:
                    logger.info(f"  📄 Skipping empty table: {table_name}")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(records)
                
                # Clean up data types
                if table_name == 'leagues':
                    # Convert boolean fields
                    bool_fields = ['is_pro_league', 'is_cash_league']
                    for field in bool_fields:
                        if field in df.columns:
                            df[field] = df[field].astype(bool)
                    
                    # Convert numeric fields
                    numeric_fields = ['num_teams', 'current_week', 'start_week', 'end_week']
                    for field in numeric_fields:
                        if field in df.columns:
                            df[field] = pd.to_numeric(df[field], errors='coerce')
                
                elif table_name == 'teams':
                    # Convert numeric fields
                    numeric_fields = ['wins', 'losses', 'ties', 'points_for', 'points_against']
                    for field in numeric_fields:
                        if field in df.columns:
                            df[field] = pd.to_numeric(df[field], errors='coerce')
                
                # Export to CSV
                csv_file = os.path.join(output_dir, f"{table_name}.csv")
                df.to_csv(csv_file, index=False)
                
                logger.info(f"  📄 Exported {table_name}: {len(df)} records → {csv_file}")
            
            logger.info("✅ CSV export completed successfully!")
            return True
            
        except ImportError:
            logger.error("❌ pandas is required for CSV export. Install with: pip install pandas")
            return False
        except Exception as e:
            logger.error(f"❌ Error exporting to CSV: {e}")
            return False
    
    def generate_sql_inserts(self, output_file: str = "insert_statements.sql") -> bool:
        """Generate SQL INSERT statements for the data"""
        try:
            logger.info(f"📝 Generating SQL INSERT statements to {output_file}...")
            
            with open(output_file, 'w') as f:
                f.write("-- Yahoo Fantasy Football Data\n")
                f.write(f"-- Generated on {datetime.now().isoformat()}\n")
                f.write("-- Total records: " + ", ".join([f"{table}: {len(records)}" for table, records in self.data.items()]) + "\n\n")
                
                # Generate INSERT statements for each table
                for table_name, records in self.data.items():
                    if not records:
                        f.write(f"-- No data for table: {table_name}\n\n")
                        continue
                    
                    f.write(f"-- INSERT statements for {table_name} ({len(records)} records)\n")
                    
                    if records:
                        # Get column names from first record
                        columns = list(records[0].keys())
                        
                        for record in records:
                            # Prepare values
                            values = []
                            for col in columns:
                                value = record.get(col)
                                if value is None:
                                    values.append("NULL")
                                elif isinstance(value, bool):
                                    values.append("TRUE" if value else "FALSE")
                                elif isinstance(value, (int, float)):
                                    values.append(str(value))
                                else:
                                    # Escape single quotes and wrap in quotes
                                    escaped_value = str(value).replace("'", "''")
                                    values.append(f"'{escaped_value}'")
                            
                            # Generate INSERT statement
                            column_list = ", ".join(columns)
                            value_list = ", ".join(values)
                            f.write(f"INSERT INTO {table_name} ({column_list}) VALUES ({value_list});\n")
                    
                    f.write("\n")
            
            logger.info("✅ SQL INSERT statements generated successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error generating SQL inserts: {e}")
            return False
    
    def create_data_summary(self, output_file: str = "data_summary.md") -> bool:
        """Create a markdown summary of the extracted data"""
        try:
            logger.info(f"📊 Creating data summary report: {output_file}...")
            
            with open(output_file, 'w') as f:
                f.write("# Yahoo Fantasy Football Data Summary\n\n")
                f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"**Source:** {self.data_file}\n\n")
                
                # Overall statistics
                f.write("## Overall Statistics\n\n")
                f.write("| Table | Records |\n")
                f.write("|-------|--------|\n")
                for table, records in self.data.items():
                    f.write(f"| {table.title()} | {len(records):,} |\n")
                
                # League analysis
                if 'leagues' in self.data and self.data['leagues']:
                    f.write("\n## League Analysis\n\n")
                    
                    # Group by season
                    leagues_by_season = {}
                    for league in self.data['leagues']:
                        season = league.get('season', 'Unknown')
                        if season not in leagues_by_season:
                            leagues_by_season[season] = []
                        leagues_by_season[season].append(league)
                    
                    f.write("### Leagues by Season\n\n")
                    f.write("| Season | Leagues | Total Teams |\n")
                    f.write("|--------|---------|------------|\n")
                    
                    for season in sorted(leagues_by_season.keys()):
                        league_count = len(leagues_by_season[season])
                        total_teams = sum(int(league.get('num_teams', 0)) for league in leagues_by_season[season])
                        f.write(f"| {season} | {league_count} | {total_teams} |\n")
                    
                    f.write("\n### League Details\n\n")
                    f.write("| Season | League Name | Teams | Type |\n")
                    f.write("|--------|-------------|-------|------|\n")
                    
                    for season in sorted(leagues_by_season.keys()):
                        for league in leagues_by_season[season]:
                            name = league.get('name', 'Unknown')
                            teams = league.get('num_teams', 0)
                            league_type = league.get('league_type', 'Unknown')
                            f.write(f"| {season} | {name} | {teams} | {league_type} |\n")
                
                # Team analysis
                if 'teams' in self.data and self.data['teams']:
                    f.write("\n## Team Analysis\n\n")
                    
                    total_teams = len(self.data['teams'])
                    f.write(f"**Total Teams:** {total_teams:,}\n\n")
                    
                    # Calculate statistics
                    points_for = [float(team.get('points_for', 0)) for team in self.data['teams'] if team.get('points_for')]
                    if points_for:
                        avg_points = sum(points_for) / len(points_for)
                        max_points = max(points_for)
                        min_points = min(points_for)
                        
                        f.write("### Scoring Statistics\n\n")
                        f.write(f"- **Average Points For:** {avg_points:.1f}\n")
                        f.write(f"- **Highest Points:** {max_points:.1f}\n")
                        f.write(f"- **Lowest Points:** {min_points:.1f}\n\n")
                
                f.write("\n## Database Schema\n\n")
                f.write("The data is structured for relational database storage with the following tables:\n\n")
                f.write("- **leagues**: League information and settings\n")
                f.write("- **teams**: Team information and standings\n")
                f.write("- **rosters**: Player assignments to teams (future)\n")
                f.write("- **matchups**: Weekly matchup results (future)\n")
                f.write("- **transactions**: Player transactions (future)\n")
                f.write("- **statistics**: Player statistics (future)\n\n")
                
                f.write("See `yahoo_fantasy_schema.sql` for complete database schema.\n")
            
            logger.info("✅ Data summary report created successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating data summary: {e}")
            return False

def main():
    """Main execution function"""
    if len(sys.argv) < 2:
        logger.error("❌ Usage: python database_loader.py <data_file.json>")
        logger.info("Example: python database_loader.py yahoo_fantasy_complete_data_20250605_091839.json")
        return 1
    
    data_file = sys.argv[1]
    
    logger.info("=" * 80)
    logger.info("Yahoo Fantasy Football - Database Loader")
    logger.info("=" * 80)
    
    # Initialize loader
    loader = YahooFantasyDatabaseLoader(data_file)
    
    # Load and validate data
    if not loader.load_data():
        return 1
    
    if not loader.validate_data():
        return 1
    
    # Generate outputs
    logger.info("📦 Generating database-ready outputs...")
    
    # CSV export
    if loader.export_to_csv():
        logger.info("✅ CSV files generated")
    
    # SQL INSERT statements
    if loader.generate_sql_inserts():
        logger.info("✅ SQL INSERT statements generated")
    
    # Data summary report
    if loader.create_data_summary():
        logger.info("✅ Data summary report generated")
    
    logger.info("\n🎯 Next Steps:")
    logger.info("1. Review the CSV files in csv_export/ directory")
    logger.info("2. Use yahoo_fantasy_schema.sql to create database tables")
    logger.info("3. Import CSV files or run insert_statements.sql")
    logger.info("4. Review data_summary.md for insights")
    logger.info("5. Provide database endpoint for automated loading")
    
    logger.info("\n✅ Database preparation completed successfully!")
    return 0

if __name__ == "__main__":
    exit(main())
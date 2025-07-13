#!/usr/bin/env python3
"""
Fantasy Football Data Dictionary Deployment Script

This script deploys the meta-data schema and populates it with metric definitions.
It can be run standalone or integrated into existing deployment processes.

Usage:
    python deploy_meta_data.py [--recreate] [--data-only] [--schema-only]

Author: AI Assistant
Date: 2025-01-27
"""

import os
import sys
import logging
import argparse
from pathlib import Path
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MetaDataDeployer:
    """Handles deployment of meta-data schema and definitions"""
    
    def __init__(self, db_url=None):
        """Initialize deployer with database connection"""
        self.db_url = db_url or os.getenv('DATABASE_URL')
        self.script_dir = Path(__file__).parent
        
    def get_connection(self):
        """Get database connection based on database type"""
        if not self.db_url:
            # Fall back to local SQLite for development
            db_path = self.script_dir / '..' / '..' / 'data' / 'edw.db'
            logger.info(f"Using SQLite database: {db_path}")
            return sqlite3.connect(str(db_path))
        elif 'postgresql' in self.db_url or 'postgres' in self.db_url:
            logger.info("Using PostgreSQL database")
            return psycopg2.connect(self.db_url)
        else:
            # Assume SQLite for other cases
            logger.info(f"Using SQLite database: {self.db_url}")
            return sqlite3.connect(self.db_url)
    
    def execute_sql_file(self, conn, file_path, description):
        """Execute a SQL file with proper error handling"""
        logger.info(f"Executing {description}...")
        
        try:
            with open(file_path, 'r') as file:
                sql_content = file.read()
            
            # Split on double newlines to handle multiple statements
            statements = [stmt.strip() for stmt in sql_content.split(';\n') if stmt.strip()]
            
            cursor = conn.cursor()
            
            for i, statement in enumerate(statements):
                if statement and not statement.startswith('--'):
                    try:
                        cursor.execute(statement)
                        logger.debug(f"Executed statement {i+1}/{len(statements)}")
                    except Exception as e:
                        if "already exists" in str(e).lower():
                            logger.warning(f"Object already exists (skipping): {str(e)[:100]}")
                            continue
                        else:
                            logger.error(f"Error in statement {i+1}: {e}")
                            logger.error(f"Statement: {statement[:200]}...")
                            raise
            
            conn.commit()
            logger.info(f"✓ {description} completed successfully")
            
        except Exception as e:
            logger.error(f"✗ Failed to execute {description}: {e}")
            conn.rollback()
            raise
    
    def check_schema_exists(self, conn):
        """Check if meta_data schema already exists"""
        cursor = conn.cursor()
        
        if 'postgresql' in str(type(conn)) or hasattr(conn, 'server_version'):
            # PostgreSQL
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = 'meta_data'
                )
            """)
        else:
            # SQLite - check for any meta_data tables
            cursor.execute("""
                SELECT COUNT(*) FROM sqlite_master 
                WHERE type = 'table' AND name LIKE 'meta_data_%'
            """)
        
        result = cursor.fetchone()
        return bool(result[0]) if result else False
    
    def deploy_schema(self, conn, recreate=False):
        """Deploy the meta_data schema"""
        schema_file = self.script_dir / 'meta_data_schema.sql'
        
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
        
        schema_exists = self.check_schema_exists(conn)
        
        if schema_exists and not recreate:
            logger.info("Meta-data schema already exists. Use --recreate to rebuild.")
            return
        
        if recreate and schema_exists:
            logger.warning("Dropping existing meta_data schema...")
            cursor = conn.cursor()
            try:
                cursor.execute("DROP SCHEMA IF EXISTS meta_data CASCADE")
                conn.commit()
                logger.info("✓ Existing schema dropped")
            except Exception as e:
                logger.warning(f"Could not drop schema (may not exist): {e}")
        
        self.execute_sql_file(conn, schema_file, "Meta-data schema creation")
    
    def deploy_data(self, conn):
        """Deploy the metric definitions data"""
        data_file = self.script_dir / 'populate_metric_definitions.sql'
        
        if not data_file.exists():
            raise FileNotFoundError(f"Data file not found: {data_file}")
        
        self.execute_sql_file(conn, data_file, "Metric definitions population")
    
    def verify_deployment(self, conn):
        """Verify the deployment was successful"""
        cursor = conn.cursor()
        
        try:
            # Check if we can query the main tables
            cursor.execute("SELECT COUNT(*) FROM meta_data.metric_definitions")
            metric_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM meta_data.metric_categories")
            category_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM meta_data.api_definitions")
            api_count = cursor.fetchone()[0]
            
            logger.info("✓ Deployment verification successful:")
            logger.info(f"  - {metric_count} metric definitions")
            logger.info(f"  - {category_count} metric categories")
            logger.info(f"  - {api_count} API definitions")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Deployment verification failed: {e}")
            return False
    
    def deploy(self, recreate=False, schema_only=False, data_only=False):
        """Execute the full deployment process"""
        logger.info("Starting meta-data schema deployment...")
        
        try:
            with self.get_connection() as conn:
                if not data_only:
                    self.deploy_schema(conn, recreate=recreate)
                
                if not schema_only:
                    self.deploy_data(conn)
                
                if not schema_only and not data_only:
                    self.verify_deployment(conn)
                
            logger.info("🎉 Meta-data deployment completed successfully!")
            
        except Exception as e:
            logger.error(f"💥 Deployment failed: {e}")
            sys.exit(1)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Deploy fantasy football meta-data schema')
    parser.add_argument('--recreate', action='store_true', 
                      help='Drop and recreate existing schema')
    parser.add_argument('--schema-only', action='store_true',
                      help='Deploy schema only (no data)')
    parser.add_argument('--data-only', action='store_true',
                      help='Deploy data only (assume schema exists)')
    parser.add_argument('--db-url', type=str,
                      help='Database URL (defaults to DATABASE_URL env var)')
    parser.add_argument('--verbose', '-v', action='store_true',
                      help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.schema_only and args.data_only:
        parser.error("Cannot specify both --schema-only and --data-only")
    
    deployer = MetaDataDeployer(db_url=args.db_url)
    deployer.deploy(
        recreate=args.recreate,
        schema_only=args.schema_only,
        data_only=args.data_only
    )

if __name__ == '__main__':
    main() 
#!/usr/bin/env python3
"""
Query Heroku Postgres Database
Run SQL queries against the deployed Yahoo Fantasy dataset
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

def run_query(query, database_url=None):
    """Run a SQL query and return results"""
    
    # Get database URL
    db_url = database_url or os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL not found. Set it as environment variable or pass directly.")
        return None
    
    # Fix URL for newer SQLAlchemy (postgres:// -> postgresql://)
    if db_url.startswith('postgres://'):
        db_url = db_url.replace('postgres://', 'postgresql://', 1)
    
    try:
        # Create engine and run query
        engine = create_engine(db_url)
        
        print(f"🔍 Running query: {query}")
        print("=" * 60)
        
        # Execute query and get results as DataFrame
        df = pd.read_sql_query(query, engine)
        
        print(f"📊 Results: {len(df)} rows")
        print("=" * 60)
        
        # Display results
        if len(df) > 0:
            # Configure pandas to show all columns
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 50)
            
            print(df.to_string(index=False))
        else:
            print("No results found.")
        
        return df
        
    except Exception as e:
        print(f"❌ Error running query: {e}")
        return None

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Query Yahoo Fantasy database')
    parser.add_argument('--query', '-q', 
                       default='SELECT * FROM leagues ORDER BY season, name',
                       help='SQL query to run')
    parser.add_argument('--database-url', 
                       help='Database URL (or use DATABASE_URL env var)')
    
    args = parser.parse_args()
    
    print("🗄️  Yahoo Fantasy Database Query Tool")
    print("=" * 40)
    
    # Run the query
    result = run_query(args.query, args.database_url)
    
    if result is not None:
        print(f"\n✅ Query completed successfully!")
        print(f"📈 Returned {len(result)} rows")
    else:
        print(f"\n❌ Query failed")

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
import psycopg2
import os

def deploy_schema():
    """Deploy the schema file using direct psycopg2 execution"""
    
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()
    
    # Read schema file
    with open('fantasy_edw_schema.sql', 'r') as f:
        schema_content = f.read()
    
    print(f"📋 Executing schema file ({len(schema_content)} characters)")
    
    try:
        # Check what tables exist before
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'edw' ORDER BY table_name;")
        tables_before = set([row[0] for row in cur.fetchall()])
        print(f"📊 Tables before: {len(tables_before)} - {sorted(tables_before)}")
        
        # Execute the entire schema - but use individual transactions for better error handling
        statements = [s.strip() for s in schema_content.split(';') if s.strip()]
        created_count = 0
        
        for stmt in statements:
            if 'CREATE TABLE' in stmt.upper():
                try:
                    # Replace CREATE TABLE with CREATE TABLE IF NOT EXISTS
                    stmt_safe = stmt.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS', 1)
                    cur.execute(stmt_safe)
                    conn.commit()
                    created_count += 1
                except Exception as e:
                    print(f"⚠️ Table creation warning: {str(e)[:100]}...")
                    conn.rollback()
        
        print(f"✅ Schema deployed: {created_count} tables processed")
        
        # Check what tables exist after
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'edw' ORDER BY table_name;")
        tables_after = [row[0] for row in cur.fetchall()]
        print(f"📊 Tables after: {len(tables_after)} - {sorted(tables_after)}")
            
    except Exception as e:
        print(f"❌ Schema deployment failed: {e}")
        conn.rollback()
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    deploy_schema() 
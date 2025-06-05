#!/usr/bin/env python3
"""
Yahoo Fantasy Database Explorer
A Flask web application for exploring and querying the Yahoo Fantasy Sports database.
"""

import os
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-in-production')

# Database connection
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is required")

# Fix for Heroku PostgreSQL URL format
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

class DatabaseExplorer:
    """Database exploration and query utilities."""
    
    def __init__(self):
        self.engine = engine
        
    def execute_query(self, query, params=None):
        """Execute a SQL query and return results."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                if result.returns_rows:
                    columns = result.keys()
                    rows = result.fetchall()
                    return {
                        'success': True,
                        'columns': list(columns),
                        'rows': [list(row) for row in rows],
                        'row_count': len(rows)
                    }
                else:
                    return {
                        'success': True,
                        'message': f"Query executed successfully. {result.rowcount} rows affected.",
                        'row_count': result.rowcount
                    }
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_table_info(self):
        """Get information about all tables in the database."""
        query = """
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
        """
        result = self.execute_query(query)
        if result['success']:
            tables = {}
            for row in result['rows']:
                table_name, column_name, data_type, is_nullable, column_default = row
                if table_name not in tables:
                    tables[table_name] = []
                tables[table_name].append({
                    'column': column_name,
                    'type': data_type,
                    'nullable': is_nullable,
                    'default': column_default
                })
            return tables
        return {}
    
    def get_table_counts(self):
        """Get row counts for all tables."""
        tables = self.get_table_info()
        counts = {}
        for table_name in tables.keys():
            try:
                result = self.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                if result['success'] and result['rows']:
                    counts[table_name] = result['rows'][0][0]
            except Exception as e:
                counts[table_name] = f"Error: {e}"
        return counts

# Initialize database explorer
db_explorer = DatabaseExplorer()

@app.route('/')
def dashboard():
    """Main dashboard with database overview."""
    try:
        table_counts = db_explorer.get_table_counts()
        table_info = db_explorer.get_table_info()
        
        # Get some basic stats
        total_records = sum(count for count in table_counts.values() if isinstance(count, int))
        
        # Simple HTML response for now
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Yahoo Fantasy Database Explorer</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .stats {{ background-color: #e7f3ff; padding: 15px; margin: 10px 0; }}
            </style>
        </head>
        <body>
            <h1>Yahoo Fantasy Database Explorer</h1>
            <div class="stats">
                <h2>Database Overview</h2>
                <p><strong>Total Records:</strong> {total_records:,}</p>
                <p><strong>Tables:</strong> {len(table_counts)}</p>
            </div>
            
            <h2>Table Counts</h2>
            <table>
                <tr><th>Table</th><th>Records</th></tr>
        """
        
        for table, count in table_counts.items():
            html += f"<tr><td>{table}</td><td>{count:,}</td></tr>"
        
        html += """
            </table>
            
            <h2>Quick Queries</h2>
            <ul>
                <li><a href="/query/leagues">League Overview</a></li>
                <li><a href="/query/teams">Team Standings</a></li>
                <li><a href="/query/players">Top Players</a></li>
                <li><a href="/query/transactions">Recent Transactions</a></li>
            </ul>
            
            <h2>Custom Query</h2>
            <form action="/execute" method="post">
                <textarea name="query" rows="5" cols="80" placeholder="Enter SQL query here..."></textarea><br>
                <input type="submit" value="Execute Query">
            </form>
        </body>
        </html>
        """
        
        return html
        
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return f"<h1>Error</h1><p>{e}</p>"

@app.route('/query/<query_type>')
def predefined_query(query_type):
    """Execute predefined queries."""
    queries = {
        'leagues': "SELECT league_id, name, season, num_teams, scoring_type FROM leagues ORDER BY season DESC, name",
        'teams': """
            SELECT t.team_name, t.owner_name, t.wins, t.losses, t.points_for, t.points_against, 
                   l.name as league_name
            FROM teams t
            JOIN leagues l ON t.league_id = l.league_id
            ORDER BY t.wins DESC, t.points_for DESC
            LIMIT 50
        """,
        'players': """
            SELECT player_name, position, SUM(total_points) as total_points
            FROM players 
            WHERE total_points > 0 
            GROUP BY player_id, player_name, position
            ORDER BY total_points DESC 
            LIMIT 25
        """,
        'transactions': """
            SELECT t.type, t.player_name, t.team_name, t.timestamp, l.name as league_name
            FROM transactions t
            JOIN leagues l ON t.league_id = l.league_id
            ORDER BY t.timestamp DESC 
            LIMIT 50
        """
    }
    
    if query_type not in queries:
        return "<h1>Query not found</h1>"
    
    result = db_explorer.execute_query(queries[query_type])
    
    if not result['success']:
        return f"<h1>Error</h1><p>{result['error']}</p>"
    
    # Generate HTML table
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Query Results - {query_type.title()}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h1>{query_type.title()} Results</h1>
        <p><a href="/">← Back to Dashboard</a></p>
        <p>Found {result['row_count']} records</p>
        
        <table>
            <tr>
    """
    
    # Add headers
    for col in result['columns']:
        html += f"<th>{col}</th>"
    html += "</tr>"
    
    # Add data rows
    for row in result['rows']:
        html += "<tr>"
        for cell in row:
            html += f"<td>{cell}</td>"
        html += "</tr>"
    
    html += """
        </table>
    </body>
    </html>
    """
    
    return html

@app.route('/execute', methods=['POST'])
def execute_custom_query():
    """Execute a custom SQL query."""
    query = request.form.get('query', '').strip()
    
    if not query:
        return "<h1>Error</h1><p>Query cannot be empty</p>"
    
    # Basic safety check
    query_lower = query.lower().strip()
    if any(dangerous in query_lower for dangerous in ['drop', 'delete', 'truncate', 'alter', 'create']):
        return "<h1>Error</h1><p>Destructive operations are not allowed</p>"
    
    result = db_explorer.execute_query(query)
    
    if not result['success']:
        return f"<h1>Error</h1><p>{result['error']}</p>"
    
    # Generate HTML response
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Custom Query Results</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .query {{ background-color: #f9f9f9; padding: 10px; margin: 10px 0; }}
        </style>
    </head>
    <body>
        <h1>Custom Query Results</h1>
        <p><a href="/">← Back to Dashboard</a></p>
        
        <div class="query">
            <strong>Query:</strong><br>
            <code>{query}</code>
        </div>
        
        <p>Found {result['row_count']} records</p>
        
        <table>
            <tr>
    """
    
    # Add headers
    for col in result['columns']:
        html += f"<th>{col}</th>"
    html += "</tr>"
    
    # Add data rows
    for row in result['rows']:
        html += "<tr>"
        for cell in row:
            html += f"<td>{cell}</td>"
        html += "</tr>"
    
    html += """
        </table>
    </body>
    </html>
    """
    
    return html

@app.route('/health')
def health_check():
    """Health check endpoint for Heroku."""
    try:
        result = db_explorer.execute_query("SELECT 1")
        if result['success']:
            return jsonify({
                'status': 'healthy',
                'database': 'connected',
                'timestamp': datetime.utcnow().isoformat()
            })
        else:
            return jsonify({
                'status': 'unhealthy',
                'database': 'disconnected',
                'error': result.get('error', 'Unknown error')
            }), 500
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug) 
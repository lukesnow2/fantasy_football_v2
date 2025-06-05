#!/usr/bin/env python3
"""
Deploy Database Explorer to Heroku
Deploys the Flask-based database explorer web application to Heroku.
"""

import os
import sys
import subprocess
import json
from datetime import datetime

def run_command(command, check=True):
    """Run a command and return the result."""
    print(f"Running: {command}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=check)
        if result.stdout:
            print(result.stdout)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        if check:
            sys.exit(1)
        return e

def check_heroku_cli():
    """Check if Heroku CLI is installed."""
    result = run_command("heroku --version", check=False)
    if result.returncode != 0:
        print("❌ Heroku CLI is not installed or not in PATH")
        print("Please install it from: https://devcenter.heroku.com/articles/heroku-cli")
        sys.exit(1)
    print("✅ Heroku CLI is available")

def create_or_get_app(app_name=None):
    """Create a new Heroku app or use existing one."""
    if not app_name:
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        app_name = f"yahoo-fantasy-explorer-{timestamp}"
    
    print(f"Creating Heroku app: {app_name}")
    result = run_command(f"heroku create {app_name}", check=False)
    
    if result.returncode != 0:
        if "Name is already taken" in result.stderr:
            print(f"App {app_name} already exists, using it...")
            return app_name
        else:
            print(f"Failed to create app: {result.stderr}")
            sys.exit(1)
    
    return app_name

def setup_database(app_name):
    """Add PostgreSQL addon to Heroku app."""
    print("Adding PostgreSQL addon...")
    
    # Check if postgres addon already exists
    result = run_command(f"heroku addons --app {app_name}", check=False)
    if "heroku-postgresql" in result.stdout:
        print("✅ PostgreSQL addon already exists")
        return
    
    # Add postgres addon
    result = run_command(f"heroku addons:create heroku-postgresql:mini --app {app_name}")
    if result.returncode == 0:
        print("✅ PostgreSQL addon added successfully")
    else:
        print("❌ Failed to add PostgreSQL addon")
        sys.exit(1)

def set_environment_variables(app_name):
    """Set required environment variables."""
    print("Setting environment variables...")
    
    # Set Flask environment
    run_command(f"heroku config:set FLASK_ENV=production --app {app_name}")
    run_command(f"heroku config:set FLASK_DEBUG=false --app {app_name}")
    
    # Generate a secret key
    import secrets
    secret_key = secrets.token_urlsafe(32)
    run_command(f"heroku config:set SECRET_KEY={secret_key} --app {app_name}")
    
    print("✅ Environment variables set")

def deploy_app(app_name):
    """Deploy the application to Heroku."""
    print("Deploying application...")
    
    # Initialize git if not already done
    if not os.path.exists('.git'):
        run_command("git init")
        run_command("git add .")
        run_command('git commit -m "Initial commit for database explorer deployment"')
    
    # Add Heroku remote
    run_command(f"heroku git:remote --app {app_name}")
    
    # Deploy
    result = run_command("git push heroku main", check=False)
    if result.returncode != 0:
        # Try pushing master branch instead
        print("Trying to push master branch...")
        result = run_command("git push heroku master", check=False)
        if result.returncode != 0:
            print("❌ Deployment failed")
            print("Make sure you have committed your changes and try again")
            sys.exit(1)
    
    print("✅ Application deployed successfully")

def get_app_url(app_name):
    """Get the deployed app URL."""
    result = run_command(f"heroku info --app {app_name}")
    lines = result.stdout.split('\n')
    for line in lines:
        if 'Web URL:' in line:
            return line.split('Web URL:')[1].strip()
    return f"https://{app_name}.herokuapp.com"

def main():
    """Main deployment function."""
    print("🚀 Yahoo Fantasy Database Explorer - Heroku Deployment")
    print("=" * 60)
    
    # Check prerequisites
    check_heroku_cli()
    
    # Get app name from user or use default
    app_name = input("Enter Heroku app name (or press Enter for auto-generated): ").strip()
    if not app_name:
        app_name = None
    
    try:
        # Create or get app
        app_name = create_or_get_app(app_name)
        
        # Setup database
        setup_database(app_name)
        
        # Set environment variables
        set_environment_variables(app_name)
        
        # Deploy application
        deploy_app(app_name)
        
        # Get app URL
        app_url = get_app_url(app_name)
        
        print("\n" + "=" * 60)
        print("🎉 DEPLOYMENT SUCCESSFUL!")
        print("=" * 60)
        print(f"App Name: {app_name}")
        print(f"Database Explorer URL: {app_url}")
        print(f"Health Check: {app_url}/health")
        print("\n📋 Next Steps:")
        print("1. Load your data using deploy_to_heroku_postgres.py")
        print("2. Visit the database explorer to query your data")
        print("3. Use the predefined queries or create custom ones")
        print("\n🔧 Management Commands:")
        print(f"- View logs: heroku logs --tail --app {app_name}")
        print(f"- Open app: heroku open --app {app_name}")
        print(f"- Database info: heroku pg:info --app {app_name}")
        
    except KeyboardInterrupt:
        print("\n\nDeployment cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Deployment failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
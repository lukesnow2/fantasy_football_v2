#!/usr/bin/env python3
"""
Helper script to get Heroku Postgres DATABASE_URL
"""

import subprocess
import sys
import os

def get_heroku_db_url(app_name=None):
    """Get DATABASE_URL from Heroku app"""
    
    print("🔍 Getting Heroku Postgres DATABASE_URL...")
    
    try:
        # Try to get from Heroku CLI
        if app_name:
            cmd = f"heroku config:get DATABASE_URL -a {app_name}"
        else:
            cmd = "heroku config:get DATABASE_URL"
        
        result = subprocess.run(cmd.split(), capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout.strip():
            db_url = result.stdout.strip()
            print(f"✅ Found DATABASE_URL: {db_url[:30]}...")
            return db_url
        else:
            print("❌ Heroku CLI command failed or returned empty result")
            print("Make sure you have Heroku CLI installed and are logged in")
            return None
            
    except FileNotFoundError:
        print("❌ Heroku CLI not found")
        print("Install it from: https://devcenter.heroku.com/articles/heroku-cli")
        return None
    except Exception as e:
        print(f"❌ Error getting DATABASE_URL: {e}")
        return None

def main():
    print("🚀 Heroku Postgres DATABASE_URL Helper")
    print("=" * 40)
    
    # Check if app name provided as argument
    app_name = sys.argv[1] if len(sys.argv) > 1 else None
    
    if app_name:
        print(f"📱 App: {app_name}")
    else:
        print("📱 Using default Heroku app (or will prompt)")
    
    # Get DATABASE_URL
    db_url = get_heroku_db_url(app_name)
    
    if db_url:
        print("\n🎯 Next steps:")
        print("1. Copy the DATABASE_URL above")
        print("2. Run the deployment:")
        print(f'   export DATABASE_URL="{db_url}"')
        print("   python3 deploy_to_heroku_postgres.py")
        print("\nOr run directly:")
        print(f'   python3 deploy_to_heroku_postgres.py --database-url "{db_url}"')
        
        # Optionally set as environment variable
        response = input("\n🔧 Set DATABASE_URL as environment variable now? (y/n): ")
        if response.lower() == 'y':
            os.environ['DATABASE_URL'] = db_url
            print("✅ DATABASE_URL set for this session")
            print("Run: python3 deploy_to_heroku_postgres.py")
    else:
        print("\n❌ Could not retrieve DATABASE_URL")
        print("Manual steps:")
        print("1. Go to Heroku Dashboard")
        print("2. Select your app") 
        print("3. Go to Settings > Config Vars")
        print("4. Copy the DATABASE_URL value")
        print("5. Run: python3 deploy_to_heroku_postgres.py --database-url 'YOUR_URL'")

if __name__ == "__main__":
    main() 
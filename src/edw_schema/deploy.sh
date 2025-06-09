#!/bin/bash

# Fantasy Football EDW Deployment Script
# Simple wrapper for common deployment tasks

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to check if DATABASE_URL is set
check_database_url() {
    if [ -z "$DATABASE_URL" ]; then
        log_error "DATABASE_URL environment variable is not set"
        log_info "Set it manually or use Heroku CLI:"
        echo "  export DATABASE_URL='your_database_url'"
        echo "  OR"
        echo "  export DATABASE_URL=\$(heroku config:get DATABASE_URL --app your-app-name)"
        exit 1
    fi
    log_success "DATABASE_URL is configured"
}

# Function to show usage
show_usage() {
    echo "Fantasy Football EDW Deployment Script"
    echo ""
    echo "Usage:"
    echo "  $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  deploy          - Complete EDW deployment (schema + ETL + verification)"
    echo "  rebuild         - Force complete rebuild (truncate all tables)"
    echo "  verify          - Run verification only"
    echo "  heroku-deploy   - Deploy using Heroku database"
    echo "  quick-check     - Quick verification of key metrics"
    echo ""
    echo "Options:"
    echo "  --app NAME      - Heroku app name (for heroku-deploy)"
    echo "  --help          - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 deploy                              # Standard deployment"
    echo "  $0 rebuild                             # Clean rebuild"
    echo "  $0 heroku-deploy --app my-ff-app       # Deploy with Heroku"
    echo "  $0 verify                              # Verification only"
}

# Function for Heroku deployment
heroku_deploy() {
    local app_name="$1"
    
    if [ -z "$app_name" ]; then
        log_error "Heroku app name required"
        echo "Usage: $0 heroku-deploy --app YOUR_APP_NAME"
        exit 1
    fi
    
    log_info "Getting DATABASE_URL from Heroku app: $app_name"
    export DATABASE_URL=$(heroku config:get DATABASE_URL --app "$app_name")
    
    if [ -z "$DATABASE_URL" ]; then
        log_error "Could not get DATABASE_URL from Heroku app: $app_name"
        exit 1
    fi
    
    log_success "Retrieved DATABASE_URL from Heroku"
    log_info "Running complete deployment..."
    python3 deploy_complete_edw.py --force-rebuild
}

# Function for quick verification
quick_check() {
    check_database_url
    
    log_info "Running quick verification checks..."
    
    python3 -c "
import os
from sqlalchemy import create_engine, text

url = os.getenv('DATABASE_URL').replace('postgres://', 'postgresql://', 1)
engine = create_engine(url)

print('📊 Quick EDW Health Check')
print('=' * 30)

try:
    with engine.connect() as conn:
        # Check league count
        result = conn.execute(text('SELECT COUNT(*) FROM edw.dim_league'))
        leagues = result.scalar()
        status = '✅' if leagues == 20 else '❌'
        print(f'{status} Leagues: {leagues} (expected: 20)')
        
        # Check matchup count
        result = conn.execute(text('SELECT COUNT(*) FROM edw.fact_matchup'))
        matchups = result.scalar()
        status = '✅' if matchups > 1000 else '❌'
        print(f'{status} Matchups: {matchups:,}')
        
        # Check recent season
        result = conn.execute(text('SELECT MAX(season_year) FROM edw.dim_league'))
        max_season = result.scalar()
        status = '✅' if max_season >= 2024 else '❌'
        print(f'{status} Latest Season: {max_season}')
        
        print()
        print('🏈 EDW Status: HEALTHY' if leagues == 20 and matchups > 1000 else '⚠️  EDW Status: NEEDS ATTENTION')

except Exception as e:
    print(f'❌ Error connecting to EDW: {e}')
    exit(1)
"
}

# Main script logic
main() {
    case "$1" in
        "deploy")
            log_info "Starting complete EDW deployment..."
            check_database_url
            python3 deploy_complete_edw.py
            ;;
        "rebuild")
            log_info "Starting complete EDW rebuild..."
            check_database_url
            python3 deploy_complete_edw.py --force-rebuild
            ;;
        "verify")
            log_info "Running EDW verification..."
            check_database_url
            python3 deploy_complete_edw.py --verify-only
            ;;
        "heroku-deploy")
            shift
            app_name=""
            while [[ $# -gt 0 ]]; do
                case $1 in
                    --app)
                        app_name="$2"
                        shift 2
                        ;;
                    *)
                        log_error "Unknown option: $1"
                        show_usage
                        exit 1
                        ;;
                esac
            done
            heroku_deploy "$app_name"
            ;;
        "quick-check")
            quick_check
            ;;
        "--help"|"help"|"")
            show_usage
            ;;
        *)
            log_error "Unknown command: $1"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@" 
#!/usr/bin/env python3
"""
Fantasy Football Data Deployment
Entry point script for deploying data to Heroku Postgres
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.deployment.heroku_deployer import main

if __name__ == "__main__":
    main() 
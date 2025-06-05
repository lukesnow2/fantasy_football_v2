#!/usr/bin/env python3
"""
Full Fantasy Football Data Extraction
Entry point script for complete historical data extraction
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.extractors.comprehensive_data_extractor import YahooFantasyExtractor
from src.extractors.draft_extractor import main as extract_drafts
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run complete data extraction including drafts"""
    start_time = datetime.now()
    logger.info("🚀 Starting FULL fantasy football data extraction")
    
    try:
        # Step 1: Extract main data
        logger.info("📊 Step 1: Extracting main fantasy data...")
        extractor = YahooFantasyExtractor()
        
        if not extractor.authenticate():
            logger.error("❌ Authentication failed")
            return None
        
        # Extract all data
        all_data = extractor.extract_all_data()
        
        # Save main data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        main_file = f'data/current/yahoo_fantasy_FINAL_complete_data_{timestamp}.json'
        
        with open(main_file, 'w') as f:
            json.dump(all_data, f, indent=2, default=str)
        
        logger.info(f"✅ Main data saved: {main_file}")
        
        # Step 2: Extract draft data and merge
        logger.info("🏈 Step 2: Extracting draft data...")
        draft_file = extract_drafts()
        
        if draft_file:
            logger.info(f"✅ Complete extraction finished: {draft_file}")
            return draft_file
        else:
            logger.info(f"✅ Main extraction finished: {main_file}")
            return main_file
            
    except Exception as e:
        logger.error(f"❌ Full extraction failed: {e}")
        return None

if __name__ == "__main__":
    main() 
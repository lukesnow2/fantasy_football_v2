#!/usr/bin/env python3
"""
Weekly Fantasy Football Data Extraction
Entry point script for automated weekly data updates
"""

import sys
import os
import argparse

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.extractors.weekly_extractor import WeeklyDataExtractor

def main():
    """Run weekly data extraction"""
    parser = argparse.ArgumentParser(description='Weekly Fantasy Football Data Extraction')
    parser.add_argument('--force', action='store_true', 
                       help='Force extraction even during off-season (for testing)')
    
    args = parser.parse_args()
    
    extractor = WeeklyDataExtractor()
    return extractor.run(force_run=args.force)

if __name__ == "__main__":
    main() 
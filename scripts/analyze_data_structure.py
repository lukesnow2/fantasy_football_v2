#!/usr/bin/env python3
"""
Data Structure Analyzer for Yahoo Fantasy Data
Analyzes data structure and detects potential duplicates to inform merge strategies
"""

import json
import logging
from collections import defaultdict, Counter
from typing import Dict, List, Any, Set, Tuple
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataStructureAnalyzer:
    """Analyzes data structure and duplication patterns"""
    
    def __init__(self, data_file: str):
        self.data_file = data_file
        self.data = None
        self.analysis_results = {}
        
    def load_data(self) -> bool:
        """Load data from JSON file"""
        try:
            logger.info(f"📂 Loading data from {self.data_file}...")
            
            with open(self.data_file, 'r') as f:
                self.data = json.load(f)
            
            logger.info("✅ Data loaded successfully!")
            logger.info(f"📊 Tables found: {list(self.data.keys())}")
            for table, records in self.data.items():
                logger.info(f"  - {table}: {len(records):,} records")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return False
    
    def analyze_table_structure(self, table_name: str, records: List[Dict]) -> Dict:
        """Analyze structure of a specific table"""
        if not records:
            return {"error": "No records to analyze"}
        
        logger.info(f"\n🔍 Analyzing {table_name} structure...")
        
        # Get all field names and types
        all_fields = set()
        field_types = defaultdict(set)
        field_examples = {}
        
        for record in records[:10]:  # Sample first 10 records
            for field, value in record.items():
                all_fields.add(field)
                field_types[field].add(type(value).__name__)
                if field not in field_examples:
                    field_examples[field] = value
        
        # Primary key analysis
        potential_pk_fields = []
        for field in all_fields:
            values = [r.get(field) for r in records]
            unique_values = len(set(str(v) for v in values if v is not None))
            
            if unique_values == len(records):
                potential_pk_fields.append(field)
        
        # Composite key analysis
        composite_candidates = []
        if len(potential_pk_fields) < 1:
            # Try common composite key combinations
            common_combinations = [
                ['league_id', 'team_id'],
                ['league_id', 'week'],
                ['team_id', 'week'],
                ['league_id', 'player_id'],
                ['team_id', 'player_id', 'week'],
                ['league_id', 'team1_id', 'team2_id', 'week'],
                ['league_id', 'pick_number'],
                ['league_id', 'round_number', 'pick_number']
            ]
            
            for combo in common_combinations:
                if all(field in all_fields for field in combo):
                    composite_values = set()
                    for record in records:
                        composite_key = tuple(str(record.get(field, '')) for field in combo)
                        composite_values.add(composite_key)
                    
                    if len(composite_values) == len(records):
                        composite_candidates.append(combo)
        
        return {
            "record_count": len(records),
            "fields": sorted(all_fields),
            "field_types": dict(field_types),
            "field_examples": field_examples,
            "potential_primary_keys": potential_pk_fields,
            "composite_key_candidates": composite_candidates
        }
    
    def detect_duplicates(self, table_name: str, records: List[Dict]) -> Dict:
        """Detect duplicates in a table based on various key strategies"""
        if not records:
            return {"duplicates_found": False}
        
        logger.info(f"🔍 Detecting duplicates in {table_name}...")
        
        # Strategy 1: Exact record duplicates
        exact_duplicates = []
        record_hashes = defaultdict(list)
        
        for i, record in enumerate(records):
            # Create hash of all fields except metadata
            key_fields = {k: v for k, v in record.items() 
                         if k not in ['extracted_at', 'created_at', 'updated_at']}
            record_hash = str(sorted(key_fields.items()))
            record_hashes[record_hash].append(i)
        
        for record_hash, indices in record_hashes.items():
            if len(indices) > 1:
                exact_duplicates.append({
                    "indices": indices,
                    "count": len(indices),
                    "sample_record": records[indices[0]]
                })
        
        # Strategy 2: Primary key duplicates
        pk_duplicates = {}
        
        # Common primary key patterns by table
        pk_patterns = {
            'leagues': ['league_id'],
            'teams': ['team_id'],
            'rosters': ['roster_id', ('team_id', 'player_id', 'week')],
            'matchups': ['matchup_id', ('league_id', 'week', 'team1_id', 'team2_id')],
            'transactions': ['transaction_id', ('league_id', 'timestamp', 'player_id')],
            'draft_picks': ['draft_pick_id', ('league_id', 'pick_number'), ('league_id', 'round_number', 'team_id')]
        }
        
        patterns_to_check = pk_patterns.get(table_name, [])
        
        for pattern in patterns_to_check:
            if isinstance(pattern, str):
                # Single field primary key
                if pattern in records[0]:
                    values = [r.get(pattern) for r in records]
                    value_counts = Counter(values)
                    duplicates = {v: count for v, count in value_counts.items() if count > 1}
                    if duplicates:
                        pk_duplicates[pattern] = duplicates
            
            elif isinstance(pattern, tuple):
                # Composite primary key
                if all(field in records[0] for field in pattern):
                    composite_values = []
                    for record in records:
                        composite_key = tuple(record.get(field) for field in pattern)
                        composite_values.append(composite_key)
                    
                    value_counts = Counter(composite_values)
                    duplicates = {v: count for v, count in value_counts.items() if count > 1}
                    if duplicates:
                        pk_duplicates[str(pattern)] = duplicates
        
        return {
            "duplicates_found": len(exact_duplicates) > 0 or len(pk_duplicates) > 0,
            "exact_duplicates": exact_duplicates[:5],  # Limit to first 5 for readability
            "exact_duplicate_count": len(exact_duplicates),
            "primary_key_duplicates": pk_duplicates,
            "total_records": len(records)
        }
    
    def recommend_merge_strategy(self, table_name: str, structure: Dict, duplicates: Dict) -> Dict:
        """Recommend merge strategy based on analysis"""
        
        strategy_recommendations = {
            'leagues': {
                'strategy': 'UPSERT',
                'rationale': 'League data changes infrequently, UPSERT on league_id',
                'key_field': 'league_id',
                'update_fields': ['name', 'current_week', 'draft_status', 'extracted_at']
            },
            'teams': {
                'strategy': 'UPSERT', 
                'rationale': 'Team data changes occasionally (standings), UPSERT on team_id',
                'key_field': 'team_id',
                'update_fields': ['wins', 'losses', 'ties', 'points_for', 'points_against', 'playoff_seed', 'faab_balance', 'extracted_at']
            },
            'rosters': {
                'strategy': 'INCREMENTAL_APPEND',
                'rationale': 'Roster data is time-series (weekly), append new weeks only',
                'key_field': ['team_id', 'week', 'player_id'],
                'filter_field': 'week',
                'description': 'Delete existing records for current week, then append new data'
            },
            'matchups': {
                'strategy': 'INCREMENTAL_APPEND',
                'rationale': 'Matchup data is time-series (weekly), append new weeks only',
                'key_field': ['league_id', 'week'],
                'filter_field': 'week',
                'description': 'Delete existing records for current week, then append new data'
            },
            'transactions': {
                'strategy': 'APPEND_ONLY',
                'rationale': 'Transaction data is immutable historical events',
                'key_field': 'transaction_id',
                'description': 'Append new transactions, skip duplicates based on transaction_id'
            },
            'draft_picks': {
                'strategy': 'APPEND_ONLY',
                'rationale': 'Draft data is immutable historical events',  
                'key_field': 'draft_pick_id',
                'description': 'Append new draft picks, skip duplicates based on draft_pick_id'
            }
        }
        
        base_strategy = strategy_recommendations.get(table_name, {
            'strategy': 'REPLACE',
            'rationale': 'Unknown table, use safe REPLACE strategy',
            'description': 'Full table replacement for unknown data pattern'
        })
        
        # Adjust based on duplicate analysis
        if duplicates['duplicates_found']:
            base_strategy['warnings'] = []
            if duplicates['exact_duplicate_count'] > 0:
                base_strategy['warnings'].append(f"Found {duplicates['exact_duplicate_count']} exact duplicates")
            if duplicates['primary_key_duplicates']:
                base_strategy['warnings'].append(f"Found primary key duplicates: {list(duplicates['primary_key_duplicates'].keys())}")
        
        return base_strategy
    
    def analyze_all_tables(self) -> bool:
        """Analyze all tables in the dataset"""
        if not self.data:
            logger.error("❌ No data loaded")
            return False
        
        logger.info("🔍 Analyzing all tables...")
        
        for table_name, records in self.data.items():
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 TABLE: {table_name.upper()}")
            logger.info(f"{'='*60}")
            
            # Structure analysis
            structure = self.analyze_table_structure(table_name, records)
            
            # Duplicate detection
            duplicates = self.detect_duplicates(table_name, records)
            
            # Merge strategy recommendation
            strategy = self.recommend_merge_strategy(table_name, structure, duplicates)
            
            # Store results
            self.analysis_results[table_name] = {
                'structure': structure,
                'duplicates': duplicates,
                'strategy': strategy
            }
            
            # Print summary
            self.print_table_summary(table_name, structure, duplicates, strategy)
        
        return True
    
    def print_table_summary(self, table_name: str, structure: Dict, duplicates: Dict, strategy: Dict):
        """Print summary for a table"""
        print(f"\n📋 SUMMARY:")
        print(f"Records: {structure.get('record_count', 0):,}")
        print(f"Fields: {len(structure.get('fields', []))}")
        
        if structure.get('potential_primary_keys'):
            print(f"Primary Keys: {', '.join(structure['potential_primary_keys'])}")
        elif structure.get('composite_key_candidates'):
            print(f"Composite Keys: {structure['composite_key_candidates'][0]}")
        else:
            print("Primary Keys: ⚠️  None found")
        
        # Duplicates
        if duplicates['duplicates_found']:
            print(f"❌ Duplicates Found:")
            if duplicates['exact_duplicate_count'] > 0:
                print(f"  - Exact duplicates: {duplicates['exact_duplicate_count']}")
            if duplicates['primary_key_duplicates']:
                for key, dups in duplicates['primary_key_duplicates'].items():
                    print(f"  - {key} duplicates: {len(dups)}")
        else:
            print("✅ No duplicates detected")
        
        # Strategy
        print(f"\n🎯 RECOMMENDED STRATEGY: {strategy['strategy']}")
        print(f"Rationale: {strategy['rationale']}")
        
        if 'warnings' in strategy:
            for warning in strategy['warnings']:
                print(f"⚠️  {warning}")
    
    def generate_report(self, output_file: str = None) -> bool:
        """Generate comprehensive analysis report"""
        if not self.analysis_results:
            logger.error("❌ No analysis results to report")
            return False
        
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"data_structure_analysis_{timestamp}.json"
        
        try:
            with open(output_file, 'w') as f:
                json.dump(self.analysis_results, f, indent=2, default=str)
            
            logger.info(f"📄 Analysis report saved: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving report: {e}")
            return False

def main():
    """Main analysis entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze data structure and detect duplicates')
    parser.add_argument('--data-file', 
                       default='data/current/yahoo_fantasy_FINAL_complete_data_20250605_101225.json',
                       help='Data file to analyze')
    parser.add_argument('--output', 
                       help='Output file for analysis report')
    
    args = parser.parse_args()
    
    logger.info("🔍 Starting data structure analysis...")
    logger.info(f"📂 Data file: {args.data_file}")
    
    try:
        analyzer = DataStructureAnalyzer(args.data_file)
        
        if not analyzer.load_data():
            sys.exit(1)
        
        if not analyzer.analyze_all_tables():
            sys.exit(1)
        
        if analyzer.generate_report(args.output):
            logger.info("\n🎉 Analysis completed successfully!")
            logger.info("📋 Check the analysis report for detailed recommendations")
        else:
            logger.error("❌ Failed to generate report")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Analysis error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
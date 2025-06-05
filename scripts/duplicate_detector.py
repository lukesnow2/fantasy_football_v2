#!/usr/bin/env python3
"""
Duplicate Detection and Alerting System
Monitors database and data files for duplicate records
"""

import json
import logging
import os
import sys
from collections import defaultdict, Counter
from typing import Dict, List, Any, Tuple
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DuplicateDetector:
    """Comprehensive duplicate detection system"""
    
    # Primary key definitions for each table
    PRIMARY_KEYS = {
        'leagues': 'league_id',
        'teams': 'team_id', 
        'rosters': 'roster_id',
        'matchups': 'matchup_id',
        'transactions': 'transaction_id',
        'draft_picks': 'draft_pick_id'
    }
    
    # Composite business keys for additional validation
    BUSINESS_KEYS = {
        'leagues': ['league_id'],
        'teams': ['league_id', 'name'],  # Same name in same league
        'rosters': ['team_id', 'week', 'player_id'],  # Player can't be on team twice in same week
        'matchups': ['league_id', 'week', 'team1_id', 'team2_id'],  # Same matchup
        'transactions': ['league_id', 'timestamp', 'player_id', 'type'],  # Same transaction
        'draft_picks': ['league_id', 'pick_number']  # Same pick number in draft
    }
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        self.engine = None
        self.alerts = []
        
    def connect_database(self) -> bool:
        """Connect to database if URL provided"""
        if not self.database_url:
            logger.info("📋 No database URL provided, skipping database checks")
            return True
            
        try:
            logger.info("🔌 Connecting to database...")
            
            # Fix URL for newer SQLAlchemy
            url = self.database_url.replace('postgres://', 'postgresql://', 1)
            self.engine = create_engine(url)
            
            # Test connection
            with self.engine.connect() as conn:
                version = conn.execute(text("SELECT version()")).fetchone()[0]
                logger.info(f"✅ Connected: {version.split()[0:2]}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def check_file_duplicates(self, data_file: str) -> Dict[str, Any]:
        """Check for duplicates in data file"""
        try:
            logger.info(f"📂 Checking duplicates in {data_file}...")
            
            with open(data_file, 'r') as f:
                data = json.load(f)
            
            file_results = {
                'file': data_file,
                'tables_checked': 0,
                'total_duplicates': 0,
                'table_results': {}
            }
            
            for table_name, records in data.items():
                if not records:
                    continue
                    
                logger.info(f"  🔍 Checking {table_name} ({len(records):,} records)...")
                
                table_duplicates = self.detect_table_duplicates(table_name, records)
                file_results['table_results'][table_name] = table_duplicates
                file_results['tables_checked'] += 1
                
                if table_duplicates['has_duplicates']:
                    file_results['total_duplicates'] += table_duplicates['duplicate_count']
                    self.alerts.append({
                        'type': 'FILE_DUPLICATES',
                        'severity': 'HIGH',
                        'table': table_name,
                        'file': data_file,
                        'details': table_duplicates
                    })
            
            logger.info(f"  ✅ File check complete: {file_results['total_duplicates']} duplicates found")
            return file_results
            
        except Exception as e:
            logger.error(f"❌ Error checking file duplicates: {e}")
            return {'error': str(e)}
    
    def detect_table_duplicates(self, table_name: str, records: List[Dict]) -> Dict[str, Any]:
        """Detect duplicates in a table using multiple strategies"""
        result = {
            'table': table_name,
            'record_count': len(records),
            'has_duplicates': False,
            'duplicate_count': 0,
            'primary_key_duplicates': {},
            'business_key_duplicates': {},
            'exact_duplicates': []
        }
        
        if not records:
            return result
        
        # 1. Primary key duplicates
        pk_field = self.PRIMARY_KEYS.get(table_name)
        if pk_field and pk_field in records[0]:
            pk_values = [r.get(pk_field) for r in records]
            pk_counts = Counter(pk_values)
            pk_duplicates = {v: count for v, count in pk_counts.items() if count > 1}
            
            if pk_duplicates:
                result['has_duplicates'] = True
                result['primary_key_duplicates'] = pk_duplicates
                result['duplicate_count'] += sum(pk_duplicates.values()) - len(pk_duplicates)
        
        # 2. Business key duplicates
        business_keys = self.BUSINESS_KEYS.get(table_name, [])
        if business_keys and all(key in records[0] for key in business_keys):
            business_values = []
            for record in records:
                bk_tuple = tuple(str(record.get(key, '')) for key in business_keys)
                business_values.append(bk_tuple)
            
            bk_counts = Counter(business_values)
            bk_duplicates = {v: count for v, count in bk_counts.items() if count > 1}
            
            if bk_duplicates:
                result['has_duplicates'] = True
                result['business_key_duplicates'] = {
                    'keys': business_keys,
                    'duplicates': dict(list(bk_duplicates.items())[:5])  # Limit for readability
                }
                if not result['duplicate_count']:  # Don't double count if PK already found
                    result['duplicate_count'] += sum(bk_duplicates.values()) - len(bk_duplicates)
        
        # 3. Exact record duplicates (excluding metadata)
        record_hashes = defaultdict(list)
        for i, record in enumerate(records):
            # Exclude timestamp fields for comparison
            clean_record = {k: v for k, v in record.items() 
                          if k not in ['extracted_at', 'created_at', 'updated_at']}
            record_hash = str(sorted(clean_record.items()))
            record_hashes[record_hash].append(i)
        
        exact_dups = [(hash_val, indices) for hash_val, indices in record_hashes.items() if len(indices) > 1]
        if exact_dups:
            result['has_duplicates'] = True
            result['exact_duplicates'] = exact_dups[:3]  # Limit for readability
            if not result['duplicate_count']:  # Don't double count
                result['duplicate_count'] += sum(len(indices) - 1 for _, indices in exact_dups)
        
        return result
    
    def check_database_duplicates(self) -> Dict[str, Any]:
        """Check for duplicates in database"""
        if not self.engine:
            return {'error': 'No database connection'}
        
        logger.info("🗄️ Checking database duplicates...")
        
        db_results = {
            'tables_checked': 0,
            'total_duplicates': 0,
            'table_results': {}
        }
        
        with self.engine.connect() as conn:
            # Get all tables
            tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """
            
            try:
                tables = [row[0] for row in conn.execute(text(tables_query))]
                
                for table_name in tables:
                    if table_name in self.PRIMARY_KEYS:
                        logger.info(f"  🔍 Checking {table_name}...")
                        
                        table_duplicates = self.check_database_table_duplicates(conn, table_name)
                        db_results['table_results'][table_name] = table_duplicates
                        db_results['tables_checked'] += 1
                        
                        if table_duplicates['has_duplicates']:
                            db_results['total_duplicates'] += table_duplicates['duplicate_count']
                            self.alerts.append({
                                'type': 'DATABASE_DUPLICATES',
                                'severity': 'CRITICAL',
                                'table': table_name,
                                'details': table_duplicates
                            })
                
                logger.info(f"  ✅ Database check complete: {db_results['total_duplicates']} duplicates found")
                
            except Exception as e:
                logger.error(f"❌ Error checking database: {e}")
                db_results['error'] = str(e)
        
        return db_results
    
    def check_database_table_duplicates(self, conn, table_name: str) -> Dict[str, Any]:
        """Check duplicates in a specific database table"""
        result = {
            'table': table_name,
            'has_duplicates': False,
            'duplicate_count': 0,
            'primary_key_duplicates': {},
            'business_key_duplicates': {}
        }
        
        try:
            # Get record count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            total_records = conn.execute(text(count_query)).fetchone()[0]
            result['record_count'] = total_records
            
            # 1. Primary key duplicates
            pk_field = self.PRIMARY_KEYS.get(table_name)
            if pk_field:
                pk_dup_query = f"""
                    SELECT {pk_field}, COUNT(*) as cnt 
                    FROM {table_name} 
                    GROUP BY {pk_field} 
                    HAVING COUNT(*) > 1
                """
                
                pk_duplicates = {}
                for row in conn.execute(text(pk_dup_query)):
                    pk_duplicates[row[0]] = row[1]
                
                if pk_duplicates:
                    result['has_duplicates'] = True
                    result['primary_key_duplicates'] = pk_duplicates
                    result['duplicate_count'] += sum(pk_duplicates.values()) - len(pk_duplicates)
            
            # 2. Business key duplicates
            business_keys = self.BUSINESS_KEYS.get(table_name, [])
            if business_keys:
                # Check if all business key columns exist
                columns_query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND table_schema = 'public'
                """
                
                existing_columns = [row[0] for row in conn.execute(text(columns_query))]
                valid_business_keys = [key for key in business_keys if key in existing_columns]
                
                if valid_business_keys:
                    business_fields = ', '.join(valid_business_keys)
                    bk_dup_query = f"""
                        SELECT {business_fields}, COUNT(*) as cnt 
                        FROM {table_name} 
                        GROUP BY {business_fields} 
                        HAVING COUNT(*) > 1
                        LIMIT 10
                    """
                    
                    bk_duplicates = []
                    for row in conn.execute(text(bk_dup_query)):
                        bk_duplicates.append({
                            'keys': dict(zip(valid_business_keys, row[:-1])),
                            'count': row[-1]
                        })
                    
                    if bk_duplicates:
                        result['has_duplicates'] = True
                        result['business_key_duplicates'] = {
                            'keys': valid_business_keys,
                            'duplicates': bk_duplicates
                        }
                        if not result['duplicate_count']:  # Don't double count
                            result['duplicate_count'] += sum(dup['count'] - 1 for dup in bk_duplicates)
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def generate_alerts_report(self) -> str:
        """Generate comprehensive alerts report"""
        if not self.alerts:
            return "✅ NO DUPLICATES DETECTED - DATA INTEGRITY CONFIRMED"
        
        report = ["🚨 DUPLICATE DETECTION ALERTS", "=" * 50, ""]
        
        critical_alerts = [a for a in self.alerts if a['severity'] == 'CRITICAL']
        high_alerts = [a for a in self.alerts if a['severity'] == 'HIGH']
        
        if critical_alerts:
            report.extend(["🔴 CRITICAL ALERTS (Database):", ""])
            for alert in critical_alerts:
                report.append(f"Table: {alert['table']}")
                report.append(f"Type: {alert['type']}")
                if 'duplicate_count' in alert['details']:
                    report.append(f"Duplicates: {alert['details']['duplicate_count']}")
                report.append("")
        
        if high_alerts:
            report.extend(["🟡 HIGH ALERTS (Files):", ""])
            for alert in high_alerts:
                report.append(f"File: {alert['file']}")
                report.append(f"Table: {alert['table']}")
                if 'duplicate_count' in alert['details']:
                    report.append(f"Duplicates: {alert['details']['duplicate_count']}")
                report.append("")
        
        return "\n".join(report)
    
    def run_comprehensive_check(self, data_files: List[str] = None) -> Dict[str, Any]:
        """Run comprehensive duplicate detection"""
        logger.info("🔍 Starting comprehensive duplicate detection...")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'file_results': [],
            'database_results': {},
            'summary': {
                'files_checked': 0,
                'tables_checked': 0,
                'total_duplicates': 0,
                'critical_alerts': 0,
                'high_alerts': 0
            }
        }
        
        # Check files
        if data_files:
            for data_file in data_files:
                if os.path.exists(data_file):
                    file_result = self.check_file_duplicates(data_file)
                    results['file_results'].append(file_result)
                    results['summary']['files_checked'] += 1
                    
                    if 'total_duplicates' in file_result:
                        results['summary']['total_duplicates'] += file_result['total_duplicates']
        
        # Check database
        if self.connect_database():
            db_result = self.check_database_duplicates()
            results['database_results'] = db_result
            
            if 'total_duplicates' in db_result:
                results['summary']['total_duplicates'] += db_result['total_duplicates']
                results['summary']['tables_checked'] += db_result.get('tables_checked', 0)
        
        # Count alerts
        results['summary']['critical_alerts'] = len([a for a in self.alerts if a['severity'] == 'CRITICAL'])
        results['summary']['high_alerts'] = len([a for a in self.alerts if a['severity'] == 'HIGH'])
        
        return results

def main():
    """Main duplicate detection entry point"""
    import argparse
    import glob
    
    parser = argparse.ArgumentParser(description='Detect duplicates in fantasy football data')
    parser.add_argument('--data-files', nargs='*',
                       default=[],
                       help='Data files to check (supports wildcards, empty for database-only check)')
    parser.add_argument('--database-url',
                       default=os.getenv('DATABASE_URL'),
                       help='Database URL to check (defaults to DATABASE_URL env var)')
    parser.add_argument('--output',
                       help='Output file for results')
    parser.add_argument('--alert-only', action='store_true',
                       help='Only show alerts, suppress detailed output')
    
    args = parser.parse_args()
    
    # Expand wildcards
    data_files = []
    if args.data_files:
        for pattern in args.data_files:
            if '*' in pattern:
                data_files.extend(glob.glob(pattern))
            else:
                data_files.append(pattern)
        
        data_files = [f for f in data_files if os.path.exists(f)]
    
    logger.info("🚨 Starting duplicate detection system...")
    logger.info(f"📂 Files to check: {len(data_files)}")
    logger.info(f"🗄️ Database check: {'Yes' if args.database_url else 'No'}")
    if args.database_url:
        logger.info(f"🔗 Database: {args.database_url[:50]}...")
    
    try:
        detector = DuplicateDetector(args.database_url)
        results = detector.run_comprehensive_check(data_files)
        
        # Generate alerts
        alerts_report = detector.generate_alerts_report()
        
        if args.alert_only:
            print(alerts_report)
        else:
            # Full report
            summary = results['summary']
            print(f"\n📊 DUPLICATE DETECTION SUMMARY")
            print(f"=" * 40)
            print(f"Files checked: {summary['files_checked']}")
            print(f"Database tables checked: {summary.get('tables_checked', 0)}")
            print(f"Total duplicates found: {summary['total_duplicates']}")
            print(f"Critical alerts: {summary['critical_alerts']}")
            print(f"High alerts: {summary['high_alerts']}")
            print(f"\n{alerts_report}")
        
        # Save detailed results
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logger.info(f"📄 Detailed results saved: {args.output}")
        
        # Exit with error code if duplicates found
        summary = results['summary']
        if summary['total_duplicates'] > 0:
            logger.error("❌ Duplicates detected - see alerts above")
            sys.exit(1)
        else:
            logger.info("✅ No duplicates detected - data integrity confirmed")
            
    except Exception as e:
        logger.error(f"❌ Duplicate detection error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
"""
Test script to demonstrate the new data_inserted_at column in DDL generation
"""
from pipeline.extractors.metadata_extractor import SnowflakeMetadataExtractor

# Create a sample metadata structure
metadata = {
    'table_info': {
        'database': 'CIGAM_PRD_RL',
        'schema': 'MORNINGSTAR_MAIN',
        'table': 'FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND',
        'full_name': 'CIGAM_PRD_RL.MORNINGSTAR_MAIN.FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND'
    },
    'columns': [
        {'name': '_ID', 'postgres_type': 'VARCHAR(255)', 'is_nullable': False, 'default_value': None},
        {'name': 'SECID', 'postgres_type': 'VARCHAR(255)', 'is_nullable': True, 'default_value': None},
        {'name': 'NAME', 'postgres_type': 'TEXT', 'is_nullable': True, 'default_value': None},
        {'name': '_TIMESTAMPTO', 'postgres_type': 'TIMESTAMP', 'is_nullable': True, 'default_value': None}
    ],
    'statistics': {
        'row_count': 1000000,
        'size_bytes': 500000000,
        'last_altered': '2024-01-01 12:00:00'
    },
    'primary_keys': [],
    'extracted_at': '2024-01-01 12:00:00'
}

extractor = SnowflakeMetadataExtractor()
ddl = extractor.generate_postgres_ddl(metadata, 'ms', 'FUND_SHARE_CLASS_BASIC_INFO_CA_OPENEND')

print("=" * 80)
print("GENERATED DDL WITH data_inserted_at COLUMN")
print("=" * 80)
print(ddl)
print("=" * 80)

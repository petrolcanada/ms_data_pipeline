#!/usr/bin/env python3
"""
Test Snowflake Connection
Simple script to test Snowflake connectivity using environment variables
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from pipeline.config.settings import get_settings, get_snowflake_connection_params
import snowflake.connector

def test_snowflake_connection():
    """Test Snowflake connection with detailed output"""
    print("Snowflake Connection Test")
    print("=" * 50)
    
    # Load settings outside try block so it's available in except block
    settings = None
    try:
        # Load settings
        settings = get_settings()
        print(f"✓ Environment variables loaded successfully")
        print(f"  User: {settings.snowflake_user}")
        print(f"  Account: {settings.snowflake_account}")
        print(f"  Warehouse: {settings.snowflake_warehouse}")
        print(f"  Database: {settings.snowflake_database}")
        print(f"  Schema: {settings.snowflake_schema}")
        print(f"  Auth Method: {settings.snowflake_auth_method}")
        
        # Get connection parameters
        conn_params = get_snowflake_connection_params()
        print(f"\n✓ Connection parameters prepared")
        print(f"  Parameters: {list(conn_params.keys())}")
        
        # Test connection
        print(f"\n🔄 Attempting to connect to Snowflake...")
        if settings.snowflake_auth_method == "sso":
            print("  Note: SSO authentication will open a browser window")
        
        conn = snowflake.connector.connect(**conn_params)
        print("✅ Connection established successfully!")
        
        # Test basic query
        print(f"\n🔄 Testing basic query...")
        cursor = conn.cursor()
        
        # Get Snowflake version
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"✅ Snowflake Version: {version}")
        
        # Get current user info
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        user_info = cursor.fetchone()
        print(f"✅ Current User: {user_info[0]}")
        print(f"✅ Current Role: {user_info[1]}")
        print(f"✅ Current Warehouse: {user_info[2]}")
        
        # Test database access
        cursor.execute(f"USE DATABASE {settings.snowflake_database}")
        cursor.execute(f"USE SCHEMA {settings.snowflake_schema}")
        print(f"✅ Successfully accessed {settings.snowflake_database}.{settings.snowflake_schema}")
        
        # List some tables (if any)
        cursor.execute(f"""
            SELECT TABLE_NAME, ROW_COUNT, BYTES 
            FROM {settings.snowflake_database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{settings.snowflake_schema}' 
            LIMIT 5
        """)
        tables = cursor.fetchall()
        
        if tables:
            print(f"\n✅ Found {len(tables)} tables in {settings.snowflake_schema} schema:")
            for table in tables:
                table_name, row_count, size_bytes = table
                size_mb = (size_bytes or 0) / (1024 * 1024)
                print(f"  - {table_name}: {row_count or 0:,} rows, {size_mb:.1f} MB")
        else:
            print(f"\n⚠️  No tables found in {settings.snowflake_schema} schema")
        
        # Clean up
        cursor.close()
        conn.close()
        
        print(f"\n🎉 All tests passed! Snowflake connection is working perfectly.")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        
        # Provide specific troubleshooting advice
        error_str = str(e).lower()
        print(f"\n🔧 Troubleshooting:")
        
        if "authentication" in error_str or "login" in error_str:
            print("  - Check your Snowflake credentials in .env file")
            print("  - Verify your username and account are correct")
            if settings and settings.snowflake_auth_method == "sso":
                print("  - Make sure you complete the SSO authentication in your browser")
            elif settings and settings.snowflake_auth_method == "password":
                print("  - Verify your password is correct")
        
        elif "network" in error_str or "connection" in error_str:
            print("  - Check your VPN connection is active")
            print("  - Verify network connectivity to Snowflake")
            print("  - Check if firewall is blocking the connection")
        
        elif "warehouse" in error_str:
            print("  - Verify the warehouse name is correct")
            print("  - Check if you have access to the specified warehouse")
        
        elif "database" in error_str or "schema" in error_str:
            print("  - Verify database and schema names are correct")
            print("  - Check if you have access to the specified database/schema")
        
        else:
            print("  - Check all settings in your .env file")
            print("  - Verify your Snowflake account is active")
            print("  - Try connecting with Snowflake's web interface first")
        
        # Only show configuration if settings were loaded successfully
        if settings:
            print(f"\n📋 Current configuration:")
            print(f"  SNOWFLAKE_USER={settings.snowflake_user}")
            print(f"  SNOWFLAKE_ACCOUNT={settings.snowflake_account}")
            print(f"  SNOWFLAKE_WAREHOUSE={settings.snowflake_warehouse}")
            print(f"  SNOWFLAKE_DATABASE={settings.snowflake_database}")
            print(f"  SNOWFLAKE_SCHEMA={settings.snowflake_schema}")
            print(f"  SNOWFLAKE_AUTH_METHOD={settings.snowflake_auth_method}")
        else:
            print(f"\n📋 Could not load configuration - check your .env file exists and is properly formatted")
        
        return False

if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
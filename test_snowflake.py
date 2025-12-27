#!/usr/bin/env python3
"""
Test Snowflake Connection
Simple script to test Snowflake connectivity using environment variables with SSO browser authentication
"""
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from pipeline.config.settings import get_settings, get_snowflake_connection_params
import snowflake.connector

def test_snowflake_connection():
    """Test Snowflake connection with detailed output"""
    print("Snowflake Connection Test (SSO Browser Authentication)")
    print("=" * 60)
    
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
        print(f"  Role: {settings.snowflake_role or '(default)'}")
        print(f"  Auth Method: {settings.snowflake_auth_method}")
        
        # Get connection parameters
        conn_params = get_snowflake_connection_params()
        print(f"\n✓ Connection parameters prepared")
        print(f"  Parameters: {list(conn_params.keys())}")
        
        # Test connection
        print(f"\n🔄 Attempting to connect to Snowflake...")
        if settings.snowflake_auth_method == "sso":
            print("=" * 60)
            print("🌐 SSO AUTHENTICATION REQUIRED")
            print("=" * 60)
            print("A browser window will open for authentication.")
            print("Please complete the SSO login in your browser.")
            print("Waiting for browser authentication...")
            print("=" * 60)
        
        # Connect with explicit timeout
        conn = snowflake.connector.connect(
            **conn_params,
            login_timeout=120,  # 2 minutes for SSO login
            network_timeout=30   # 30 seconds for network operations
        )
        
        print("\n✅ Connection established successfully!")
        print("   Browser authentication completed.")
        
        # Test basic query
        print(f"\n🔄 Testing basic queries...")
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
        
        # Get current database and schema
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        db_info = cursor.fetchone()
        print(f"✅ Current Database: {db_info[0]}")
        print(f"✅ Current Schema: {db_info[1]}")
        
        # Test database access
        print(f"\n🔄 Testing database access...")
        cursor.execute(f"USE DATABASE {settings.snowflake_database}")
        cursor.execute(f"USE SCHEMA {settings.snowflake_schema}")
        print(f"✅ Successfully accessed {settings.snowflake_database}.{settings.snowflake_schema}")
        
        # List some tables (if any)
        print(f"\n🔄 Querying available tables...")
        cursor.execute(f"""
            SELECT TABLE_NAME, ROW_COUNT, BYTES 
            FROM {settings.snowflake_database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{settings.snowflake_schema}' 
            ORDER BY ROW_COUNT DESC
            LIMIT 5
        """)
        tables = cursor.fetchall()
        
        if tables:
            print(f"✅ Found {len(tables)} tables in {settings.snowflake_schema} schema:")
            for table in tables:
                table_name, row_count, size_bytes = table
                size_mb = (size_bytes or 0) / (1024 * 1024)
                print(f"  - {table_name}: {row_count or 0:,} rows, {size_mb:.1f} MB")
        else:
            print(f"⚠️  No tables found in {settings.snowflake_schema} schema")
            print(f"   (This might be normal if the schema is empty)")
        
        # Clean up
        cursor.close()
        conn.close()
        
        print(f"\n" + "=" * 60)
        print("🎉 All tests passed! Snowflake connection is working perfectly.")
        print("=" * 60)
        return True
        
    except snowflake.connector.errors.DatabaseError as e:
        print(f"\n❌ Snowflake Database Error: {e}")
        error_str = str(e).lower()
        
        print(f"\n🔧 Troubleshooting:")
        if "250001" in str(e):
            print("  ⚠️  Could not connect to Snowflake backend")
            print("  - Verify your SNOWFLAKE_ACCOUNT is correct")
            print("  - Check your account format (e.g., 'xy12345.us-east-1.aws')")
            print("  - Ensure VPN is connected and active")
            print("  - Test by logging into Snowflake web UI first")
        elif "authentication" in error_str or "login" in error_str:
            print("  ⚠️  Authentication failed")
            print("  - Complete the SSO authentication in the browser window")
            print("  - Check if browser window opened (check taskbar)")
            print("  - Verify your username is correct")
            print("  - Try logging into Snowflake web UI to verify credentials")
        elif "warehouse" in error_str:
            print("  ⚠️  Warehouse access issue")
            print("  - Verify warehouse name is correct")
            print("  - Check if your role has access to the warehouse")
            print("  - Try setting SNOWFLAKE_ROLE in .env file")
        elif "database" in error_str or "schema" in error_str:
            print("  ⚠️  Database/Schema access issue")
            print("  - Verify database and schema names are correct")
            print("  - Check if your role has access to them")
            print("  - Try setting SNOWFLAKE_ROLE in .env file")
        
        if settings:
            print(f"\n📋 Current configuration:")
            print(f"  SNOWFLAKE_USER={settings.snowflake_user}")
            print(f"  SNOWFLAKE_ACCOUNT={settings.snowflake_account}")
            print(f"  SNOWFLAKE_WAREHOUSE={settings.snowflake_warehouse}")
            print(f"  SNOWFLAKE_DATABASE={settings.snowflake_database}")
            print(f"  SNOWFLAKE_SCHEMA={settings.snowflake_schema}")
            print(f"  SNOWFLAKE_ROLE={settings.snowflake_role or '(not set - using default)'}")
            print(f"  SNOWFLAKE_AUTH_METHOD={settings.snowflake_auth_method}")
        
        return False
        
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        
        # Provide specific troubleshooting advice
        error_str = str(e).lower()
        print(f"\n🔧 Troubleshooting:")
        
        if "browser" in error_str or "webbrowser" in error_str:
            print("  ⚠️  Browser issue detected")
            print("  - Make sure you're running this on a machine with a browser")
            print("  - Check if browser is installed and accessible")
            print("  - Try running from a terminal with GUI access")
            print("  - If in WSL, ensure X11 forwarding is set up")
        elif "network" in error_str or "connection" in error_str or "timeout" in error_str:
            print("  ⚠️  Network connectivity issue")
            print("  - Check your VPN connection is active")
            print("  - Verify network connectivity to Snowflake")
            print("  - Check if firewall is blocking the connection")
            print("  - Try pinging your Snowflake account URL")
        else:
            print("  - Check all settings in your .env file")
            print("  - Verify your Snowflake account is active")
            print("  - Try connecting with Snowflake's web interface first")
            print("  - Make sure VPN is connected")
        
        # Only show configuration if settings were loaded successfully
        if settings:
            print(f"\n📋 Current configuration:")
            print(f"  SNOWFLAKE_USER={settings.snowflake_user}")
            print(f"  SNOWFLAKE_ACCOUNT={settings.snowflake_account}")
            print(f"  SNOWFLAKE_WAREHOUSE={settings.snowflake_warehouse}")
            print(f"  SNOWFLAKE_DATABASE={settings.snowflake_database}")
            print(f"  SNOWFLAKE_SCHEMA={settings.snowflake_schema}")
            print(f"  SNOWFLAKE_ROLE={settings.snowflake_role or '(not set - using default)'}")
            print(f"  SNOWFLAKE_AUTH_METHOD={settings.snowflake_auth_method}")
        else:
            print(f"\n📋 Could not load configuration - check your .env file exists and is properly formatted")
        
        return False

if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
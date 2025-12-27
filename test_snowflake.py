#!/usr/bin/env python3
"""
Test Snowflake Connection
Simple script to test Snowflake connectivity using environment variables with SSO browser authentication
Based on successful YAML connection practice
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from pipeline.config.settings import get_settings, get_snowflake_connection_params
import snowflake.connector

def test_snowflake_connection():
    """Test Snowflake connection with detailed output - matches successful YAML practice"""
    print("=" * 70)
    print("Snowflake Connection Test (SSO Browser Authentication)")
    print("=" * 70)
    
    # Load settings outside try block so it's available in except block
    settings = None
    try:
        # Load settings
        settings = get_settings()
        print("\n✓ Environment variables loaded successfully")
        print(f"  Account: {settings.snowflake_account}")
        print(f"  User: {settings.snowflake_user}")
        print(f"  Role: {settings.snowflake_role}")
        print(f"  Warehouse: {settings.snowflake_warehouse}")
        print(f"  Database: {settings.snowflake_database}")
        print(f"  Schema: {settings.snowflake_schema}")
        print(f"  Auth Method: {settings.snowflake_auth_method}")
        
        # Get connection parameters
        conn_params = get_snowflake_connection_params()
        print(f"\n✓ Connection parameters prepared")
        print(f"  Parameters: {list(conn_params.keys())}")
        print(f"  Session keep-alive: {conn_params.get('client_session_keep_alive', False)}")
        print(f"  Login timeout: {conn_params.get('login_timeout', 'default')}s")
        
        # Test connection
        if settings.snowflake_auth_method == "sso":
            print("\n" + "=" * 70)
            print("🌐 SSO AUTHENTICATION REQUIRED")
            print("=" * 70)
            print("A browser window will open for authentication.")
            print("Please complete the SSO login in your browser.")
            print("Waiting for browser authentication...")
            print("=" * 70 + "\n")
        
        print("🔄 Connecting to Snowflake...")
        
        # Connect with parameters matching successful YAML practice
        conn = snowflake.connector.connect(**conn_params)
        
        print("✅ Connection established successfully!")
        print("   Browser authentication completed.\n")
        
        # Run the same checks as successful YAML script
        cursor = conn.cursor()
        
        checks = [
            ("SELECT CURRENT_VERSION()", "Snowflake version"),
            ("SELECT CURRENT_ACCOUNT()", "Account"),
            ("SELECT CURRENT_ROLE()", "Role"),
            ("SELECT CURRENT_WAREHOUSE()", "Warehouse"),
            ("SELECT CURRENT_DATABASE()", "Database"),
            ("SELECT CURRENT_SCHEMA()", "Schema"),
            ("SELECT CURRENT_USER()", "User"),
        ]
        
        print("🔄 Running connection verification checks...")
        print("-" * 70)
        for sql, label in checks:
            cursor.execute(sql)
            result = cursor.fetchone()[0]
            print(f"✅ {label}: {result}")
        
        # Test basic query
        print("\n🔄 Testing basic query...")
        cursor.execute("SELECT 1 AS ok")
        test_result = cursor.fetchone()[0]
        print(f"✅ Test SELECT 1 -> {test_result}")
        
        # List available tables in current schema
        print(f"\n🔄 Querying available tables in {settings.snowflake_schema}...")
        cursor.execute(f"""
            SELECT TABLE_NAME, ROW_COUNT, BYTES 
            FROM {settings.snowflake_database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{settings.snowflake_schema}' 
            ORDER BY ROW_COUNT DESC NULLS LAST
            LIMIT 10
        """)
        tables = cursor.fetchall()
        
        if tables:
            print(f"✅ Found {len(tables)} tables (showing top 10 by row count):")
            print("-" * 70)
            for table_name, row_count, size_bytes in tables:
                size_mb = (size_bytes or 0) / (1024 * 1024)
                rows = row_count or 0
                print(f"  • {table_name}")
                print(f"    Rows: {rows:,} | Size: {size_mb:.1f} MB")
        else:
            print(f"⚠️  No tables found in {settings.snowflake_schema} schema")
            print(f"   (This might be normal if the schema is empty)")
        
        # Clean up
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print("🎉 ALL TESTS PASSED!")
        print("=" * 70)
        print("✅ Connection successful")
        print("✅ Authentication working")
        print("✅ Database access verified")
        print("✅ Query execution confirmed")
        print("=" * 70)
        return True
        
    except snowflake.connector.errors.DatabaseError as e:
        print(f"\n❌ Snowflake Database Error")
        print("=" * 70)
        print(f"Error: {e}")
        error_str = str(e)
        
        print(f"\n🔧 Troubleshooting:")
        print("-" * 70)
        
        if "250001" in error_str:
            print("⚠️  Could not connect to Snowflake backend")
            print("   • Verify SNOWFLAKE_ACCOUNT is correct: CIX-CIX")
            print("   • Check account format (use orgname-accountname, not account.region)")
            print("   • Ensure VPN is connected and active")
            print("   • Test by logging into Snowflake web UI first")
            
        elif "authentication" in error_str.lower() or "login" in error_str.lower():
            print("⚠️  Authentication failed")
            print("   • Complete the SSO authentication in the browser window")
            print("   • Check if browser window opened (check taskbar)")
            print("   • Verify SNOWFLAKE_USER is your full email")
            print("   • Try logging into Snowflake web UI to verify credentials")
            
        elif "warehouse" in error_str.lower():
            print("⚠️  Warehouse access issue")
            print("   • Verify SNOWFLAKE_WAREHOUSE name is correct")
            print("   • Check if your role has access to the warehouse")
            print("   • Verify SNOWFLAKE_ROLE is set correctly")
            
        elif "database" in error_str.lower() or "schema" in error_str.lower():
            print("⚠️  Database/Schema access issue")
            print("   • Verify SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA are correct")
            print("   • Check if your role has access to them")
            print("   • Verify SNOWFLAKE_ROLE has proper permissions")
            
        else:
            print("⚠️  General connection error")
            print("   • Check all settings in your .env file")
            print("   • Verify VPN connection is active")
            print("   • Try connecting with Snowflake web UI first")
        
        if settings:
            print(f"\n📋 Current configuration:")
            print("-" * 70)
            print(f"  SNOWFLAKE_ACCOUNT={settings.snowflake_account}")
            print(f"  SNOWFLAKE_USER={settings.snowflake_user}")
            print(f"  SNOWFLAKE_ROLE={settings.snowflake_role}")
            print(f"  SNOWFLAKE_WAREHOUSE={settings.snowflake_warehouse}")
            print(f"  SNOWFLAKE_DATABASE={settings.snowflake_database}")
            print(f"  SNOWFLAKE_SCHEMA={settings.snowflake_schema}")
            print(f"  SNOWFLAKE_AUTH_METHOD={settings.snowflake_auth_method}")
        
        print("=" * 70)
        return False
        
    except Exception as e:
        print(f"\n❌ Connection failed")
        print("=" * 70)
        print(f"Error: {e}")
        
        # Provide specific troubleshooting advice
        error_str = str(e).lower()
        print(f"\n🔧 Troubleshooting:")
        print("-" * 70)
        
        if "browser" in error_str or "webbrowser" in error_str:
            print("⚠️  Browser issue detected")
            print("   • Make sure you're running this on a machine with a browser")
            print("   • Check if browser is installed and accessible")
            print("   • Try running from a terminal with GUI access")
            print("   • If in WSL, ensure X11 forwarding is set up")
            
        elif "network" in error_str or "connection" in error_str or "timeout" in error_str:
            print("⚠️  Network connectivity issue")
            print("   • Check your VPN connection is active")
            print("   • Verify network connectivity to Snowflake")
            print("   • Check if firewall is blocking the connection")
            print("   • Try pinging your Snowflake account URL")
            
        elif "field required" in error_str or "validation error" in error_str:
            print("⚠️  Configuration missing or invalid")
            print("   • Check that .env file exists")
            print("   • Verify all required fields are set:")
            print("     - SNOWFLAKE_USER (full email)")
            print("     - SNOWFLAKE_ACCOUNT (orgname-accountname format)")
            print("     - SNOWFLAKE_ROLE (required for SSO)")
            print("     - SNOWFLAKE_WAREHOUSE")
            print("     - SNOWFLAKE_DATABASE")
            print("     - SNOWFLAKE_SCHEMA")
            
        else:
            print("⚠️  Unexpected error")
            print("   • Check all settings in your .env file")
            print("   • Verify your Snowflake account is active")
            print("   • Try connecting with Snowflake's web interface first")
            print("   • Make sure VPN is connected")
        
        # Only show configuration if settings were loaded successfully
        if settings:
            print(f"\n📋 Current configuration:")
            print("-" * 70)
            print(f"  SNOWFLAKE_ACCOUNT={settings.snowflake_account}")
            print(f"  SNOWFLAKE_USER={settings.snowflake_user}")
            print(f"  SNOWFLAKE_ROLE={settings.snowflake_role}")
            print(f"  SNOWFLAKE_WAREHOUSE={settings.snowflake_warehouse}")
            print(f"  SNOWFLAKE_DATABASE={settings.snowflake_database}")
            print(f"  SNOWFLAKE_SCHEMA={settings.snowflake_schema}")
            print(f"  SNOWFLAKE_AUTH_METHOD={settings.snowflake_auth_method}")
        else:
            print(f"\n📋 Could not load configuration")
            print("-" * 70)
            print("   • Check your .env file exists and is properly formatted")
            print("   • Copy from env.example: cp env.example .env")
            print("   • Fill in all required values")
        
        print("=" * 70)
        return False

if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
#!/usr/bin/env python3
"""
Test PostgreSQL Connection
Simple script to test local PostgreSQL connectivity
"""
import sys
import psycopg2

def test_postgres_connection():
    """Test PostgreSQL connection with different credentials"""
    print("=" * 70)
    print("PostgreSQL Connection Test")
    print("=" * 70)
    
    # Common default settings for local PostgreSQL 16
    test_configs = [
        {
            "name": "Default postgres user",
            "host": "localhost",
            "port": 50211,  # Your PostgreSQL port
            "database": "postgres",
            "user": "postgres",
        },
        {
            "name": "Windows user",
            "host": "localhost",
            "port": 50211,
            "database": "postgres",
            "user": "lzhyx",  # Your Windows username
        }
    ]
    
    print("\nEnter your PostgreSQL password:")
    password = input("Password: ")
    
    for config in test_configs:
        print(f"\n{'=' * 70}")
        print(f"Testing: {config['name']}")
        print(f"{'=' * 70}")
        print(f"Host: {config['host']}")
        print(f"Port: {config['port']}")
        print(f"Database: {config['database']}")
        print(f"User: {config['user']}")
        
        try:
            # Try to connect
            conn = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                database=config['database'],
                user=config['user'],
                password=password
            )
            
            print("\n✅ Connection successful!")
            
            # Get PostgreSQL version
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            print(f"✅ PostgreSQL Version: {version[:50]}...")
            
            # List databases
            cursor.execute("""
                SELECT datname, pg_size_pretty(pg_database_size(datname)) as size
                FROM pg_database 
                WHERE datistemplate = false
                ORDER BY datname
            """)
            databases = cursor.fetchall()
            
            print(f"\n✅ Available Databases:")
            print("-" * 70)
            for db_name, db_size in databases:
                print(f"  • {db_name} ({db_size})")
            
            # Get current user info
            cursor.execute("SELECT current_user, current_database()")
            user, db = cursor.fetchone()
            print(f"\n✅ Current User: {user}")
            print(f"✅ Current Database: {db}")
            
            cursor.close()
            conn.close()
            
            print("\n" + "=" * 70)
            print("🎉 CONNECTION SUCCESSFUL!")
            print("=" * 70)
            print("\nUse these settings in your .env file:")
            print("-" * 70)
            print(f"POSTGRES_HOST={config['host']}")
            print(f"POSTGRES_PORT={config['port']}")
            print(f"POSTGRES_DATABASE={config['database']}")
            print(f"POSTGRES_USER={config['user']}")
            print(f"POSTGRES_PASSWORD=your_password_here")
            print("=" * 70)
            
            return True
            
        except psycopg2.OperationalError as e:
            print(f"\n❌ Connection failed: {e}")
            
            error_str = str(e).lower()
            if "password authentication failed" in error_str:
                print("\n🔧 Troubleshooting:")
                print("  • Password is incorrect")
                print("  • Try the password you set during PostgreSQL installation")
            elif "could not connect" in error_str or "connection refused" in error_str:
                print("\n🔧 Troubleshooting:")
                print("  • PostgreSQL service might not be running")
                print("  • Check if port 50211 is correct")
            elif "database" in error_str and "does not exist" in error_str:
                print("\n🔧 Troubleshooting:")
                print("  • Database doesn't exist")
                print("  • Try 'postgres' database (default)")
            
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
    
    print("\n" + "=" * 70)
    print("❌ All connection attempts failed")
    print("=" * 70)
    print("\n🔧 General Troubleshooting:")
    print("  1. Verify PostgreSQL service is running:")
    print("     Get-Service -Name 'postgresql-x64-16'")
    print("  2. Check your password from installation")
    print("  3. Try pgAdmin 4 to connect first")
    print("  4. Check pg_hba.conf for authentication settings")
    print("=" * 70)
    
    return False

if __name__ == "__main__":
    try:
        success = test_postgres_connection()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user")
        sys.exit(1)

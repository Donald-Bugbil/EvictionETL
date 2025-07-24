import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Print what we're loading
print("Environment variables:")
print(f"ENVIRONMENT: {os.environ.get('ENVIRONMENT', 'NOT SET')}")
print(f"POSTGRES_HOST: {os.environ.get('POSTGRES_HOST', 'NOT SET')}")
print(f"POSTGRES_PORT: {os.environ.get('POSTGRES_PORT', 'NOT SET')}")
print(f"POSTGRES_DB: {os.environ.get('POSTGRES_DB', 'NOT SET')}")
print(f"POSTGRES_USER: {os.environ.get('POSTGRES_USER', 'NOT SET')}")
print(f"POSTGRES_PASSWORD: {'SET' if os.environ.get('POSTGRES_PASSWORD') else 'NOT SET'}")

# Test connection
try:
    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=int(os.environ['POSTGRES_PORT']),
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD']
    )
    print("\n✅ Connection successful!")
    
    # Test a simple query
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"PostgreSQL version: {version[0]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"\n❌ Connection failed: {e}")
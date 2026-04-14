import psycopg

try:
    conn = psycopg.connect("dbname=postgres user=postgres password=pass host=localhost port=5432")
    print("✅ Connected to PostgreSQL successfully!")
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)


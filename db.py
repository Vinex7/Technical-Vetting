import psycopg
import logging

def init_db():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting database initialization...")

    conn = psycopg.connect("dbname=postgres user=postgres password=pass host=localhost port=5432")
    cur = conn.cursor()

    # --- Customers table ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        full_name TEXT NOT NULL,
        signup_date DATE NOT NULL,
        country_code CHAR(2),
        is_active BOOLEAN NOT NULL,
        CONSTRAINT email_lowercase CHECK (email = LOWER(email))
    );
    """)

    # --- Orders table ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
        order_ts TIMESTAMP NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('placed','shipped','cancelled','refunded')),
        total_amount NUMERIC(10,2) NOT NULL CHECK (total_amount >= 0),
        currency CHAR(3) NOT NULL
    );
    """)

    # --- Order Items table ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        order_id INTEGER NOT NULL REFERENCES orders(order_id),
        line_no INTEGER NOT NULL,
        sku TEXT NOT NULL,
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price NUMERIC(10,2) NOT NULL CHECK (unit_price > 0),
        category TEXT,
        PRIMARY KEY (order_id, line_no)
    );
    """)

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Database schema created successfully.")


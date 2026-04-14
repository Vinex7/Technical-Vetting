import logging
import pandas as pd
import psycopg
import yaml

def run_etl():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting ETL process...")

    # Load config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    db_cfg = config["database"]
    files = config["files"]

    # Connect to Postgres
    conn = psycopg.connect(
        dbname=db_cfg["dbname"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        host=db_cfg["host"],
        port=db_cfg["port"]
    )
    cur = conn.cursor()

    # --- Customers ---
    logging.info("Loading customers...")
    customers = pd.read_csv(files["customers"])
    customers["email"] = customers["email"].str.lower()
    customers = customers.drop_duplicates(subset=["customer_id"])
    for _, row in customers.iterrows():
        cur.execute("""
            INSERT INTO customers (customer_id, email, full_name, signup_date, country_code, is_active)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING;
        """, tuple(row))

    # --- Orders ---
    logging.info("Loading orders...")
    orders = pd.read_json(files["orders"], lines=True)
    orders = orders[orders["status"].isin(["placed","shipped","cancelled","refunded"])]
    for _, row in orders.iterrows():
        cur.execute("""
            INSERT INTO orders (order_id, customer_id, order_ts, status, total_amount, currency)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, tuple(row))

    # --- Order Items ---
    logging.info("Loading order_items...")
    order_items = pd.read_csv(files["order_items"])
    order_items = order_items[(order_items["quantity"] > 0) & (order_items["unit_price"] > 0)]
    for _, row in order_items.iterrows():
        cur.execute("""
            INSERT INTO order_items (order_id, line_no, sku, quantity, unit_price, category)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, line_no) DO NOTHING;
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
    logging.info("ETL process completed successfully.")


import sys
from db import init_db
from etl import run_etl

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py [init|run]")
    elif sys.argv[1] == "init":
        init_db()
    elif sys.argv[1] == "run":
        run_etl()


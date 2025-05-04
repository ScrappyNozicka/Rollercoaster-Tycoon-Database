from dotenv import load_dotenv
import os
from pg8000.native import Connection

load_dotenv()

def create_con():
    con = Connection(
        os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        database=os.environ["PGDATABASE"],
    )
    return con

if __name__ == "__main__":
    con = create_con()

def close_db(con):
    con.close()
from src.seed import seed
from db.connection import create_con, close_db
from src.data.index import index as data

db = None
try:
    db = create_con()
    seed(db, **data)
except Exception as e:
    print(e)
finally:
    if db:
        close_db(db)
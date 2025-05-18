from src.connection import create_con, close_db

db = create_con()


def get_parks_data(db):
    close_db(db)

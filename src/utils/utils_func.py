from src.connection import create_con

db = create_con()


def get_parks_data(db):
    columns = [
        "park_id",
        "park_name",
        "year_opened",
        "annual_attendance",
    ]
    raw_parks_data = db.run(
        """
            SELECT *
            FROM parks;
            """
    )
    parks_data = [dict(zip(columns, row)) for row in raw_parks_data]
    return parks_data

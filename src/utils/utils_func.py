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


def get_rides_data(db):
    columns = [
        "ride_id",
        "ride_name",
        "ride_type",
        "year_opened",
        "park_name",
        "votes",
    ]
    raw_rides_data = db.run(
        """
            SELECT *
            FROM rides;
            """
    )
    rides_data = [dict(zip(columns, row)) for row in raw_rides_data]
    return rides_data

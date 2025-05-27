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


def modify_rides_data(db):
    rides_data = get_rides_data(db)
    parks_data = get_parks_data(db)
    park_name_to_id = {
        park["park_name"]: park["park_id"] for park in parks_data
    }
    for ride in rides_data:
        park_name = ride.get("park_name")
        if park_name in park_name_to_id:
            ride["park_id"] = park_name_to_id[park_name]
            ride.pop("park_name", None)

    return rides_data


def modify_raw_parks_data(rides, db):
    parks_data = get_parks_data(db)
    park_name_to_id = {
        park["park_name"]: park["park_id"] for park in parks_data
    }
    for ride in rides:
        park_name = ride.get("park_name")
        if park_name in park_name_to_id:
            ride["park_id"] = park_name_to_id[park_name]
            ride.pop("park_name", None)

    return rides

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


def modify_raw_parks_data(rides, db):
    parks_data = get_parks_data(db)
    park_name_to_id = {
        park["park_name"]: park["park_id"] for park in parks_data
    }

    updated_rides = []
    for ride in rides:
        park_name = ride.get("park_name")
        updated_rides.append(
            {
                "ride_name": ride["ride_name"],
                "ride_type": ride["ride_type"],
                "year_opened": ride["year_opened"],
                "votes": ride["votes"],
                "park_id": park_name_to_id[park_name],
            }
        )

    return updated_rides

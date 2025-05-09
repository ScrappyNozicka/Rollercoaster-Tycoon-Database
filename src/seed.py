from src.connection import create_con, close_db

from src.data.parks import parks
from src.data.rides import rides
from src.data.other_fac import other_fac
from src.data.shops import shops
from src.data.stalls import stalls
from src.data.foods import foods
from src.data.items import items

db = create_con()

def seed(db, parks, rides, shops, stalls, other_fac, foods, items):
    """Seeds database"""
    db.run("DROP TABLE IF EXISTS rides")
    db.run("DROP TABLE IF EXISTS other_fac")
    db.run("DROP TABLE IF EXISTS stalls")
    db.run("DROP TABLE IF EXISTS foods")
    db.run("DROP TABLE IF EXISTS stalls_foods")
    db.run("DROP TABLE IF EXISTS shops")
    db.run("DROP TABLE IF EXISTS items")
    db.run("DROP TABLE IF EXISTS shops_items")
    db.run("DROP TABLE IF EXISTS parks")

    create_parks(db)
    create_rides(db)
    create_other_fac(db)
    # create_stalls(db)
    # create_foods(db)
    # create_shops_foods(db)
    # create_shops(db)
    # create_items(db)
    # create_shops_items(db)

    insert_parks_data(db)
    insert_rides_data(db)
    insert_other_fac_data(db)
    # insert_stalls_data(db)
    # insert_foods_data(db)
    # insert_stalls_foods_data(db)
    # insert_shops_data(db)
    # insert_items_data(db)
    # insert_shops_items_data(db)

def create_parks(db):
    return db.run(
        """
            CREATE TABLE parks 
                (
                park_id SERIAL PRIMARY KEY,
                park_name VARCHAR(127) NOT NULL,
                year_opened INT NOT NULL,
                annual_attendance INT NOT NULL
                )
            """
        )

def insert_parks_data(db):
    insert_query = """
    INSERT INTO parks
    (park_name, year_opened, annual_attendance)
    VALUES
    (:park_name, :year_opened, :annual_attendance)
    """
    for park in parks:
        park_name = park["park_name"]
        year_opened = park["year_opened"]
        annual_attendance = park["annual_attendance"]
        db.run(
            insert_query,
            park_name=park_name,
            year_opened=year_opened,
            annual_attendance=annual_attendance

        )

def create_rides(db):
    return db.run(
        """
            CREATE TABLE rides 
                (
                ride_id SERIAL PRIMARY KEY,
                ride_name VARCHAR(127) NOT NULL,
                ride_type VARCHAR(127) NOT NULL,
                year_opened INT NOT NULL,
                park_name VARCHAR(127) NOT NULL,
                votes INT NOT NULL
                )
            """
        )

def insert_rides_data(db):
    insert_query = """
    INSERT INTO rides
    (ride_name, ride_type, year_opened, park_name, votes)
    VALUES
    (:ride_name, :ride_type, :year_opened, :park_name, :votes)
    """
    for ride in rides:
        ride_name = ride["ride_name"]
        ride_type = ride["ride_type"]
        year_opened = ride["year_opened"]
        park_name = ride["park_name"]
        votes = ride["votes"]
        db.run(
            insert_query,
            ride_name=ride_name,
            ride_type=ride_type,
            year_opened=year_opened,
            park_name=park_name,
            votes=votes
        )

def create_other_fac(db):
    return db.run(
        """
            CREATE TABLE other_fac 
                (
                fac_id SERIAL PRIMARY KEY,
                fac_name VARCHAR(127) NOT NULL,
                park_name VARCHAR(127) NOT NULL
                )
            """
        )

def insert_other_fac_data(db):
    insert_query = """
    INSERT INTO other_fac
    (fac_name, park_name)
    VALUES
    (:fac_name, :park_name )
    """
    for fac in other_fac:
        fac_name = fac["fac_name"]
        park_name = fac["park_name"]
        db.run(
            insert_query,
            fac_name=fac_name,
            park_name=park_name
        )
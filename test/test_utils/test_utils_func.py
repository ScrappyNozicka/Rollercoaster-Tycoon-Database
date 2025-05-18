from src.utils.utils_func import get_parks_data
import pytest
from src.seed import seed
from src.connection import close_db, create_con
from src.data.index import index as data


@pytest.fixture(scope="session")
def db():
    """Runs seed before starting tests.
    Yields a database connection object to be used in tests.
    Closes connection to db after tests have ran"""
    test_db = create_con()
    seed(test_db, **data)
    yield test_db
    close_db(test_db)


def test_get_parks_data(db):
    result = get_parks_data(db)
    expected = [
        {
            "park_id": 1,
            "park_name": "Forest Frontiers",
            "year_opened": 1999,
            "annual_attendance": 250,
        },
        {
            "park_id": 2,
            "park_name": "Dynamite Dunes",
            "year_opened": 2000,
            "annual_attendance": 650,
        },
        {
            "park_id": 3,
            "park_name": "Leafy Lake",
            "year_opened": 2003,
            "annual_attendance": 500,
        },
        {
            "park_id": 4,
            "park_name": "Diamond Heights",
            "year_opened": 2006,
            "annual_attendance": 700,
        },
        {
            "park_id": 5,
            "park_name": "Evergreen Gardens",
            "year_opened": 2009,
            "annual_attendance": 1000,
        },
        {
            "park_id": 6,
            "park_name": "Bumbly Beach",
            "year_opened": 2013,
            "annual_attendance": 750,
        },
        {
            "park_id": 7,
            "park_name": "Trinity Islands",
            "year_opened": 2015,
            "annual_attendance": 750,
        },
        {
            "park_id": 8,
            "park_name": "Katie's Dreamland",
            "year_opened": 2018,
            "annual_attendance": 800,
        },
        {
            "park_id": 9,
            "park_name": "Pokey Park",
            "year_opened": 2021,
            "annual_attendance": 400,
        },
        {
            "park_id": 10,
            "park_name": "White Water Park",
            "year_opened": 2023,
            "annual_attendance": 900,
        },
    ]
    assert result == expected

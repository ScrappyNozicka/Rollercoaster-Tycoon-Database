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

def test_parks_table_exists(db):
    """test if parks table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'parks');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_parks_table_has_park_id_column_as_serial_primary_key(db):
    """Tests if parks table has park_id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'parks' \
                  AND column_name = 'park_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "park_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('parks_park_id_seq'::regclass)" 

def test_parks_table_has_park_name_column(db):
    """Tests if parks table has park name column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'parks' \
                  AND column_name = 'park_name';"
    expected = db.run(test_query)
    assert  expected == [["park_name", "character varying", None]]

def test_parks_table_has_park_id_column(db):
    """Tests if parks table has park id column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'parks' \
                  AND column_name = 'park_id';"
    expected = db.run(test_query)
    assert  expected == [["park_id"]]

def test_parks_table_has_year_opened_column(db):
    """Tests if parks table has year opened column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'parks' \
                  AND column_name = 'year_opened';"
    expected = db.run(test_query)
    assert  expected == [["year_opened"]]

def test_parks_table_has_annual_attendance_column(db):
    """Tests if parks table has annual attendance column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'parks' \
                  AND column_name = 'annual_attendance';"
    expected = db.run(test_query)
    assert  expected == [["annual_attendance"]]
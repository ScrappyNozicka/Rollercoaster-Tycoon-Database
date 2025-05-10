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


def test_rides_table_exists(db):
    """test if rides table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'rides');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_rides_table_has_ride_id_column_as_serial_primary_key(db):
    """Tests if rides table has ride_id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'rides' \
                  AND column_name = 'ride_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "ride_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('rides_ride_id_seq'::regclass)" 

def test_rides_table_has_ride_name_column(db):
    """Tests if rides table has park name column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'rides' \
                  AND column_name = 'ride_name';"
    expected = db.run(test_query)
    assert  expected == [["ride_name", "character varying", None]]

def test_rides_table_has_ride_type_column(db):
    """Tests if rides table has ride type column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'rides' \
                  AND column_name = 'ride_type';"
    expected = db.run(test_query)
    assert  expected == [["ride_type"]]

def test_rides_table_has_year_opened_column(db):
    """Tests if rides table has year opened column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'rides' \
                  AND column_name = 'year_opened';"
    expected = db.run(test_query)
    assert  expected == [["year_opened"]]

def test_rides_table_has_park_name_column(db):
    """Tests if rides table has park name column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'rides' \
                  AND column_name = 'park_name';"
    expected = db.run(test_query)
    assert  expected == [["park_name"]]

def test_rides_table_has_votes_column(db):
    """Tests if rides table has votes column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'rides' \
                  AND column_name = 'votes';"
    expected = db.run(test_query)
    assert  expected == [["votes"]]




def test_other_fac_table_exists(db):
    """test if other facilities table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'other_fac');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_other_fac_has__fac_id_column_as_serial_primary_key(db):
    """Tests if other facilities table has fac_id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'other_fac' \
                  AND column_name = 'fac_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "fac_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('other_fac_fac_id_seq'::regclass)" 

def test_other_fac_table_has_park_name_column(db):
    """Tests if other facilities table has park name column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'other_fac' \
                  AND column_name = 'park_name';"
    expected = db.run(test_query)
    assert  expected == [["park_name", "character varying", None]]

def test_other_fac_table_has_ride_type_column(db):
    """Tests if other facilities table has facility name column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'other_fac' \
                  AND column_name = 'fac_name';"
    expected = db.run(test_query)
    assert  expected == [["fac_name"]]




def test_stalls_table_exists(db):
    """test if stalls table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'stalls');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_stalls_has_other_stall_id_column_as_serial_primary_key(db):
    """Tests if stalls table has stall_id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'stalls' \
                  AND column_name = 'stall_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "stall_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('stalls_stall_id_seq'::regclass)" 

def test_stalls_table_has_stall_name_column(db):
    """Tests if stalls table has stall name column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'stalls' \
                  AND column_name = 'stall_name';"
    expected = db.run(test_query)
    assert  expected == [["stall_name", "character varying", None]]

def test_stalls_table_has_park_name_column(db):
    """Tests if stalls table has park name column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'stalls' \
                  AND column_name = 'park_name';"
    expected = db.run(test_query)
    assert  expected == [["park_name"]]

def test_stalls_table_has_food_served_column(db):
    """Tests if stalls table has food served column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'stalls' \
                  AND column_name = 'foods_served';"
    expected = db.run(test_query)
    assert  expected == [["foods_served"]]

def test_drinks_stall_has_expected_foods_served_value(db):
    """Tests if drinks stall has at least one value in food served column"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Drinks' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) > 0

def test_fruity_ices_stall_has_expected_value(db):
    """Tests if fruity ices stall has at least one value in food served column"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Ice Cream' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) > 0

def test_burger_bar_stall_has_expected_foods_served_value(db):
    """Tests if burger bar stall has at least one value in food served column"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Burgers' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) > 0

def test_candyfloss_stall_has_expected_foods_served_value(db):
    """Tests if candyfloss stall has at least one value in food served column"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Candyfloss' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) > 0

def test_pizza_stall_has_expected_foods_served_value(db):
    """Tests if pizza stall has at least one value in food served column"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Pizza' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) > 0

def test_chip_shop_stall_has_expected_foods_served_value(db):
    """Tests if chip shop stall has at least one value in food served column"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Chips' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) > 0

def test_popcorn_stall_has_expected_foods_served_value(db):
    """Tests if popcorn stall has at least one value in food served column"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Popcorn' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) > 0








def test_foods_table_exists(db):
    """test if foods table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'foods');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_foods_table_has__fac_id_column_as_serial_primary_key(db):
    """Tests if foods table has food_id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'foods' \
                  AND column_name = 'food_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "food_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('foods_food_id_seq'::regclass)" 

def test_foods_table_has_food_name_column(db):
    """Tests if foods table has food name column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'foods' \
                  AND column_name = 'food_name';"
    expected = db.run(test_query)
    assert  expected == [["food_name", "character varying", None]]

def test_foods_table_has_vegan_option_column(db):
    """Tests if foods table has vegan option column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'foods' \
                  AND column_name = 'vegan_option';"
    expected = db.run(test_query)
    assert  expected == [["vegan_option"]]




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
    """Tests if parks table has park id as serial primary key"""
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
    """Tests if rides table has ride id as serial primary key"""
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
    """Tests if other facilities table has facility id as serial primary key"""
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

def test_stalls_has_stall_id_column_as_serial_primary_key(db):
    """Tests if stalls table has stall id as serial primary key"""
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

def test_expected_amount_of_drinks_stalls_sells_drinks(db):
    """Tests for the expected amount of drinks stalls selling drinks"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Drinks' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) == 9

def test_expected_amount_of_fruity_ices_stalls_sells_ice_creams(db):
    """Tests for the expected amount of fruity ices stalls selling ice creams"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Ice Cream' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) == 7

def test_expected_amount_of_burger_bar_stall_sells_burgers(db):
    """Tests for the expected amount of burger bar stalls selling burgers"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Burgers' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) == 6

def test_expected_amount_of_candyfloss_stall_sells_candyfloss(db):
    """Tests for the expected amount of candyfloss stalls selling candyfloss"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Candyfloss' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) == 7

def test_expected_amount_of_pizza_stall_sells_pizzas(db):
    """Tests for the expected amount of pizza stalls selling pizzas"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Pizza' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) == 5

def test_expected_amount_of_chip_shop_stall_sells_chips(db):
    """Tests for the expected amount of chip shop stalls selling chips"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Chips' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) == 5

def test_expected_amount_of_popcorn_stalls_sells_popcorns(db):
    """Tests for the expected amount of popcorn stalls selling popcorn"""
    test_query = "SELECT * \
                  FROM stalls \
                  WHERE 'Popcorn' = ANY(foods_served);"
    expected = db.run(test_query)
    assert len(expected) == 5

def test_foods_table_exists(db):
    """test if foods table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'foods');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_foods_table_has__food_id_column_as_serial_primary_key(db):
    """Tests if foods table has food id as serial primary key"""
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

def test_shops_table_exists(db):
    """test if shops table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'shops');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_shops_has_shop_id_column_as_serial_primary_key(db):
    """Tests if shops table has shop id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'shops' \
                  AND column_name = 'shop_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "shop_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('shops_shop_id_seq'::regclass)" 

def test_shops_table_has_shop_name_column(db):
    """Tests if shops table has shop name column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'shops' \
                  AND column_name = 'shop_name';"
    expected = db.run(test_query)
    assert  expected == [["shop_name", "character varying", None]]

def test_shops_table_has_park_name_column(db):
    """Tests if shops table has park name column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'shops' \
                  AND column_name = 'park_name';"
    expected = db.run(test_query)
    assert  expected == [["park_name"]]

def test_shops_table_has_items_sold_column(db):
    """Tests if shops table has items sold column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'shops' \
                  AND column_name = 'items_sold';"
    expected = db.run(test_query)
    assert  expected == [["items_sold"]]

def test_expected_amount_of_balloon_stall_sells_balloons(db):
    """Tests for the expected amount of balloon stall selling balloons"""
    test_query = "SELECT * \
                  FROM shops \
                  WHERE 'Balloons' = ANY(items_sold);"
    expected = db.run(test_query)
    assert len(expected) == 9

def test_expected_amount_of_information_kiosks_sells_park_maps(db):
    """Tests for the expected amount of information kiosk selling park maps"""
    test_query = "SELECT * \
                  FROM shops \
                  WHERE 'Park Maps' = ANY(items_sold);"
    expected = db.run(test_query)
    assert len(expected) == 7

def test_expected_amount_of_souvenir_stall_sells_cuddly_toys(db):
    """Test for the expected amount of souvenir stalls selling cuddly toys"""
    test_query = "SELECT * \
                  FROM shops \
                  WHERE 'Cuddly Toys' = ANY(items_sold);"
    expected = db.run(test_query)
    assert len(expected) == 5

def test_items_table_exists(db):
    """test if items table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'items');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_items_table_has__item_id_column_as_serial_primary_key(db):
    """Tests if items table has item id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'items' \
                  AND column_name = 'item_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "item_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('items_item_id_seq'::regclass)" 

def test_items_table_has_item_name_column(db):
    """Tests if items table has item name column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'items' \
                  AND column_name = 'item_name';"
    expected = db.run(test_query)
    assert  expected == [["item_name", "character varying", None]]

def test_items_table_has_multi_colour_option_column(db):
    """Tests if items table has multi colour option column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'items' \
                  AND column_name = 'multi_colour_option';"
    expected = db.run(test_query)
    assert  expected == [["multi_colour_option"]]
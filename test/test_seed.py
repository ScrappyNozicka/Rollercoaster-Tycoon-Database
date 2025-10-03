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
    """Tests if rides table has ride name column"""
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

def test_rides_table_has_park_id_column(db):
    """Tests if rides table has park id column"""
    test_query = "SELECT column_name \
                  FROM information_schema.columns \
                  WHERE table_name = 'rides' \
                  AND column_name = 'park_id';"
    expected = db.run(test_query)
    assert  expected == [["park_id"]]

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

def test_other_fac_table_has_park_id_column(db):
    """Tests if other facilities table has park id column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'other_fac' \
                  AND column_name = 'park_id';"
    expected = db.run(test_query)
    assert  expected == [["park_id", "integer", None]]

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

def test_stalls_table_has_park_id_column(db):
    """Tests if stalls table has park id column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'stalls' \
                  AND column_name = 'park_id';"
    expected = db.run(test_query)
    assert  expected == [["park_id", "integer", None]]

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

def test_shops_table_has_park_id_column(db):
    """Tests if shops table has park id column"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'shops' \
                  AND column_name = 'park_id';"
    expected = db.run(test_query)
    assert  expected == [['park_id', 'integer', None]] 

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

def test_bridge_stall_foods_table_exists(db):
    """test if bridge stall foods table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'bridge_stall_foods');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_bridge_stall_foods_table_has_stall_foods_id_column_as_serial_primary_key(db):
    """Tests if bridge stall foods table has stall foods id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'bridge_stall_foods' \
                  AND column_name = 'stall_foods_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "stall_foods_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('bridge_stall_foods_stall_foods_id_seq'::regclass)" 

def test__bridge_stall_foods_table_has_stall_id_column(db):
    """Tests if bridge stall foods table has stall id column"""
    test_query = "SELECT column_name, data_type \
                  FROM information_schema.columns \
                  WHERE table_name = 'bridge_stall_foods' \
                  AND column_name = 'stall_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "stall_id"
    assert  expected[0][1] == "integer"

def test__bridge_stall_foods_table_has_food_id_column(db):
    """Tests if bridge stall foods table has food id column"""
    test_query = "SELECT column_name, data_type \
                  FROM information_schema.columns \
                  WHERE table_name = 'bridge_stall_foods' \
                  AND column_name = 'food_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "food_id"
    assert  expected[0][1] == "integer"

def test_bridge_shop_items_table_exists(db):
    """test if bridge shop items table exists"""
    test_query = "SELECT EXISTS (SELECT FROM information_schema.tables \
                  WHERE table_name = 'bridge_shop_items');"
    expected = db.run(test_query)
    assert expected == [[True]]

def test_bridge_shop_items_table_has_shop_items_id_column_as_serial_primary_key(db):
    """Tests if bridge shop items table has shop items id as serial primary key"""
    test_query = "SELECT column_name, data_type, column_default \
                  FROM information_schema.columns \
                  WHERE table_name = 'bridge_shop_items' \
                  AND column_name = 'shop_items_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "shop_items_id"
    assert  expected[0][1] == "integer"
    assert  expected[0][2] == "nextval('bridge_shop_items_shop_items_id_seq'::regclass)" 

def test__bridge_shop_items_table_has_stall_id_column(db):
    """Tests if bridge shop items table has shop id column"""
    test_query = "SELECT column_name, data_type \
                  FROM information_schema.columns \
                  WHERE table_name = 'bridge_shop_items' \
                  AND column_name = 'shop_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "shop_id"
    assert  expected[0][1] == "integer"

def test__bridge_shop_items_table_has_food_id_column(db):
    """Tests if bridge shop items table has item id column"""
    test_query = "SELECT column_name, data_type \
                  FROM information_schema.columns \
                  WHERE table_name = 'bridge_shop_items' \
                  AND column_name = 'item_id';"
    expected = db.run(test_query)
    assert  expected[0][0] == "item_id"
    assert  expected[0][1] == "integer"

def test_bridge_shop_items_table_content(db):
    """Tests if bridge shop items table has the right content"""
    result = [
        [1, 1, 1],
        [2, 2, 1],
        [3, 5, 1],
        [4, 7, 1],
        [5, 10, 1],
        [6, 12, 1],
        [7, 15, 1],
        [8, 17, 1],
        [9, 19, 1],
        [10, 3, 3],
        [11, 3, 4],
        [12, 4, 3],
        [13, 4, 4],
        [14, 6, 3],
        [15, 6, 4],
        [16, 9, 3],
        [17, 9, 4],
        [18, 14, 3],
        [19, 14, 4],
        [20, 18, 3],
        [21, 18, 4],
        [22, 21, 3],
        [23, 21, 4],
        [24, 8, 2],
        [25, 8, 3],
        [26, 11, 2],
        [27, 11, 3],
        [28, 13, 2],
        [29, 13, 3],
        [30, 16, 2],
        [31, 16, 3],
        [32, 20, 2],
        [33, 20, 3]
        ]
    test_query = "SELECT * FROM bridge_shop_items;"
    expected = db.run(test_query)
    assert  expected == result

def test_shops_table_content(db):
    """Tests if bridge shops table has the right content"""
    result = [
        [1, 'Balloon Stall', 1],
        [3, 'Information Kiosk', 2],
        [2, 'Balloon Stall', 2],
        [4, 'Information Kiosk', 3],
        [6, 'Information Kiosk', 4],
        [5, 'Balloon Stall', 4],
        [9, 'Information Kiosk', 5],
        [8, 'Souvenir Stall', 5],
        [7, 'Balloon Stall', 5],
        [11, 'Souvenir Stall', 6],
        [10, 'Balloon Stall', 6],
        [14, 'Information Kiosk', 7],
        [13, 'Souvenir Stall', 7],
        [12, 'Balloon Stall', 7],
        [16, 'Souvenir Stall', 8],
        [15, 'Balloon Stall', 8],
        [18, 'Information Kiosk', 9],
        [17, 'Balloon Stall', 9],
        [21, 'Information Kiosk', 10],
        [20, 'Souvenir Stall', 10],
        [19, 'Balloon Stall', 10]
        ] 
    test_query = "SELECT * FROM shops;"
    expected = db.run(test_query)
    assert  expected == result

def test_items_table_content(db):
    """Tests if bridge items table has the right content"""
    result = [
        [1, 'Balloons', True],
        [2, 'Cuddly Toys', False],
        [3, 'Umbrellas', True],
        [4, 'Park Maps', False]
        ]
    test_query = "SELECT * FROM items;"
    expected = db.run(test_query)
    assert  expected == result

def test_bridge_stall_foods_table_content(db):
    """Tests if bridge stall foods table has the right content"""
    result = [
        [1, 1, 1],
        [2, 5, 1],
        [3, 9, 1],
        [4, 18, 1],
        [5, 25, 1],
        [6, 32, 1],
        [7, 40, 1],
        [8, 2, 4],
        [9, 7, 4],
        [10, 15, 4],
        [11, 21, 4],
        [12, 29, 4],
        [13, 37, 4],
        [14, 3, 5],
        [15, 8, 5],
        [16, 16, 5],
        [17, 22, 5],
        [18, 27, 5],
        [19, 30, 5],
        [20, 33, 5],
        [21, 38, 5],
        [22, 42, 5],
        [23, 4, 6],
        [24, 11, 6],
        [25, 17, 6],
        [26, 23, 6],
        [27, 31, 6],
        [28, 34, 6],
        [29, 43, 6],
        [30, 6, 3],
        [31, 14, 3],
        [32, 20, 3],
        [33, 26, 3],
        [34, 36, 3],
        [35, 10, 2],
        [36, 13, 2],
        [37, 19, 2],
        [38, 28, 2],
        [39, 41, 2],
        [40, 12, 7],
        [41, 24, 7],
        [42, 35, 7],
        [43, 39, 7],
        [44, 44, 7]
        ] 
    test_query = "SELECT * FROM bridge_stall_foods;"
    expected = db.run(test_query)
    assert  expected == result

def test_stalls_table_content(db):
    """Tests if bridge stalls table has the right content"""
    result = [
        [4, 'Candyfloss Stall', 1],
        [3, 'Drinks Stall', 1],
        [2, 'Burger Bar', 1],
        [1, 'Fruity Ices Stall', 1],
        [8, 'Drinks Stall', 2],
        [7, 'Burger Bar', 2],
        [6, 'Pizza Stall', 2],
        [5, 'Fruity Ices Stall', 2],
        [12, 'Popcorn Stall', 3],
        [11, 'Candyfloss Stall', 3],
        [10, 'Chip Shop', 3],
        [9, 'Fruity Ices Stall', 3],
        [17, 'Candyfloss Stall', 4],
        [16, 'Drinks Stall', 4],
        [15, 'Burger Bar', 4],
        [14, 'Pizza Stall', 4],
        [13, 'Chip Shop', 4],
        [24, 'Popcorn Stall', 5],
        [23, 'Candyfloss Stall', 5],
        [22, 'Drinks Stall', 5],
        [21, 'Burger Bar', 5],
        [20, 'Pizza Stall', 5],
        [19, 'Chip Shop', 5],
        [18, 'Fruity Ices Stall', 5],
        [27, 'Drinks Stall', 6],
        [26, 'Pizza Stall', 6],
        [25, 'Fruity Ices Stall', 6],
        [31, 'Candyfloss Stall', 7],
        [30, 'Drinks Stall', 7],
        [29, 'Burger Bar', 7],
        [28, 'Chip Shop', 7],
        [35, 'Popcorn Stall', 8],
        [34, 'Candyfloss Stall', 8],
        [33, 'Drinks Stall', 8],
        [32, 'Fruity Ices Stall', 8],
        [39, 'Popcorn Stall', 9],
        [38, 'Drinks Stall', 9],
        [37, 'Burger Bar', 9],
        [36, 'Pizza Stall', 9],
        [44, 'Popcorn Stall', 10],
        [43, 'Candyfloss Stall', 10],
        [42, 'Drinks Stall', 10],
        [41, 'Chip Shop', 10],
        [40, 'Fruity Ices Stall', 10]
        ] 
    test_query = "SELECT * FROM stalls;"
    expected = db.run(test_query)
    assert  expected == result

def test_foods_table_content(db):
    """Tests if bridge foods table has the right content"""
    result = [
        [1, 'Ice Cream', True],
        [2, 'Chips', False],
        [3, 'Pizza', False],
        [4, 'Burgers', True],
        [5, 'Drinks', True],
        [6, 'Candyfloss', False],
        [7, 'Popcorn', True]
        ] 
    test_query = "SELECT * FROM foods;"
    expected = db.run(test_query)
    assert  expected == result

def test_parks_table_content(db):
    """Tests if parks table has the right content"""
    result = [
        [1, 'Forest Frontiers', 1999, 250],
        [2, 'Dynamite Dunes', 2000, 650],
        [3, 'Leafy Lake', 2003, 500],
        [4, 'Diamond Heights', 2006, 700],
        [5, 'Evergreen Gardens', 2009, 1000],
        [6, 'Bumbly Beach', 2013, 750],
        [7, 'Trinity Islands', 2015, 750],
        [8, "Katie's Dreamland", 2018, 800],
        [9, 'Pokey Park', 2021, 400],
        [10, 'White Water Park', 2023, 900]
        ]
    test_query = "SELECT * FROM parks;"
    expected = db.run(test_query)
    assert  expected == result

def test_other_fac_table_content(db):
    """Tests if other_fac table has the right content"""
    result = [
        [1, 'Woodland Toilets', 1],
        [2, 'Sandy Toilets', 2],
        [3, 'Naval Toilets', 3],
        [4, 'Sky Toilets', 4],
        [5, 'Lush Toilets', 5],
        [6, 'Rocky Toilets', 6],
        [7, 'Sleepy Toilets', 8],
        [9, 'Innapropriate Toilets', 9],
        [8, 'Rapid Toilets', 10]
        ]
    test_query = "SELECT * FROM other_fac;"
    expected = db.run(test_query)
    assert  expected == result

def test_rides_table_content(db):
    """Tests if rides table has the right content"""
    result = [
        [41, 'Pirate Ship', 'Thrill Rides', 1999, 857, 1],
        [19, 'Wooden Wild Mouse', 'Roller Coasters', 2000, 1396, 1],
        [17, 'Wooden Roller Coaster', 'Roller Coasters', 1999, 2059, 1],
        [8, 'Merry-Go-Round', 'Gentle Rides', 1999, 106, 1],
        [6, 'Ferris Wheel', 'Gentle Rides', 1999, 840, 1],
        [39, 'Twist', 'Thrill Rides', 2000, 48, 2],
        [20, 'Wooden Wild Mine Ride', 'Roller Coasters', 2000, 1294, 2],
        [18, 'Reversed Wooden Roller Coaster', 'Roller Coasters', 2001, 1947, 2],
        [5, 'Haunted House', 'Gentle Rides', 2000, 84, 2],
        [1, 'Steam Trains', 'Transport Rides', 2005, 104, 2],
        [47, 'Dinghy Slide', 'Water Rides', 2003, 583, 3],
        [40, 'Launched Freefall', 'Thrill Rides', 2004, 375, 3],
        [24, 'Spinning Cars', 'Roller Coasters', 2003, 1058, 3],
        [21, 'Ladybird Trains', 'Roller Coasters', 2003, 857, 3],
        [9, 'Observation Tower', 'Gentle Rides', 2003, 720, 3],
        [48, 'Log Flume', 'Water Rides', 2007, 739, 4],
        [46, 'Top Spin', 'Thrill Rides', 2006, 239, 4],
        [30, 'Suspended Swinging Cars', 'Roller Coasters', 2013, 2056, 4],
        [25, 'Mine Train Coaster', 'Roller Coasters', 2015, 1302, 4],
        [23, 'Rocket Cars', 'Roller Coasters', 2017, 1503, 4],
        [11, 'Racing Cars', 'Gentle Rides', 2006, 20, 4],
        [50, 'Rowing Boats', 'Water Rides', 2009, 392, 5],
        [42, 'Go Karts', 'Thrill Rides', 2011, 127, 5],
        [34, 'Steeplechase', 'Roller Coasters', 2017, 927, 5],
        [28, 'Stand-up Roller Coaster', 'Roller Coasters', 2015, 503, 5],
        [16, 'Space Rings', 'Gentle Rides', 2010, 1068, 5],
        [7, 'Hedge Maze', 'Gentle Rides', 2009, 295, 5],
        [3, 'Large Monorail Train', 'Transport Rides', 2009, 194, 5],
        [51, 'Swans', 'Water Rides', 2013, 295, 6],
        [44, 'Motion Simulator', 'Thrill Rides', 2016, 285, 6],
        [35, 'Motorbike Races', 'Roller Coasters', 2014, 1720, 6],
        [29, 'Corkscrew Roller Coaster', 'Roller Coasters', 2013, 1734, 6],
        [10, 'Sportscars', 'Gentle Rides', 2013, 1058, 6],
        [52, 'Canoes', 'Water Rides', 2015, 21, 7],
        [37, 'Reverse Freefall Coaster', 'Roller Coasters', 2022, 2312, 7],
        [33, 'Mini Suspended Flying Coaster', 'Roller Coasters', 2020, 1003, 7],
        [32, 'Mini Suspended Coaster', 'Roller Coasters', 2015, 1845, 7],
        [26, 'Looping Roller Coaster', 'Roller Coasters', 2015, 1956, 7],
        [12, 'Pickup Trucks', 'Gentle Rides', 2015, 60, 7],
        [2, 'Small Monorail Cars', 'Transport Rides', 2016, 749, 7],
        [53, 'Bumper Boats', 'Water Rides', 2018, 520, 8],
        [45, '3D Cinema', 'Thrill Rides', 2019, 539, 8],
        [31, 'Suspended Swinging Airplane Cars', 'Roller Coasters', 2016, 2104, 8],
        [13, 'Cheshire Cats', 'Gentle Rides', 2018, 306, 8],
        [54, 'Water Tricycles', 'Water Rides', 2023, 133, 9],
        [43, 'Swinging Inverter Ship', 'Thrill Rides', 2021, 386, 9],
        [38, 'Vertical Drop Roller Coaster', 'Roller Coasters', 2021, 1840, 9],
        [36, 'Bobsleigh Coaster', 'Roller Coasters', 2021, 1940, 9],
        [14, 'Spiral Slide', 'Gentle Rides', 2021, 481, 9],
        [49, 'River Rapids', 'Water Rides', 2024, 920, 10],
        [27, 'Backwards Roller Coaster Train', 'Roller Coasters', 2024, 2057, 10],
        [22, 'Log Trains', 'Roller Coasters', 2023, 920, 10],
        [15, 'Dodgems', 'Gentle Rides', 2023, 82, 10],
        [4, 'Chairlift Cars', 'Transport Rides', 2023, 438, 10]
        ] 
    test_query = "SELECT * FROM rides;"
    expected = db.run(test_query)
    assert  expected == result


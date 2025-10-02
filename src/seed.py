from src.connection import create_con, close_db
from src.utils.utils_func import modify_raw_rides_data

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
    db.run("DROP TABLE IF EXISTS bridge_stall_foods")
    db.run("DROP TABLE IF EXISTS bridge_shop_items")    
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
    create_stalls(db)
    create_foods(db)
    create_shops(db)
    create_items(db)

    insert_parks_data(db)
    insert_rides_data(db)
    insert_other_fac_data(db)
    insert_stalls_data(db)
    insert_foods_data(db)
    insert_shops_data(db)
    insert_items_data(db)

    create_bridge_stall_foods(db)
    create_bridge_shop_items(db)
    insert_bridge_stall_foods_data(db)
    insert_bridge_shop_items_data(db)

    alter_table_drop_column(db, table_name="stalls", column_name="foods_served")
    alter_table_drop_column(db, table_name="shops", column_name="items_sold")

    alter_table_add_column(db, table_name="rides", column_name="park_id", column_type="INT")
    populate_foreign_key(db, source_table="parks", target_table="rides", fk_column="park_id", match_column="park_name")
    alter_table_set_fk(db, table_name="rides", constarints_name="fk_rides_name", column_name="park_id", reference_table="parks")
    alter_table_drop_column(db, table_name="rides", column_name="park_name")

    alter_table_add_column(db, table_name="other_fac", column_name="park_id", column_type="INT")
    populate_foreign_key(db, source_table="parks", target_table="other_fac", fk_column="park_id", match_column="park_name")
    alter_table_set_fk(db, table_name="other_fac", constarints_name="fk_other_fac_name", column_name="park_id", reference_table="parks")
    alter_table_drop_column(db, table_name="other_fac", column_name="park_name")

    alter_table_add_column(db, table_name="stalls", column_name="park_id", column_type="INT")
    populate_foreign_key(db, source_table="parks", target_table="stalls", fk_column="park_id", match_column="park_name")
    alter_table_set_fk(db, table_name="stalls", constarints_name="fk_stalls_name", column_name="park_id", reference_table="parks")
    alter_table_drop_column(db, table_name="stalls", column_name="park_name")

    alter_table_add_column(db, table_name="shops", column_name="park_id", column_type="INT")
    populate_foreign_key(db, source_table="parks", target_table="shops", fk_column="park_id", match_column="park_name")
    alter_table_set_fk(db, table_name="shops", constarints_name="fk_shops_name", column_name="park_id", reference_table="parks")
    alter_table_drop_column(db, table_name="shops", column_name="park_name")

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
    (ride_name, ride_type, year_opened, votes, park_name)
    VALUES
    (:ride_name, :ride_type, :year_opened,:votes, :park_name)
    """
    for ride in rides:
        ride_name = ride["ride_name"]
        ride_type = ride["ride_type"]
        year_opened = ride["year_opened"]
        votes = ride["votes"]
        park_name = ride["park_name"]
        db.run(
            insert_query,
            ride_name=ride_name,
            ride_type=ride_type,
            year_opened=year_opened,
            votes=votes,
            park_name=park_name
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

def create_stalls(db):
    return db.run(
        """
            CREATE TABLE stalls 
                (
                stall_id SERIAL PRIMARY KEY,
                stall_name VARCHAR(127) NOT NULL,
                park_name VARCHAR(127) NOT NULL,
                foods_served TEXT[]
                )
            """
        )

def insert_stalls_data(db):
    insert_query = """
    INSERT INTO stalls
    (stall_name, park_name, foods_served)
    VALUES
    (:stall_name, :park_name, :foods_served )
    """
    for stall in stalls:
        stall_name = stall["stall_name"]
        park_name = stall["park_name"]
        foods_served = stall["foods_served"]
        db.run(
            insert_query,
            stall_name=stall_name,
            park_name=park_name,
            foods_served=foods_served
        )

def create_foods(db):
    return db.run(
        """
            CREATE TABLE foods 
                (
                food_id SERIAL PRIMARY KEY,
                food_name VARCHAR(127) NOT NULL,
                vegan_option BOOL NOT NULL
                )
            """
        )

def insert_foods_data(db):
    insert_query = """
    INSERT INTO foods
    (food_name, vegan_option)
    VALUES
    (:food_name, :vegan_option)
    """
    for food in foods:
        food_name = food["food_name"]
        vegan_option = food["vegan_option"]
        db.run(
            insert_query,
            food_name=food_name,
            vegan_option=vegan_option
        )

def create_shops(db):
    return db.run(
        """
            CREATE TABLE shops 
                (
                shop_id SERIAL PRIMARY KEY,
                shop_name VARCHAR(127) NOT NULL,
                park_name VARCHAR(127) NOT NULL,
                items_sold TEXT[]
                )
            """
        )

def insert_shops_data(db):
    insert_query = """
    INSERT INTO shops
    (shop_name, park_name, items_sold)
    VALUES
    (:shop_name, :park_name, :items_sold)
    """
    for shop in shops:
        shop_name = shop["shop_name"]
        park_name = shop["park_name"]
        items_sold = shop["items_sold"]
        db.run(
            insert_query,
            shop_name=shop_name,
            park_name=park_name,
            items_sold=items_sold
        )

def create_items(db):
    return db.run(
        """
            CREATE TABLE items 
                (
                item_id SERIAL PRIMARY KEY,
                item_name VARCHAR(127) NOT NULL,
                multi_colour_option BOOL NOT NULL
                )
            """
        )

def insert_items_data(db):
    insert_query = """
    INSERT INTO items
    (item_name, multi_colour_option)
    VALUES
    (:item_name, :multi_colour_option)
    """
    for item in items:
        item_name = item["item_name"]
        multi_colour_option = item["multi_colour_option"]
        db.run(
            insert_query,
            item_name=item_name,
            multi_colour_option=multi_colour_option
        )

def alter_table_add_column(db, table_name, column_name, column_type):
    return db.run(
        f"""
            ALTER TABLE {table_name} 
                ADD COLUMN {column_name} {column_type} 
            """
        )

def populate_foreign_key(db, source_table, target_table, fk_column, match_column):
    return db.run(
        f"""
            UPDATE {target_table} t
                SET {fk_column} = s.{fk_column}
                FROM {source_table} s
                WHERE t.{match_column} = s.{match_column}
    """)


def alter_table_set_fk(db, table_name, constarints_name, column_name, reference_table):
    return db.run(
        f"""
            ALTER TABLE {table_name} 
                ADD CONSTRAINT {constarints_name}
                FOREIGN KEY ({column_name})
                REFERENCES {reference_table}({column_name})
            """
        )

def alter_table_drop_column(db, table_name, column_name):
    return db.run(
        f"""
            ALTER TABLE {table_name} 
                DROP COLUMN {column_name}
            """
        )

def create_bridge_stall_foods(db):
    return db.run(
        """
            CREATE TABLE bridge_stall_foods 
                (
                stall_foods_id SERIAL PRIMARY KEY,
                stall_id INT NOT NULL,
                food_id INT NOT NULL,
                FOREIGN KEY (stall_id) REFERENCES stalls(stall_id),
                FOREIGN KEY (food_id) REFERENCES foods(food_id)
                )
            """
        )

def insert_bridge_stall_foods_data(db):
    insert_query = """
        INSERT INTO bridge_stall_foods (stall_id, food_id)
        SELECT :stall_id, :food_id
        WHERE NOT EXISTS (
            SELECT 1 FROM bridge_stall_foods
            WHERE stall_id = :stall_id AND food_id = :food_id
        )
    """
    for stall in stalls:
        stall_rows = db.run(
            "SELECT stall_id FROM stalls WHERE stall_name = :stall_name",
            stall_name=stall["stall_name"]
        )
        for stall_row in stall_rows:
            stall_id = stall_row[0]
            for food_name in stall["foods_served"]:
                food_row = db.run(
                    "SELECT food_id FROM foods WHERE food_name = :food_name",
                    food_name=food_name
                )
                food_id = food_row[0][0]
                db.run(insert_query, stall_id=stall_id, food_id=food_id)

def create_bridge_shop_items(db):
    return db.run(
        """
            CREATE TABLE bridge_shop_items 
                (
                shop_items_id SERIAL PRIMARY KEY,
                shop_id INT NOT NULL,
                item_id INT NOT NULL,
                FOREIGN KEY (shop_id) REFERENCES shops(shop_id),
                FOREIGN KEY (item_id) REFERENCES items(item_id)
                )
            """
        )

def insert_bridge_shop_items_data(db):
    insert_query = """
        INSERT INTO bridge_shop_items (shop_id, item_id)
        SELECT :shop_id, :item_id
        WHERE NOT EXISTS (
            SELECT 1 FROM bridge_shop_items
            WHERE shop_id = :shop_id AND item_id = :item_id
        )
    """
    for shop in shops:
        shop_rows = db.run(
            "SELECT shop_id FROM shops WHERE shop_name = :shop_name",
            shop_name=shop["shop_name"]
        )
        for shop_row in shop_rows:
            shop_id = shop_row[0]
            for item_name in shop["items_sold"]:
                item_row = db.run(
                    "SELECT item_id FROM items WHERE item_name = :item_name",
                    item_name=item_name
                )
                item_id = item_row[0][0]
                db.run(insert_query, shop_id=shop_id, item_id=item_id)
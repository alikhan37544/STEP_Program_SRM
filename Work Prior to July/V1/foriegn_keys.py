import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name):
    """Create a connection to MySQL/MariaDB database."""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Database connection successful.")
    except Error as e:
        print(f"The error '{e}' occurred while connecting.")
    return connection

def check_db_structure(connection):
    """
    Check database structure to ensure tables and columns exist before adding foreign keys
    """
    cursor = connection.cursor()
    
    # List of tables and their required columns for foreign keys
    required_columns = {
        'users': ['user_id'],
        'memberships': ['user_id'],
        'reviews': ['user_id', 'movie_id'],
        'points_transactions': ['user_id'],
        'movies': ['movie_id'],
        'movie_casts': ['movie_id'],
        'screens': ['screen_id'],
        'shows': ['movie_id', 'screen_id', 'show_id'],
        'seats': ['seat_id', 'screen_id'],
        'show_seats': ['show_id', 'seat_id', 'show_seat_id'],
        'bookings': ['booking_id', 'user_id', 'show_id'],
        'tickets': ['booking_id', 'show_seat_id'],
        'food_items': ['food_item_id'],
        'food_item_sizes': ['food_item_id', 'size_id'],
        'food_orders': ['food_order_id', 'user_id'],
        'food_order_items': ['food_order_id', 'food_item_id', 'size_id'],
        'payment_gateways': ['gateway_id'],
        'payments': ['user_id', 'gateway_id', 'booking_id', 'food_order_id']
    }
    
    issues_found = False
    
    # Check if tables exist
    print("\nChecking database structure:")
    for table, columns in required_columns.items():
        cursor.execute(f"SHOW TABLES LIKE '{table}'")
        if not cursor.fetchone():
            print(f"⚠️  Missing table: {table}")
            issues_found = True
            continue
            
        # Check if columns exist in the table
        cursor.execute(f"DESCRIBE `{table}`")
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        for column in columns:
            if column not in existing_columns:
                print(f"⚠️  Missing column: {column} in table {table}")
                issues_found = True
    
    # Check primary keys
    for table in required_columns.keys():
        try:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '{table}' 
                AND CONSTRAINT_NAME = 'PRIMARY'
            """)
            if cursor.fetchone()[0] == 0:
                print(f"⚠️  Table {table} has no primary key defined")
                issues_found = True
        except Error as e:
            print(f"Error checking primary key for {table}: {e}")
    
    cursor.close()
    return not issues_found

def drop_foreign_keys(connection):
    """
    Drop existing foreign key constraints before re-adding them.
    This helps when the script needs to be re-run.
    """
    fk_drops = [
        # Names of constraints to drop, matching the ones we'll add
        "ALTER TABLE `memberships` DROP FOREIGN KEY IF EXISTS `fk_memberships_users`",
        "ALTER TABLE `reviews` DROP FOREIGN KEY IF EXISTS `fk_reviews_users`",
        "ALTER TABLE `reviews` DROP FOREIGN KEY IF EXISTS `fk_reviews_movies`",
        "ALTER TABLE `points_transactions` DROP FOREIGN KEY IF EXISTS `fk_points_transactions_users`",
        "ALTER TABLE `movie_casts` DROP FOREIGN KEY IF EXISTS `fk_movie_casts_movies`",
        "ALTER TABLE `shows` DROP FOREIGN KEY IF EXISTS `fk_shows_movies`",
        "ALTER TABLE `shows` DROP FOREIGN KEY IF EXISTS `fk_shows_screens`",
        "ALTER TABLE `seats` DROP FOREIGN KEY IF EXISTS `fk_seats_screens`",
        "ALTER TABLE `show_seats` DROP FOREIGN KEY IF EXISTS `fk_show_seats_shows`",
        "ALTER TABLE `show_seats` DROP FOREIGN KEY IF EXISTS `fk_show_seats_seats`",
        "ALTER TABLE `bookings` DROP FOREIGN KEY IF EXISTS `fk_bookings_users`",
        "ALTER TABLE `bookings` DROP FOREIGN KEY IF EXISTS `fk_bookings_shows`",
        "ALTER TABLE `tickets` DROP FOREIGN KEY IF EXISTS `fk_tickets_bookings`",
        "ALTER TABLE `tickets` DROP FOREIGN KEY IF EXISTS `fk_tickets_show_seats`",
        "ALTER TABLE `food_item_sizes` DROP FOREIGN KEY IF EXISTS `fk_food_item_sizes_food_items`",
        "ALTER TABLE `food_orders` DROP FOREIGN KEY IF EXISTS `fk_food_orders_users`",
        "ALTER TABLE `food_order_items` DROP FOREIGN KEY IF EXISTS `fk_food_order_items_food_orders`",
        "ALTER TABLE `food_order_items` DROP FOREIGN KEY IF EXISTS `fk_food_order_items_food_items`",
        "ALTER TABLE `food_order_items` DROP FOREIGN KEY IF EXISTS `fk_food_order_items_food_item_sizes`",
        "ALTER TABLE `payments` DROP FOREIGN KEY IF EXISTS `fk_payments_users`",
        "ALTER TABLE `payments` DROP FOREIGN KEY IF EXISTS `fk_payments_payment_gateways`",
        "ALTER TABLE `payments` DROP FOREIGN KEY IF EXISTS `fk_payments_bookings`",
        "ALTER TABLE `payments` DROP FOREIGN KEY IF EXISTS `fk_payments_food_orders`"
    ]
    
    cursor = connection.cursor()
    for statement in fk_drops:
        try:
            print(f"Executing: {statement}")
            cursor.execute(statement)
        except Error as e:
            # MySQL 8.0+ supports "DROP FOREIGN KEY IF EXISTS", but older versions don't
            if "Unknown table" not in str(e) and "check that it exists" not in str(e):
                print(f"Error executing: {statement}\nError: {e}\n")
    connection.commit()
    cursor.close()

def add_foreign_keys(connection):
    """
    Add foreign key constraints to the movie theater database tables.
    """
    fk_statements = [
        # Users related foreign keys
        """
        ALTER TABLE `memberships`
        ADD CONSTRAINT `fk_memberships_users`
        FOREIGN KEY (`user_id`)
        REFERENCES `users`(`user_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `reviews`
        ADD CONSTRAINT `fk_reviews_users`
        FOREIGN KEY (`user_id`)
        REFERENCES `users`(`user_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `points_transactions`
        ADD CONSTRAINT `fk_points_transactions_users`
        FOREIGN KEY (`user_id`)
        REFERENCES `users`(`user_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        # Movies related foreign keys
        """
        ALTER TABLE `reviews`
        ADD CONSTRAINT `fk_reviews_movies`
        FOREIGN KEY (`movie_id`)
        REFERENCES `movies`(`movie_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `movie_casts`
        ADD CONSTRAINT `fk_movie_casts_movies`
        FOREIGN KEY (`movie_id`)
        REFERENCES `movies`(`movie_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        # Shows related foreign keys
        """
        ALTER TABLE `shows`
        ADD CONSTRAINT `fk_shows_movies`
        FOREIGN KEY (`movie_id`)
        REFERENCES `movies`(`movie_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `shows`
        ADD CONSTRAINT `fk_shows_screens`
        FOREIGN KEY (`screen_id`)
        REFERENCES `screens`(`screen_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        # Seats related foreign keys
        """
        ALTER TABLE `seats`
        ADD CONSTRAINT `fk_seats_screens`
        FOREIGN KEY (`screen_id`)
        REFERENCES `screens`(`screen_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `show_seats`
        ADD CONSTRAINT `fk_show_seats_shows`
        FOREIGN KEY (`show_id`)
        REFERENCES `shows`(`show_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `show_seats`
        ADD CONSTRAINT `fk_show_seats_seats`
        FOREIGN KEY (`seat_id`)
        REFERENCES `seats`(`seat_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        # Bookings related foreign keys
        """
        ALTER TABLE `bookings`
        ADD CONSTRAINT `fk_bookings_users`
        FOREIGN KEY (`user_id`)
        REFERENCES `users`(`user_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `bookings`
        ADD CONSTRAINT `fk_bookings_shows`
        FOREIGN KEY (`show_id`)
        REFERENCES `shows`(`show_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        # Tickets related foreign keys
        """
        ALTER TABLE `tickets`
        ADD CONSTRAINT `fk_tickets_bookings`
        FOREIGN KEY (`booking_id`)
        REFERENCES `bookings`(`booking_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `tickets`
        ADD CONSTRAINT `fk_tickets_show_seats`
        FOREIGN KEY (`show_seat_id`)
        REFERENCES `show_seats`(`show_seat_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        # Food related foreign keys
        """
        ALTER TABLE `food_item_sizes`
        ADD CONSTRAINT `fk_food_item_sizes_food_items`
        FOREIGN KEY (`food_item_id`)
        REFERENCES `food_items`(`food_item_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `food_orders`
        ADD CONSTRAINT `fk_food_orders_users`
        FOREIGN KEY (`user_id`)
        REFERENCES `users`(`user_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `food_order_items`
        ADD CONSTRAINT `fk_food_order_items_food_orders`
        FOREIGN KEY (`food_order_id`)
        REFERENCES `food_orders`(`food_order_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `food_order_items`
        ADD CONSTRAINT `fk_food_order_items_food_items`
        FOREIGN KEY (`food_item_id`)
        REFERENCES `food_items`(`food_item_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `food_order_items`
        ADD CONSTRAINT `fk_food_order_items_food_item_sizes`
        FOREIGN KEY (`size_id`)
        REFERENCES `food_item_sizes`(`size_id`)
        ON UPDATE CASCADE
        ON DELETE SET NULL
        """,
        
        # Payment related foreign keys
        """
        ALTER TABLE `payments`
        ADD CONSTRAINT `fk_payments_users`
        FOREIGN KEY (`user_id`)
        REFERENCES `users`(`user_id`)
        ON UPDATE CASCADE
        ON DELETE CASCADE
        """,
        
        """
        ALTER TABLE `payments`
        ADD CONSTRAINT `fk_payments_payment_gateways`
        FOREIGN KEY (`gateway_id`)
        REFERENCES `payment_gateways`(`gateway_id`)
        ON UPDATE CASCADE
        ON DELETE SET NULL
        """,
        
        """
        ALTER TABLE `payments`
        ADD CONSTRAINT `fk_payments_bookings`
        FOREIGN KEY (`booking_id`)
        REFERENCES `bookings`(`booking_id`)
        ON UPDATE CASCADE
        ON DELETE SET NULL
        """,
        
        """
        ALTER TABLE `payments`
        ADD CONSTRAINT `fk_payments_food_orders`
        FOREIGN KEY (`food_order_id`)
        REFERENCES `food_orders`(`food_order_id`)
        ON UPDATE CASCADE
        ON DELETE SET NULL
        """
    ]

    cursor = connection.cursor()
    for statement in fk_statements:
        clean_statement = statement.strip()
        if not clean_statement:
            continue  # skip empty lines if any
        try:
            print(f"Executing:\n{clean_statement}\n")
            cursor.execute(clean_statement)
        except Error as e:
            print(f"Error executing:\n{clean_statement}\nError: {e}\n")
    connection.commit()
    cursor.close()

def verify_foreign_keys(connection):
    """
    Verify that the foreign keys were successfully added.
    """
    query = """
    SELECT tc.TABLE_NAME, tc.CONSTRAINT_NAME, rc.REFERENCED_TABLE_NAME
    FROM information_schema.TABLE_CONSTRAINTS tc
    JOIN information_schema.REFERENTIAL_CONSTRAINTS rc USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
    JOIN information_schema.KEY_COLUMN_USAGE kcu USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
    WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    AND tc.TABLE_SCHEMA = DATABASE()
    GROUP BY tc.TABLE_NAME, tc.CONSTRAINT_NAME, rc.REFERENCED_TABLE_NAME
    ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME
    """
    
    cursor = connection.cursor()
    print("\nVerifying foreign key constraints:")
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        for result in results:
            print(f"Table: {result[0]}, Constraint: {result[1]}, References: {result[2]}")
        print(f"\nTotal foreign keys found: {len(results)}")
    except Error as e:
        print(f"Error verifying foreign keys: {e}")
    cursor.close()

def main():
    # Adjust these credentials to your environment
    host = "localhost"
    user = "ali"
    password = "admin"
    database = "UNOX"

    # 1) Connect to the database
    connection = create_connection(host, user, password, database)
    if connection is None:
        return  # cannot proceed if connection failed

    # 2) Verify table structure before proceeding
    if not check_db_structure(connection):
        print("\n⛔ Database structure issues detected. Fix these before adding foreign keys.")
        connection.close()
        return

    # 3) Drop existing foreign keys to avoid errors
    drop_foreign_keys(connection)
    
    # 4) Add all foreign keys
    add_foreign_keys(connection)
    
    # 5) Verify the foreign keys were added correctly
    verify_foreign_keys(connection)

    # 6) Close the connection
    connection.close()

if __name__ == "__main__":
    main()

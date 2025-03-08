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

def add_missing_columns(connection):
    """Add missing columns required for foreign keys to work."""
    column_additions = [
        "ALTER TABLE `users` MODIFY COLUMN `user_id` INT NOT NULL AUTO_INCREMENT",
        "ALTER TABLE `reviews` ADD COLUMN IF NOT EXISTS `user_id` INT NOT NULL",
        "ALTER TABLE `reviews` ADD COLUMN IF NOT EXISTS `movie_id` INT NOT NULL",
        "ALTER TABLE `movies` MODIFY COLUMN `movie_id` INT NOT NULL AUTO_INCREMENT",
        "ALTER TABLE `food_items` MODIFY COLUMN `food_item_id` INT NOT NULL AUTO_INCREMENT",
        "ALTER TABLE `food_item_sizes` MODIFY COLUMN `size_id` INT NOT NULL AUTO_INCREMENT",
        "ALTER TABLE `food_item_sizes` ADD COLUMN IF NOT EXISTS `food_item_id` INT NOT NULL",
        "ALTER TABLE `food_orders` MODIFY COLUMN `food_order_id` INT NOT NULL AUTO_INCREMENT",
        "ALTER TABLE `food_orders` ADD COLUMN IF NOT EXISTS `user_id` INT NOT NULL",
        "ALTER TABLE `food_order_items` ADD COLUMN IF NOT EXISTS `food_order_id` INT NOT NULL",
        "ALTER TABLE `food_order_items` ADD COLUMN IF NOT EXISTS `food_item_id` INT NOT NULL",
        "ALTER TABLE `food_order_items` ADD COLUMN IF NOT EXISTS `size_id` INT NULL",
        "ALTER TABLE `payments` ADD COLUMN IF NOT EXISTS `user_id` INT NOT NULL",
        "ALTER TABLE `payments` ADD COLUMN IF NOT EXISTS `gateway_id` INT NULL",
        "ALTER TABLE `payments` ADD COLUMN IF NOT EXISTS `booking_id` INT NULL",
        "ALTER TABLE `payments` ADD COLUMN IF NOT EXISTS `food_order_id` INT NULL"
    ]
    
    cursor = connection.cursor()
    for statement in column_additions:
        try:
            print(f"Executing: {statement}")
            cursor.execute(statement)
        except Error as e:
            print(f"Error executing: {statement}\nError: {e}")
    connection.commit()
    cursor.close()

def add_primary_keys(connection):
    """Add primary keys to tables that lack them."""
    primary_keys = [
        "ALTER TABLE `users` ADD PRIMARY KEY (`user_id`)",
        "ALTER TABLE `memberships` ADD PRIMARY KEY (`user_id`)",
        "ALTER TABLE `reviews` ADD COLUMN IF NOT EXISTS `review_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "ALTER TABLE `points_transactions` ADD COLUMN IF NOT EXISTS `transaction_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "ALTER TABLE `movies` ADD PRIMARY KEY (`movie_id`)",
        "ALTER TABLE `movie_casts` ADD COLUMN IF NOT EXISTS `cast_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "ALTER TABLE `screens` ADD PRIMARY KEY (`screen_id`)",
        "ALTER TABLE `shows` ADD PRIMARY KEY (`show_id`)",
        "ALTER TABLE `seats` ADD PRIMARY KEY (`seat_id`)",
        "ALTER TABLE `show_seats` ADD PRIMARY KEY (`show_seat_id`)",
        "ALTER TABLE `bookings` ADD PRIMARY KEY (`booking_id`)",
        "ALTER TABLE `tickets` ADD COLUMN IF NOT EXISTS `ticket_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "ALTER TABLE `food_items` ADD PRIMARY KEY (`food_item_id`)",
        "ALTER TABLE `food_item_sizes` ADD PRIMARY KEY (`size_id`)",
        "ALTER TABLE `food_orders` ADD PRIMARY KEY (`food_order_id`)",
        "ALTER TABLE `food_order_items` ADD COLUMN IF NOT EXISTS `item_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY",
        "ALTER TABLE `payment_gateways` ADD PRIMARY KEY (`gateway_id`)",
        "ALTER TABLE `payments` ADD COLUMN IF NOT EXISTS `payment_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY"
    ]
    
    cursor = connection.cursor()
    for statement in primary_keys:
        try:
            print(f"Executing: {statement}")
            cursor.execute(statement)
        except Error as e:
            print(f"Error executing: {statement}\nError: {e}")
    connection.commit()
    cursor.close()

def add_indexes(connection):
    """Add necessary indexes for foreign key columns."""
    indexes = [
        "CREATE INDEX IF NOT EXISTS `idx_memberships_user` ON `memberships` (`user_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_reviews_user` ON `reviews` (`user_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_reviews_movie` ON `reviews` (`movie_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_points_user` ON `points_transactions` (`user_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_cast_movie` ON `movie_casts` (`movie_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_shows_movie` ON `shows` (`movie_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_shows_screen` ON `shows` (`screen_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_seats_screen` ON `seats` (`screen_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_show_seats_show` ON `show_seats` (`show_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_show_seats_seat` ON `show_seats` (`seat_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_bookings_user` ON `bookings` (`user_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_bookings_show` ON `bookings` (`show_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_tickets_booking` ON `tickets` (`booking_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_tickets_seat` ON `tickets` (`show_seat_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_food_sizes_item` ON `food_item_sizes` (`food_item_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_food_orders_user` ON `food_orders` (`user_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_food_order_items_order` ON `food_order_items` (`food_order_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_food_order_items_food` ON `food_order_items` (`food_item_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_food_order_items_size` ON `food_order_items` (`size_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_payments_user` ON `payments` (`user_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_payments_gateway` ON `payments` (`gateway_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_payments_booking` ON `payments` (`booking_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_payments_food_order` ON `payments` (`food_order_id`)"
    ]
    
    cursor = connection.cursor()
    for statement in indexes:
        try:
            print(f"Executing: {statement}")
            cursor.execute(statement)
        except Error as e:
            print(f"Error executing: {statement}\nError: {e}")
    connection.commit()
    cursor.close()

def check_db_structure(connection):
    """Check if the database structure is now correct for adding foreign keys."""
    # Using the same function from foreign_keys.py
    cursor = connection.cursor()
    
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
    
    print("\nVerifying database structure after repairs:")
    
    # Check columns and primary keys
    for table, columns in required_columns.items():
        cursor.execute(f"SHOW TABLES LIKE '{table}'")
        if not cursor.fetchone():
            print(f"⚠️  Table {table} still missing")
            issues_found = True
            continue
        
        # Check columns
        cursor.execute(f"DESCRIBE `{table}`")
        existing_columns = [row[0] for row in cursor.fetchall()]
        
        for column in columns:
            if column not in existing_columns:
                print(f"⚠️  Column {column} still missing in table {table}")
                issues_found = True
        
        # Check primary key
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = '{table}' 
            AND CONSTRAINT_NAME = 'PRIMARY'
        """)
        if cursor.fetchone()[0] == 0:
            print(f"⚠️  Table {table} still has no primary key defined")
            issues_found = True
    
    cursor.close()
    
    if issues_found:
        print("\n⚠️ Some issues still remain. Please review the errors above.")
        return False
    else:
        print("\n✅ Database structure looks good! You can now run foreign_keys.py")
        return True

def main():
    # Adjust these credentials to your environment
    host = "localhost"
    user = "ali"
    password = "admin"
    database = "UNOX"

    # Connect to the database
    connection = create_connection(host, user, password, database)
    if connection is None:
        return
    
    print("\n=== Step 1: Adding missing columns ===")
    add_missing_columns(connection)
    
    print("\n=== Step 2: Adding primary keys ===")
    add_primary_keys(connection)
    
    print("\n=== Step 3: Adding necessary indexes ===")
    add_indexes(connection)
    
    print("\n=== Step 4: Verifying database structure ===")
    check_db_structure(connection)
    
    connection.close()
    print("\nDatabase repair completed. Please run foreign_keys.py to add foreign key constraints.")

if __name__ == "__main__":
    main()

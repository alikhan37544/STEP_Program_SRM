import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name=None):
    """Create a connection to MySQL database"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print(f"MySQL{'/' + db_name if db_name else ''} connection successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def create_database(connection, db_name):
    """Create a database"""
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

def execute_query(connection, query):
    """Execute a query"""
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
        return True
    except Error as e:
        print(f"The error '{e}' occurred")
        return False
    finally:
        cursor.close()

def create_tables(connection):
    """Create all tables"""
    
    # 1. Screen Table
    create_screen_table = """
    CREATE TABLE IF NOT EXISTS Screen (
        screen_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        class_type VARCHAR(10) NOT NULL,
        capacity INT NOT NULL
    ) ENGINE=InnoDB;
    """
    
    # 2. Seat Table
    create_seat_table = """
    CREATE TABLE IF NOT EXISTS Seat (
        seat_id INT AUTO_INCREMENT PRIMARY KEY,
        screen_id INT NOT NULL,
        seat_number VARCHAR(10) NOT NULL,
        CONSTRAINT fk_seat_screen FOREIGN KEY (screen_id) 
        REFERENCES Screen(screen_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 3. Movie Table
    create_movie_table = """
    CREATE TABLE IF NOT EXISTS Movie (
        movie_id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        genre VARCHAR(50) NOT NULL,
        rating DECIMAL(3,1) NOT NULL,
        status VARCHAR(20) NOT NULL,
        poster_image_url VARCHAR(255) NULL
    ) ENGINE=InnoDB;
    """
    
    # 4. MovieCast Table
    create_moviecast_table = """
    CREATE TABLE IF NOT EXISTS MovieCast (
        cast_id INT AUTO_INCREMENT PRIMARY KEY,
        movie_id INT NOT NULL,
        person_name VARCHAR(100) NOT NULL,
        role VARCHAR(100) NOT NULL,
        CONSTRAINT fk_moviecast_movie FOREIGN KEY (movie_id) 
        REFERENCES Movie(movie_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 5. Review Table
    create_review_table = """
    CREATE TABLE IF NOT EXISTS Review (
        review_id INT AUTO_INCREMENT PRIMARY KEY,
        movie_id INT NOT NULL,
        content TEXT NOT NULL,
        review_date DATETIME NOT NULL,
        reviewer_name VARCHAR(100) NOT NULL,
        CONSTRAINT fk_review_movie FOREIGN KEY (movie_id) 
        REFERENCES Movie(movie_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 6. Show Table
    create_show_table = """
    CREATE TABLE IF NOT EXISTS `Show` (
        show_id INT AUTO_INCREMENT PRIMARY KEY,
        screen_id INT NOT NULL,
        movie_id INT NOT NULL,
        show_datetime DATETIME NOT NULL,
        CONSTRAINT fk_show_screen FOREIGN KEY (screen_id) 
        REFERENCES Screen(screen_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_show_movie FOREIGN KEY (movie_id) 
        REFERENCES Movie(movie_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 7. ShowSeat Table
    create_showseat_table = """
    CREATE TABLE IF NOT EXISTS ShowSeat (
        show_seat_id INT AUTO_INCREMENT PRIMARY KEY,
        show_id INT NOT NULL,
        seat_id INT NOT NULL,
        is_available BOOLEAN NOT NULL DEFAULT TRUE,
        CONSTRAINT fk_showseat_show FOREIGN KEY (show_id) 
        REFERENCES `Show`(show_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_showseat_seat FOREIGN KEY (seat_id) 
        REFERENCES Seat(seat_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 8. User Table
    create_user_table = """
    CREATE TABLE IF NOT EXISTS User (
        user_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(150) NOT NULL,
        phone VARCHAR(15) NULL
    ) ENGINE=InnoDB;
    """
    
    # 9. Membership Table
    create_membership_table = """
    CREATE TABLE IF NOT EXISTS Membership (
        membership_id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        current_points INT NOT NULL DEFAULT 0,
        CONSTRAINT fk_membership_user FOREIGN KEY (user_id) 
        REFERENCES User(user_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 10. Booking Table
    create_booking_table = """
    CREATE TABLE IF NOT EXISTS Booking (
        booking_id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        show_id INT NOT NULL,
        booking_datetime DATETIME NOT NULL,
        total_cost DECIMAL(10,2) NOT NULL,
        CONSTRAINT fk_booking_user FOREIGN KEY (user_id) 
        REFERENCES User(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_booking_show FOREIGN KEY (show_id) 
        REFERENCES `Show`(show_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 11. Ticket Table
    create_ticket_table = """
    CREATE TABLE IF NOT EXISTS Ticket (
        ticket_id INT AUTO_INCREMENT PRIMARY KEY,
        booking_id INT NOT NULL,
        show_seat_id INT NOT NULL,
        qr_code VARCHAR(100) NOT NULL,
        delivery_method VARCHAR(50) NOT NULL,
        is_downloaded BOOLEAN NOT NULL DEFAULT FALSE,
        scanned_at DATETIME NULL,
        CONSTRAINT fk_ticket_booking FOREIGN KEY (booking_id) 
        REFERENCES Booking(booking_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_ticket_showseat FOREIGN KEY (show_seat_id) 
        REFERENCES ShowSeat(show_seat_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 12. PaymentGateway Table
    create_paymentgateway_table = """
    CREATE TABLE IF NOT EXISTS PaymentGateway (
        gateway_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    ) ENGINE=InnoDB;
    """
    
    # 13. Payment Table
    create_payment_table = """
    CREATE TABLE IF NOT EXISTS Payment (
        payment_id INT AUTO_INCREMENT PRIMARY KEY,
        booking_id INT NOT NULL,
        gateway_id INT NOT NULL,
        transaction_amount DECIMAL(10,2) NOT NULL,
        transaction_datetime DATETIME NOT NULL,
        status VARCHAR(20) NOT NULL,
        failure_reason TEXT NULL,
        credit_card_name VARCHAR(100) NULL,
        credit_card_number VARCHAR(20) NULL,
        expiry_date DATE NULL,
        cvv VARCHAR(4) NULL,
        CONSTRAINT fk_payment_booking FOREIGN KEY (booking_id) 
        REFERENCES Booking(booking_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_payment_gateway FOREIGN KEY (gateway_id) 
        REFERENCES PaymentGateway(gateway_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 14. FoodItem Table
    create_fooditem_table = """
    CREATE TABLE IF NOT EXISTS FoodItem (
        item_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description TEXT NULL,
        is_combo BOOLEAN NOT NULL DEFAULT FALSE
    ) ENGINE=InnoDB;
    """
    
    # 15. FoodItemSize Table
    create_fooditemsize_table = """
    CREATE TABLE IF NOT EXISTS FoodItemSize (
        size_id INT AUTO_INCREMENT PRIMARY KEY,
        item_id INT NOT NULL,
        size_name VARCHAR(50) NOT NULL,
        rate DECIMAL(10,2) NOT NULL,
        CONSTRAINT fk_fooditemsize_fooditem FOREIGN KEY (item_id) 
        REFERENCES FoodItem(item_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 16. FoodOrder Table
    create_foodorder_table = """
    CREATE TABLE IF NOT EXISTS FoodOrder (
        order_id INT AUTO_INCREMENT PRIMARY KEY,
        booking_id INT NOT NULL,
        screen_id INT NOT NULL,
        seat_id INT NOT NULL,
        order_datetime DATETIME NOT NULL,
        total_cost DECIMAL(10,2) NOT NULL,
        delivery_method VARCHAR(50) NOT NULL,
        CONSTRAINT fk_foodorder_booking FOREIGN KEY (booking_id) 
        REFERENCES Booking(booking_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_foodorder_screen FOREIGN KEY (screen_id) 
        REFERENCES Screen(screen_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_foodorder_seat FOREIGN KEY (seat_id) 
        REFERENCES Seat(seat_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 17. FoodOrderItem Table
    create_foodorderitem_table = """
    CREATE TABLE IF NOT EXISTS FoodOrderItem (
        order_item_id INT AUTO_INCREMENT PRIMARY KEY,
        order_id INT NOT NULL,
        item_id INT NOT NULL,
        size_id INT NOT NULL,
        quantity INT NOT NULL,
        price_at_time DECIMAL(10,2) NOT NULL,
        CONSTRAINT fk_foodorderitem_foodorder FOREIGN KEY (order_id) 
        REFERENCES FoodOrder(order_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_foodorderitem_fooditem FOREIGN KEY (item_id) 
        REFERENCES FoodItem(item_id) ON DELETE CASCADE ON UPDATE CASCADE,
        CONSTRAINT fk_foodorderitem_fooditemsize FOREIGN KEY (size_id) 
        REFERENCES FoodItemSize(size_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # 18. PointsTransaction Table
    create_pointstransaction_table = """
    CREATE TABLE IF NOT EXISTS PointsTransaction (
        transaction_id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        points_earned INT NOT NULL,
        transaction_datetime DATETIME NOT NULL,
        transaction_type VARCHAR(20) NOT NULL,
        CONSTRAINT fk_pointstransaction_user FOREIGN KEY (user_id) 
        REFERENCES User(user_id) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB;
    """
    
    # Execute all table creation queries
    tables = [
        create_screen_table, create_movie_table, create_user_table, create_paymentgateway_table, create_fooditem_table,  # Tables without FK dependencies
        create_seat_table, create_moviecast_table, create_review_table, create_membership_table,  # Tables with one FK dependency
        create_show_table,  # Tables with two FK dependencies
        create_booking_table, create_showseat_table, create_fooditemsize_table,  # More tables with FK dependencies
        create_ticket_table, create_payment_table, create_foodorder_table,  # More complex dependencies
        create_foodorderitem_table, create_pointstransaction_table  # Final tables
    ]
    
    for i, table_query in enumerate(tables):
        if execute_query(connection, table_query):
            print(f"Table {i+1} created successfully")
        else:
            print(f"Failed to create table {i+1}")

def main():
    # Define your database credentials
    host = "localhost"
    user = "ali"
    password = "admin"
    db_name = "SRM_STEP"
    
    # Connect to MySQL server (without database selected)
    connection = create_connection(host, user, password)
    if connection is None:
        return
    
    # Create the database
    create_database(connection, db_name)
    connection.close()
    
    # Connect to the newly created database
    connection = create_connection(host, user, password, db_name)
    if connection is None:
        return
    
    # Create all tables with relationships
    create_tables(connection)
    
    # Close the connection
    connection.close()
    print("Database setup completed successfully!")

if __name__ == "__main__":
    main()
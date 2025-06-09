import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

def main():
    """
    Main function to connect to the database, fetch accounts,
    and insert a new transaction.
    """
    # Load environment variables from .env file
    load_dotenv()

    connection = None  # Initialize connection to None
    try:
        # Establish the database connection
        # Get the password from an environment variable
        db_password = os.getenv('DB_PASSWORD')
        if not db_password:
            raise ValueError("DB_PASSWORD environment variable not set.")

        connection = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password=db_password,
            database='fund_playground_db'
        )

        if connection.is_connected():
            print("Successfully connected to the database!")

            # --- 1. Fetch and Print All Records from the accounts table ---
            # Using dictionary=True makes fetchall() return a list of dictionaries, which is very readable
            cursor = connection.cursor(dictionary=True)

            print("\n--- Fetching all active accounts ---")
            cursor.execute("SELECT account_id, account_holder, balance FROM accounts WHERE is_deleted = FALSE")
            accounts = cursor.fetchall()

            if accounts:
                for account in accounts:
                    print(f"  Account ID: {account['account_id']}, Holder: {account['account_holder']}, Balance: {account['balance']:.2f}")
            else:
                print("  No active accounts found.")


            # --- 2. Insert a new test transaction ---
            print("\n--- Inserting a new test transaction ---")
            # Using %s placeholders is crucial to prevent SQL injection
            sql_insert_query = """
                INSERT INTO transactions
                (to_account_id, transaction_type, amount, description)
                VALUES (%s, %s, %s, %s)
            """
            # This is a deposit to account ID 1 (e.g., Alice) from an external source
            insert_tuple = (1, 'DEPOSIT', 77.77, 'Test deposit from Python')

            cursor.execute(sql_insert_query, insert_tuple)

            # To make the changes permanent, we must commit the transaction
            connection.commit()
            print(f"  Successfully inserted transaction with ID: {cursor.lastrowid}")

            cursor.close()

    except Error as e:
        print(f"Error while connecting to MySQL or performing operations: {e}")

    finally:
        # Ensure the connection is closed when we're done
        if connection and connection.is_connected():
            connection.close()
            print("\nMySQL connection is closed.")

if __name__ == '__main__':
    main()

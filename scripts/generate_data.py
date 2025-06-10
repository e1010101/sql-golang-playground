import mysql.connector
from mysql.connector import Error
import random
from faker import Faker
from decimal import Decimal, ROUND_UP
import os
from dotenv import load_dotenv

# Initialize the Faker library to generate fake data
fake = Faker()

def generate_random_transactions(num_transactions=100):
    load_dotenv()
    
    """
    Connects to the database and generates a specified number of
    random deposit transactions for existing active accounts.
    """
    connection = None
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
        cursor = connection.cursor()

        # --- 1. First, get all active account IDs to deposit into ---
        cursor.execute("SELECT account_id FROM accounts WHERE is_deleted = FALSE")
        account_ids = [item[0] for item in cursor.fetchall()]

        if not account_ids:
            print("No active accounts found. Cannot generate transactions.")
            return

        print(f"Found {len(account_ids)} active accounts to generate deposits for.")
        print(f"Generating {num_transactions} random transactions...")

        # --- 2. Loop and generate data ---
        for i in range(num_transactions):
            # Pick a random account to receive the deposit
            random_account_id = random.choice(account_ids)

            # Generate a random deposit amount using Decimal for precision
            random_amount = Decimal(random.uniform(5.0, 500.0)).quantize(Decimal('0.01'), rounding=ROUND_UP)

            # Generate a fake description
            description = fake.sentence(nb_words=4)

            # --- 3. Use a database transaction for atomicity ---
            # This ensures that both the account balance UPDATE and the transaction INSERT
            # succeed or fail together.
            try:
                # Update the account's balance
                update_account_sql = "UPDATE accounts SET balance = balance + %s WHERE account_id = %s"
                cursor.execute(update_account_sql, (random_amount, random_account_id))

                # Insert the transaction record
                insert_transaction_sql = """
                    INSERT INTO transactions
                    (to_account_id, transaction_type, amount, description)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_transaction_sql, (random_account_id, 'DEPOSIT', random_amount, description))

                # If both operations were successful, commit the transaction to the database
                connection.commit()
                print(f"  Success {i+1}/{num_transactions}: Deposited {random_amount} to account {random_account_id}.")

            except Error as e:
                print(f"  Error during transaction, rolling back: {e}")
                connection.rollback() # Roll back the changes if anything failed

    except Error as e:
        print(f"Database error: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\nData generation complete. MySQL connection closed.")

if __name__ == '__main__':
    generate_random_transactions(10)
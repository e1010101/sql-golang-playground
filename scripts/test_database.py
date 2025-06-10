import pytest
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# A "fixture" is a function that sets up a resource for your tests.
# This fixture provides a database connection to any test that needs it.
# The `yield` keyword passes the connection to the test, and the code after
# `yield` (the cleanup code) runs after the test is finished.
@pytest.fixture(scope="module")
def db_connection():
    load_dotenv()
    
    """PyTest fixture to create a database connection."""
    print("\n--- Setting up database connection for tests ---")
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
        yield connection  # This is what the test function will receive
    except Error as e:
        pytest.fail(f"Failed to connect to the database: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\n--- Database connection closed ---")


# PyTest automatically finds functions starting with "test_"
# We pass our `db_connection` fixture as an argument.
def test_alice_balance(db_connection):
    """
    Tests if the balance for account ID 1 (Alice) is what we expect.
    """
    print("\nRunning test: test_alice_balance")
    # --- IMPORTANT ---
    # Set this to the current balance of account ID 1 in your database to make the test pass.
    # Check it with: SELECT balance FROM accounts WHERE account_id = 1;
    expected_balance = 969.25

    cursor = db_connection.cursor(dictionary=True)
    cursor.execute("SELECT balance FROM accounts WHERE account_id = 1")
    result = cursor.fetchone() # fetchone() gets the first result

    assert result is not None, "Account with ID 1 (Alice) was not found."
    # The `assert` keyword is the core of the test. If the condition is false, the test fails.
    assert float(result['balance']) == expected_balance, f"Alice's balance should be {expected_balance} but was {result['balance']}"

def test_bob_exists(db_connection):
    """
    Tests if an account with the holder name 'Bob The Builder' exists.
    """
    print("\nRunning test: test_bob_exists")
    cursor = db_connection.cursor() # No dictionary needed, we just check existence
    cursor.execute("SELECT 1 FROM accounts WHERE account_holder = 'Bob The Builder' AND is_deleted = FALSE")
    result = cursor.fetchone()

    assert result is not None, "Account for 'Bob The Builder' was not found."
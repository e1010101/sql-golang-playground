package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"errors"

	"github.com/joho/godotenv" // Import godotenv
	_ "github.com/go-sql-driver/mysql"
	"sql-golang-playground/repository"
)

func main() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Main: Error loading .env file: %v", err)
	}

	dsn := os.Getenv("DATABASE_DSN")
	if dsn == "" {
		log.Fatal("Main: DATABASE_DSN environment variable not set in .env file or environment.")
	}

	// Open database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Main: Error opening database: %v", err)
	}
	defer db.Close() // Ensure database connection is closed when main function exits

	// Ping to verify connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Main: Error connecting to database: %v", err)
	}
	log.Println("Main: Successfully connected to database!")

    // Initialize transaction repository
    transactionRepo := repository.NewMySQLTransactionRepository(db)

    // Assume accID1 was created earlier
    accID1 := int64(1) // Replace with an actual ID from your DB

    // Soft Delete Account
    fmt.Printf("\nAttempting to soft delete account ID %d...\n", accID1)
    affected, err := SoftDeleteAccount(db, accID1)
    if err != nil {
        log.Printf("Error soft deleting account: %v\n", err)
    } else if affected > 0 {
        fmt.Printf("Soft deleted account ID: %d\n", accID1)
    }

    // Try to get it with the normal function (should fail or return not found)
    _, err = GetAccountByID(db, accID1)
    if err != nil {
        fmt.Printf("Attempt to get active account ID %d after soft delete: %v (this is expected)\n", accID1, err)
    }

	// Undelete Account
    fmt.Printf("\nAttempting to undelete account ID %d...\n", accID1)
    affected, err = UndeleteAccount(db, accID1)
    if err != nil {
        log.Printf("Error undeleting account: %v", err)
    } else if affected > 0 {
        fmt.Printf("Undeleted account ID: %d\n", accID1)
    }

	// Get total balance of active accounts
	totalBal, err := CalculateTotalBalanceOfActiveAccounts(db)
	if err != nil {
		log.Printf("Error calculating total balance: %v", err)
	} else {
		fmt.Printf("Total balance of all active accounts: %.2f\n", totalBal)
	}

	// Get transactions with categories
	aliceTransactionsWithCat, err := GetTransactionsWithCategory(db, 1)
	if err != nil {
		log.Printf("Error getting transactions with categories: %v", err)
	} else {
		fmt.Println("\nAlice's Transactions with Categories:")
		for _, tx := range aliceTransactionsWithCat {
			fmt.Printf("  ID: %d, Type: %s, Amount: %.2f, Desc: %s",
				tx.Transaction.TransactionID, tx.Transaction.TransactionType, tx.Transaction.Amount, tx.Transaction.Description.String)
			if tx.CategoryName.Valid {
				fmt.Printf(", Category: %s", tx.CategoryName.String)
			}
			fmt.Println()
		}
	}

	// Transaction WITH notes
	desc1 := sql.NullString{String: "Salary payment", Valid: true}
	notes1 := sql.NullString{String: "Monthly salary for June", Valid: true}
	// Assuming account ID 1 exists (e.g., Alice's account)
	toAccount1 := sql.NullInt64{Int64: 1, Valid: true} // Deposit to Alice
	fromAccount1 := sql.NullInt64{Valid: false}      // From external source

	txID1, err := CreateTransactionWithNotes(db, fromAccount1, toAccount1, "DEPOSIT", 3000.00, desc1, notes1)
	if err != nil {
		log.Printf("Error creating transaction with notes: %v", err)
	} else {
		fmt.Printf("Created transaction with notes, ID: %d\n", txID1)
	}

	// Transaction WITHOUT notes (notes will be NULL in DB)
	desc2 := sql.NullString{String: "Coffee purchase", Valid: true}
	notes2 := sql.NullString{Valid: false} // No notes, so Valid is false
	fromAccount2 := sql.NullInt64{Int64: 1, Valid: true} // Withdrawal from Alice
	toAccount2 := sql.NullInt64{Valid: false}        // To external vendor

	txID2, err := CreateTransactionWithNotes(db, fromAccount2, toAccount2, "WITHDRAWAL", -4.50, desc2, notes2) // Amount is negative for withdrawal
	if err != nil {
		log.Printf("Error creating transaction without notes: %v", err)
	} else {
		fmt.Printf("Created transaction without notes, ID: %d\n", txID2)
	}

    // --- Example: Fund Transfer (demonstrating new error handling) ---
    // Make sure accounts 1 and 2 exist and account 1 has funds
    // E.g., CreateAccount(db, "Sender One", 1000.00) -> ID 1
    //       CreateAccount(db, "Receiver Two", 200.00) -> ID 2
    err = TransferFunds(db, 1, 2, 50.75, "Payment for services", "Invoice #123")
    if err != nil {
        log.Printf("ERROR: Fund transfer failed: %v", err) // Log the full wrapped error
        if errors.Is(err, ErrInsufficientFunds) {
            fmt.Println("User Message: The transfer could not be completed due to insufficient funds.")
        } else if errors.Is(err, ErrAccountNotFound) || errors.Is(err, ErrAccountInactive) {
            fmt.Println("User Message: One of the accounts involved is not valid or active.")
        }
        // etc.
    } else {
        fmt.Println("Fund transfer initiated successfully (check logs for details).")
    }


    // --- Example: Reconciliation ---
    csvTransactions, err := loadExternalTransactions("external_transactions.csv")
    if err != nil {
        log.Fatalf("Main: Failed to load external transactions: %v", err)
    }
    log.Printf("Main: Loaded %d transactions from CSV.\n", len(csvTransactions))

    databaseTransactions, err := transactionRepo.GetAllTransactionsForReconciliation()
    if err != nil {
        log.Fatalf("Main: Failed to fetch database transactions: %v", err)
    }
    log.Printf("Main: Fetched %d transactions from Database.\n", len(databaseTransactions))

    reconcileTransactions(databaseTransactions, csvTransactions)
}

// TransferFunds handles the atomic transfer of funds between two accounts.
// It logs the transaction and ensures proper error handling and rollback.
func TransferFunds(db *sql.DB, fromAccountID int64, toAccountID int64, amount float64, description string, notes string) error {
    if fromAccountID == toAccountID {
        return ErrSameAccountTransfer
    }
    if amount <= 0 {
        return ErrInvalidTransferAmount
    }

    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("TransferFunds: failed to begin transaction: %w", err)
    }
    // Defer a rollback in case anything goes wrong.
    // If Commit() is called, the Rollback() is a no-op.
    defer tx.Rollback()

    var fromBalance float64
    var fromIsDeleted bool
    // Check sender's account status and balance
    // Ensure you select 'is_deleted' if you've implemented soft deletes
    err = tx.QueryRow("SELECT balance, is_deleted FROM accounts WHERE account_id = ?", fromAccountID).Scan(&fromBalance, &fromIsDeleted)
    if err != nil {
        if err == sql.ErrNoRows {
            return fmt.Errorf("TransferFunds: sender %w (ID: %d)", ErrAccountNotFound, fromAccountID)
        }
        return fmt.Errorf("TransferFunds: failed to query sender account (ID: %d): %w", fromAccountID, err)
    }
    if fromIsDeleted {
        return fmt.Errorf("TransferFunds: sender %w (ID: %d)", ErrAccountInactive, fromAccountID)
    }
    if fromBalance < amount {
        return fmt.Errorf("TransferFunds: sender %w (ID: %d, Balance: %.2f, Amount: %.2f)", ErrInsufficientFunds, fromAccountID, fromBalance, amount)
    }

    var toIsDeleted bool
    // Check receiver's account status
    err = tx.QueryRow("SELECT is_deleted FROM accounts WHERE account_id = ?", toAccountID).Scan(&toIsDeleted)
    if err != nil {
        if err == sql.ErrNoRows {
            return fmt.Errorf("TransferFunds: receiver %w (ID: %d)", ErrAccountNotFound, toAccountID)
        }
        return fmt.Errorf("TransferFunds: failed to query receiver account (ID: %d): %w", toAccountID, err)
    }
    if toIsDeleted {
        return fmt.Errorf("TransferFunds: receiver %w (ID: %d)", ErrAccountInactive, toAccountID)
    }

    // Decrement sender's balance
    _, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE account_id = ?", amount, fromAccountID)
    if err != nil {
        return fmt.Errorf("TransferFunds: failed to decrement sender's balance (ID: %d): %w", fromAccountID, err)
    }

    // Increment receiver's balance
    _, err = tx.Exec("UPDATE accounts SET balance = balance + ? WHERE account_id = ?", amount, toAccountID)
    if err != nil {
        return fmt.Errorf("TransferFunds: failed to increment receiver's balance (ID: %d): %w", toAccountID, err)
    }

    // Log the transaction
    sqlFromID := sql.NullInt64{Int64: fromAccountID, Valid: true}
    sqlToID := sql.NullInt64{Int64: toAccountID, Valid: true}
    sqlDescription := sql.NullString{String: description, Valid: description != ""}
    sqlNotes := sql.NullString{String: notes, Valid: notes != ""}

    _, err = CreateTransactionWithNotes(tx, sqlFromID, sqlToID, "TRANSFER", amount, sqlDescription, sqlNotes)
    if err != nil {
        return fmt.Errorf("TransferFunds: failed to log transaction: %w", err)
    }

    // If all operations were successful, commit the transaction
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("TransferFunds: failed to commit transaction: %w", err)
    }

    log.Printf("INFO: Successfully transferred %.2f from account %d to account %d", amount, fromAccountID, toAccountID)
    return nil
}

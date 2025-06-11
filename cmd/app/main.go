package main

import (
	"database/sql"
	"fmt"
	"log"
	"errors" // Keep errors import for service.Err*

	"sql-golang-playground/internal/db"
	"sql-golang-playground/repository"
	"sql-golang-playground/internal/service"
	"sql-golang-playground/internal/util"
)

func softDeleteDemo(accountRepo repository.AccountRepository) {
    // Assume accID1 was created earlier
    accID1 := int64(1) // Replace with an actual ID from your DB

    // Soft Delete Account
    fmt.Printf("\nAttempting to soft delete account ID %d...\n", accID1)
    affected, err := accountRepo.SoftDeleteAccount(accID1)
    if err != nil {
        log.Printf("Error soft deleting account: %v\n", err)
    } else if affected > 0 {
        fmt.Printf("Soft deleted account ID: %d\n", accID1)
    }

    // Try to get it with the normal function (should fail or return not found)
    _, err = accountRepo.GetAccountByID(accID1)
    if err != nil {
        fmt.Printf("Attempt to get active account ID %d after soft delete: %v (this is expected)\n", accID1, err)
    }

	// Undelete Account
    fmt.Printf("\nAttempting to undelete account ID %d...\n", accID1)
    affected, err = accountRepo.UndeleteAccount(accID1)
    if err != nil {
        log.Printf("Error undeleting account: %v", err)
    } else if affected > 0 {
        fmt.Printf("Undeleted account ID: %d\n", accID1)
    }
}

func totalBalanceDemo(accountRepo repository.AccountRepository) {
	// Get total balance of active accounts
	totalBal, err := accountRepo.CalculateTotalBalanceOfActiveAccounts()
	if err != nil {
		log.Printf("Error calculating total balance: %v", err)
	} else {
		fmt.Printf("Total balance of all active accounts: %.2f\n", totalBal)
	}
}

func transactionsWithCategoryDemo(transactionRepo repository.TransactionRepository) {
	// Get transactions with categories
	aliceTransactionsWithCat, err := transactionRepo.GetTransactionsWithCategory(1)
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
}

func transactionWithNotesDemo(transactionRepo repository.TransactionRepository) {
	// Transaction WITH notes
	desc1 := sql.NullString{String: "Salary payment", Valid: true}
	notes1 := sql.NullString{String: "Monthly salary for June", Valid: true}
	// Assuming account ID 1 exists (e.g., Alice's account)
	toAccount1 := sql.NullInt64{Int64: 1, Valid: true} // Deposit to Alice
	fromAccount1 := sql.NullInt64{Valid: false}      // From external source

	txID1, err := transactionRepo.CreateTransactionWithNotes(fromAccount1, toAccount1, "DEPOSIT", 3000.00, desc1, notes1)
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

	txID2, err := transactionRepo.CreateTransactionWithNotes(fromAccount2, toAccount2, "WITHDRAWAL", -4.50, desc2, notes2) // Amount is negative for withdrawal
	if err != nil {
		log.Printf("Error creating transaction without notes: %v", err)
	} else {
		fmt.Printf("Created transaction without notes, ID: %d\n", txID2)
	}
}

func fundTransferDemo(txService service.TransactionService) {
    // --- Example: Fund Transfer (demonstrating new error handling) ---
    // Make sure accounts 1 and 2 exist and account 1 has funds
    // E.g., CreateAccount(db, "Sender One", 1000.00) -> ID 1
    //       CreateAccount(db, "Receiver Two", 200.00) -> ID 2
    err := txService.TransferFunds(1, 2, 50.75, "Payment for services", "Invoice #123")
    if err != nil {
        log.Printf("ERROR: Fund transfer failed: %v", err) // Log the full wrapped error
        if errors.Is(err, service.ErrInsufficientFunds) {
            fmt.Println("User Message: The transfer could not be completed due to insufficient funds.")
        } else if errors.Is(err, service.ErrAccountNotFound) || errors.Is(err, service.ErrAccountInactive) {
            fmt.Println("User Message: One of the accounts involved is not valid or active.")
        } else if errors.Is(err, service.ErrSameAccountTransfer) {
            fmt.Println("User Message: Cannot transfer funds to the same account.")
        } else if errors.Is(err, service.ErrInvalidTransferAmount) {
            fmt.Println("User Message: Invalid transfer amount.")
        }
        // etc.
    } else {
        fmt.Println("Fund transfer initiated successfully (check logs for details).")
    }
}

func reconciliationDemo(reconciliationService service.ReconciliationService) {
    // --- Example: Reconciliation ---
    reconciliationService.ReconcileTransactions("data/external_transactions.csv")
}

func main() {
	dbConn := db.Connect() // Use the new db.Connect function
	defer dbConn.Close()   // Ensure database connection is closed

    // Initialize repositories
    accountRepo := repository.NewMySQLAccountRepository(dbConn)
    transactionRepo := repository.NewMySQLTransactionRepository(dbConn)

    // Initialize services
    txService := service.NewTransactionService(accountRepo, transactionRepo)
    dataLoader := util.NewCSVDataLoader()
    reconciliationService := service.NewReconciliationService(transactionRepo, dataLoader)

    demos := map[string]func(){ // Change signature to no parameters
        "soft_delete": func() { softDeleteDemo(accountRepo) },
        "total_balance": func() { totalBalanceDemo(accountRepo) },
        "transactions_with_category": func() { transactionsWithCategoryDemo(transactionRepo) },
        "transaction_with_notes": func() { transactionWithNotesDemo(transactionRepo) },
        "fund_transfer": func() { fundTransferDemo(txService) }, // Pass txService
        "reconciliation": func() { reconciliationDemo(reconciliationService) }, // Pass reconciliationService
    }

    fmt.Println("Available Demos:")
    for name := range demos {
        fmt.Printf("  - %s\n", name)
    }

    fmt.Print("Enter the name of the demo to run (or 'all' to run all, 'exit' to quit): ")
    var choice string
    fmt.Scanln(&choice)

    if choice == "all" {
        for name, demoFunc := range demos {
            fmt.Printf("\n--- Running Demo: %s ---\n", name)
            demoFunc() // Call without parameters
            fmt.Printf("--- Finished Demo: %s ---\n", name)
        }
    } else if choice == "exit" {
        fmt.Println("Exiting demo program.")
    } else {
        if demoFunc, ok := demos[choice]; ok {
            fmt.Printf("\n--- Running Demo: %s ---\n", choice)
            demoFunc() // Call without parameters
            fmt.Printf("--- Finished Demo: %s ---\n", choice)
        } else {
            fmt.Printf("Unknown demo: %s\n", choice)
        }
    }
}

package main

import (
	"database/sql"
	"fmt"
	"sql-golang-playground/models" // Import your models package
)

// CreateAccount inserts a new account into the database and returns the new account's ID.
func CreateAccount(dbtx DBTX, holderName string, initialBalance float64) (int64, error) {
    query := "INSERT INTO accounts (account_holder, balance) VALUES (?, ?)"
    result, err := dbtx.Exec(query, holderName, initialBalance)
    if err != nil {
        return 0, fmt.Errorf("CreateAccount: %w", err)
    }

    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("CreateAccount: LastInsertId failed: %w", err)
    }
    return id, nil
}

// GetAccountByID retrieves a single active account by its ID.
func GetAccountByID(dbtx DBTX, accountID int64) (models.Account, error) {
    var acc models.Account
    // Added is_deleted to SELECT and WHERE clause
    query := "SELECT account_id, account_holder, balance, last_updated, is_deleted FROM accounts WHERE account_id = ? AND is_deleted = FALSE"
    row := dbtx.QueryRow(query, accountID)
    // Added &acc.IsDeleted to Scan
    err := row.Scan(&acc.AccountID, &acc.AccountHolder, &acc.Balance, &acc.LastUpdated, &acc.IsDeleted)
    if err != nil {
        if err == sql.ErrNoRows {
            return acc, fmt.Errorf("GetAccountByID: no active account found with ID %d", accountID)
        }
        return acc, fmt.Errorf("GetAccountByID: %w", err)
    }
    return acc, nil
}

// GetAllAccounts retrieves all active accounts from the database.
func GetAllAccounts(dbtx DBTX) ([]models.Account, error) {
    // Added is_deleted to SELECT and WHERE clause
    query := "SELECT account_id, account_holder, balance, last_updated, is_deleted FROM accounts WHERE is_deleted = FALSE"
    rows, err := dbtx.Query(query)
    if err != nil {
        return nil, fmt.Errorf("GetAllAccounts: %w", err)
    }
    defer rows.Close()

    var accounts []models.Account
    for rows.Next() {
        var acc models.Account
        // Added &acc.IsDeleted to Scan
        if err := rows.Scan(&acc.AccountID, &acc.AccountHolder, &acc.Balance, &acc.LastUpdated, &acc.IsDeleted); err != nil {
            return nil, fmt.Errorf("GetAllAccounts: scan error: %w", err)
        }
        accounts = append(accounts, acc)
    }
    if err = rows.Err(); err != nil {
        return nil, fmt.Errorf("GetAllAccounts: rows iteration error: %w", err)
    }
    return accounts, nil
}

// UpdateAccountHolderName updates the name of an existing account.
func UpdateAccountHolderName(dbtx DBTX, accountID int64, newHolderName string) (int64, error) {
    query := "UPDATE accounts SET account_holder = ? WHERE account_id = ?"
    result, err := dbtx.Exec(query, newHolderName, accountID)
    if err != nil {
        return 0, fmt.Errorf("UpdateAccountHolderName: %w", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("UpdateAccountHolderName: RowsAffected failed: %w", err)
    }
    return rowsAffected, nil
}

// AdjustAccountBalance adds a specified amount to an account's balance.
// Amount can be positive (deposit) or negative (withdrawal).
func AdjustAccountBalance(dbtx DBTX, accountID int64, amountChange float64) (int64, error) {
    // Consider adding a check if account exists first, or if balance would go negative if not allowed
    query := "UPDATE accounts SET balance = balance + ? WHERE account_id = ?"
    result, err := dbtx.Exec(query, amountChange, accountID)
    if err != nil {
        return 0, fmt.Errorf("AdjustAccountBalance: %w", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("AdjustAccountBalance: RowsAffected failed: %w", err)
    }
    return rowsAffected, nil
}

// SoftDeleteAccount marks an account as deleted instead of removing it from the database.
func SoftDeleteAccount(dbtx DBTX, accountID int64) (int64, error) {
    query := "UPDATE accounts SET is_deleted = TRUE WHERE account_id = ? AND is_deleted = FALSE" // Only soft delete if not already deleted
    result, err := dbtx.Exec(query, accountID)
    if err != nil {
        return 0, fmt.Errorf("SoftDeleteAccount: %w", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("SoftDeleteAccount: RowsAffected failed: %w", err)
    }
    if rowsAffected == 0 {
        // This could mean the account didn't exist or was already soft-deleted.
        // You might want to check if the account exists first for a more specific error.
        // For now, we'll just indicate no rows were affected.
        return 0, fmt.Errorf("SoftDeleteAccount: no active account found with ID %d to soft delete, or already soft-deleted", accountID)
    }
    return rowsAffected, nil
}

// UndeleteAccount marks a soft-deleted account as active again.
func UndeleteAccount(dbtx DBTX, accountID int64) (int64, error) {
    query := "UPDATE accounts SET is_deleted = FALSE WHERE account_id = ? AND is_deleted = TRUE" // Only undelete if currently soft-deleted
    result, err := dbtx.Exec(query, accountID)
    if err != nil {
        return 0, fmt.Errorf("UndeleteAccount: %w", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("UndeleteAccount: RowsAffected failed: %w", err)
    }
    if rowsAffected == 0 {
         return 0, fmt.Errorf("UndeleteAccount: no soft-deleted account found with ID %d to undelete, or already active", accountID)
    }
    return rowsAffected, nil
}

// computes the sum of balances for all non-deleted accounts.
func CalculateTotalBalanceOfActiveAccounts(dbtx DBTX) (float64, error) {
    var totalBalance sql.NullFloat64 // Use sql.NullFloat64 in case there are no accounts or all balances are NULL (unlikely for balance)

    // Query to sum balances of active accounts
    query := "SELECT SUM(balance) FROM accounts WHERE is_deleted = FALSE"
    row := dbtx.QueryRow(query)
    err := row.Scan(&totalBalance)
    if err != nil {
        // This error could also occur if Scan fails for other reasons
        return 0, fmt.Errorf("CalculateTotalBalanceOfActiveAccounts: Scan failed: %w", err)
    }

    if totalBalance.Valid {
        return totalBalance.Float64, nil
    }
    // If totalBalance is not valid, it means SUM(balance) returned NULL.
    // This happens if there are no rows matching the WHERE clause (no active accounts).
    // In this case, the total balance is effectively 0.
    return 0, nil
}

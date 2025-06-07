package repository

import (
	"database/sql"
	"fmt"
	"sql-golang-playground/models"
)

// mysqlAccountRepository implements AccountRepository for MySQL.
type mysqlAccountRepository struct {
	db *sql.DB
}

// NewMySQLAccountRepository creates a new MySQL account repository.
func NewMySQLAccountRepository(db *sql.DB) AccountRepository {
	return &mysqlAccountRepository{db: db}
}

// CreateAccount inserts a new account into the database and returns the new account's ID.
func (r *mysqlAccountRepository) CreateAccount(holderName string, initialBalance float64) (int64, error) {
    query := "INSERT INTO accounts (account_holder, balance) VALUES (?, ?)"
    result, err := r.db.Exec(query, holderName, initialBalance)
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
func (r *mysqlAccountRepository) GetAccountByID(accountID int64) (models.Account, error) {
    var acc models.Account
    query := "SELECT account_id, account_holder, balance, last_updated, is_deleted FROM accounts WHERE account_id = ? AND is_deleted = FALSE"
    row := r.db.QueryRow(query, accountID)
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
func (r *mysqlAccountRepository) GetAllAccounts() ([]models.Account, error) {
    query := "SELECT account_id, account_holder, balance, last_updated, is_deleted FROM accounts WHERE is_deleted = FALSE"
    rows, err := r.db.Query(query)
    if err != nil {
        return nil, fmt.Errorf("GetAllAccounts: %w", err)
    }
    defer rows.Close()

    var accounts []models.Account
    for rows.Next() {
        var acc models.Account
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
func (r *mysqlAccountRepository) UpdateAccountHolderName(accountID int64, newHolderName string) (int64, error) {
    query := "UPDATE accounts SET account_holder = ? WHERE account_id = ?"
    result, err := r.db.Exec(query, newHolderName, accountID)
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
func (r *mysqlAccountRepository) AdjustAccountBalance(accountID int64, amountChange float64) (int64, error) {
    query := "UPDATE accounts SET balance = balance + ? WHERE account_id = ?"
    result, err := r.db.Exec(query, amountChange, accountID)
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
func (r *mysqlAccountRepository) SoftDeleteAccount(accountID int64) (int64, error) {
    query := "UPDATE accounts SET is_deleted = TRUE WHERE account_id = ? AND is_deleted = FALSE"
    result, err := r.db.Exec(query, accountID)
    if err != nil {
        return 0, fmt.Errorf("SoftDeleteAccount: %w", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("SoftDeleteAccount: RowsAffected failed: %w", err)
    }
    if rowsAffected == 0 {
        return 0, fmt.Errorf("SoftDeleteAccount: no active account found with ID %d to soft delete, or already soft-deleted", accountID)
    }
    return rowsAffected, nil
}

// CalculateTotalBalanceOfActiveAccounts computes the sum of balances for all non-deleted accounts.
func (r *mysqlAccountRepository) CalculateTotalBalanceOfActiveAccounts() (float64, error) {
    var totalBalance sql.NullFloat64

    query := "SELECT SUM(balance) FROM accounts WHERE is_deleted = FALSE"
    row := r.db.QueryRow(query)
    err := row.Scan(&totalBalance)
    if err != nil {
        return 0, fmt.Errorf("CalculateTotalBalanceOfActiveAccounts: Scan failed: %w", err)
    }

    if totalBalance.Valid {
        return totalBalance.Float64, nil
    }
    return 0, nil
}

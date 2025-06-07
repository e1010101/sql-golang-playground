package repository

import (
	"database/sql"
	"fmt"
	"sql-golang-playground/models"
)

// mysqlTransactionRepository implements TransactionRepository for MySQL.
type mysqlTransactionRepository struct {
	db *sql.DB
}

// NewMySQLTransactionRepository creates a new MySQL transaction repository.
func NewMySQLTransactionRepository(db *sql.DB) TransactionRepository {
	return &mysqlTransactionRepository{db: db}
}

// CreateTransaction inserts a new transaction and returns its ID.
func (r *mysqlTransactionRepository) CreateTransaction(fromID, toID sql.NullInt64, txType string, amount float64, description sql.NullString) (int64, error) {
    query := "INSERT INTO transactions (from_account_id, to_account_id, transaction_type, amount, description, transaction_ts) VALUES (?, ?, ?, ?, ?, NOW())"
    result, err := r.db.Exec(query, fromID, toID, txType, amount, description)
    if err != nil {
        return 0, fmt.Errorf("CreateTransaction: %w", err)
    }

    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("CreateTransaction: LastInsertId failed: %w", err)
    }
    return id, nil
}

// GetTransactionByID retrieves a single transaction by its ID.
func (r *mysqlTransactionRepository) GetTransactionByID(transactionID int64) (models.Transaction, error) {
    var tx models.Transaction
    query := "SELECT transaction_id, from_account_id, to_account_id, transaction_type, amount, transaction_ts, description FROM transactions WHERE transaction_id = ?"
    row := r.db.QueryRow(query, transactionID)
    err := row.Scan(&tx.TransactionID, &tx.FromAccountID, &tx.ToAccountID, &tx.TransactionType, &tx.Amount, &tx.TransactionTs, &tx.Description)
    if err != nil {
        if err == sql.ErrNoRows {
            return tx, fmt.Errorf("GetTransactionByID: no transaction with ID %d", transactionID)
        }
        return tx, fmt.Errorf("GetTransactionByID: %w", err)
    }
    return tx, nil
}

// GetTransactionsForAccount retrieves all transactions involving a specific account ID.
func (r *mysqlTransactionRepository) GetTransactionsForAccount(accountID int64) ([]models.Transaction, error) {
    query := "SELECT transaction_id, from_account_id, to_account_id, transaction_type, amount, transaction_ts, description FROM transactions WHERE from_account_id = ? OR to_account_id = ? ORDER BY transaction_ts DESC"
    rows, err := r.db.Query(query, accountID, accountID)
    if err != nil {
        return nil, fmt.Errorf("GetTransactionsForAccount: %w", err)
    }
    defer rows.Close()

    var transactions []models.Transaction
    for rows.Next() {
        var tx models.Transaction
        if err := rows.Scan(&tx.TransactionID, &tx.FromAccountID, &tx.ToAccountID, &tx.TransactionType, &tx.Amount, &tx.TransactionTs, &tx.Description); err != nil {
            return nil, fmt.Errorf("GetTransactionsForAccount: scan error: %w", err)
        }
        transactions = append(transactions, tx)
    }
    if err = rows.Err(); err != nil {
        return nil, fmt.Errorf("GetTransactionsForAccount: rows iteration error: %w", err)
    }
    return transactions, nil
}

// GetTransactionsWithCategory retrieves transactions along with their category names.
func (r *mysqlTransactionRepository) GetTransactionsWithCategory(accountID int64) ([]models.TransactionWithCategory, error) {
    query := `
        SELECT
            t.transaction_id, t.from_account_id, t.to_account_id,
            t.transaction_type, t.amount, t.transaction_ts, t.description,
            tc.category_name
        FROM
            transactions t
        LEFT JOIN
            transaction_categories tc ON t.category_id = tc.category_id
        WHERE
            (t.from_account_id = ? OR t.to_account_id = ?)
        ORDER BY
            t.transaction_ts DESC;`

    rows, err := r.db.Query(query, accountID, accountID)
    if err != nil {
        return nil, fmt.Errorf("GetTransactionsWithCategory: db.Query failed: %w", err)
    }
    defer rows.Close()

    var results []models.TransactionWithCategory
    for rows.Next() {
        var twc models.TransactionWithCategory
        err := rows.Scan(
            &twc.Transaction.TransactionID, &twc.Transaction.FromAccountID, &twc.Transaction.ToAccountID,
            &twc.Transaction.TransactionType, &twc.Transaction.Amount, &twc.Transaction.TransactionTs,
            &twc.Transaction.Description,
            &twc.CategoryName,
        )
        if err != nil {
            return nil, fmt.Errorf("GetTransactionsWithCategory: rows.Scan failed: %w", err)
        }
        results = append(results, twc)
    }
    if err = rows.Err(); err != nil {
        return nil, fmt.Errorf("GetTransactionsWithCategory: rows.Err: %w", err)
    }
    return results, nil
}

// UpdateTransactionDescription updates the description of an existing transaction.
func (r *mysqlTransactionRepository) UpdateTransactionDescription(transactionID int64, newDescription sql.NullString) (int64, error) {
    query := "UPDATE transactions SET description = ? WHERE transaction_id = ?"
    result, err := r.db.Exec(query, newDescription, transactionID)
    if err != nil {
        return 0, fmt.Errorf("UpdateTransactionDescription: %w", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("UpdateTransactionDescription: RowsAffected failed: %w", err)
    }
    return rowsAffected, nil
}

// DeleteTransaction removes a transaction from the database.
func (r *mysqlTransactionRepository) DeleteTransaction(transactionID int64) (int64, error) {
    query := "DELETE FROM transactions WHERE transaction_id = ?"
    result, err := r.db.Exec(query, transactionID)
    if err != nil {
        return 0, fmt.Errorf("DeleteTransaction: %w", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("DeleteTransaction: RowsAffected failed: %w", err)
    }
    return rowsAffected, nil
}

// GetAllTransactionsForReconciliation retrieves all transactions from the database for reconciliation.
func (r *mysqlTransactionRepository) GetAllTransactionsForReconciliation() ([]models.Transaction, error) {
    query := "SELECT transaction_id, from_account_id, to_account_id, transaction_type, amount, description, notes, transaction_ts FROM transactions ORDER BY transaction_id"
    rows, err := r.db.Query(query)
    if err != nil {
        return nil, fmt.Errorf("GetAllTransactionsForReconciliation: %w", err)
    }
    defer rows.Close()

    var transactions []models.Transaction
    for rows.Next() {
        var tx models.Transaction
        if err := rows.Scan(&tx.TransactionID, &tx.FromAccountID, &tx.ToAccountID, &tx.TransactionType, &tx.Amount, &tx.Description, &tx.Notes, &tx.TransactionTs); err != nil {
            return nil, fmt.Errorf("GetAllTransactionsForReconciliation: scan error: %w", err)
        }
        transactions = append(transactions, tx)
    }
    if err = rows.Err(); err != nil {
        return nil, fmt.Errorf("GetAllTransactionsForReconciliation: rows iteration error: %w", err)
    }
    return transactions, nil
}

package main

import (
	"database/sql"
	"fmt"
	"sql-golang-playground/models" // Import your models package
)

// CreateTransaction inserts a new transaction and returns its ID.
// Use sql.NullInt64{Int64: id, Valid: true} for account IDs, or sql.NullInt64{Valid: false} if NULL.
// Use sql.NullString{String: desc, Valid: true} for description, or sql.NullString{Valid: false} if NULL.
func CreateTransaction(dbtx DBTX, fromID, toID sql.NullInt64, txType string, amount float64, description sql.NullString) (int64, error) {
    query := "INSERT INTO transactions (from_account_id, to_account_id, transaction_type, amount, description, transaction_ts) VALUES (?, ?, ?, ?, ?, NOW())"
    result, err := dbtx.Exec(query, fromID, toID, txType, amount, description)
    if err != nil {
        return 0, fmt.Errorf("CreateTransaction: %w", err)
    }

    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("CreateTransaction: LastInsertId failed: %w", err)
    }
    return id, nil
}

// CreateTransactionWithNotes inserts a new transaction, allowing for nullable description and notes.
func CreateTransactionWithNotes(dbtx DBTX, fromID, toID sql.NullInt64, txType string, amount float64, description, notes sql.NullString) (int64, error) {
    query := "INSERT INTO transactions (from_account_id, to_account_id, transaction_type, amount, description, notes, transaction_ts) VALUES (?, ?, ?, ?, ?, ?, NOW())"
    result, err := dbtx.Exec(query, fromID, toID, txType, amount, description, notes)
    if err != nil {
        return 0, fmt.Errorf("CreateTransactionWithNotes: %w", err)
    }

    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("CreateTransactionWithNotes: LastInsertId failed: %w", err)
    }
    return id, nil
}

// GetTransactionByID retrieves a single transaction by its ID, including new 'notes' field.
func GetTransactionByID(dbtx DBTX, transactionID int64) (models.Transaction, error) {
    var tx models.Transaction
    // Added 'notes' to the SELECT list
    query := "SELECT transaction_id, from_account_id, to_account_id, transaction_type, amount, transaction_ts, description, notes FROM transactions WHERE transaction_id = ?"
    row := dbtx.QueryRow(query, transactionID)
    // Added &tx.Notes to Scan
    err := row.Scan(&tx.TransactionID, &tx.FromAccountID, &tx.ToAccountID, &tx.TransactionType, &tx.Amount, &tx.TransactionTs, &tx.Description, &tx.Notes)
    if err != nil {
        if err == sql.ErrNoRows {
            return tx, fmt.Errorf("GetTransactionByID: no transaction with ID %d", transactionID)
        }
        return tx, fmt.Errorf("GetTransactionByID: %w", err)
    }
    return tx, nil
}

// GetTransactionsForAccount retrieves all transactions involving a specific account ID.
func GetTransactionsForAccount(dbtx DBTX, accountID int64) ([]models.Transaction, error) {
    query := "SELECT transaction_id, from_account_id, to_account_id, transaction_type, amount, transaction_ts, description FROM transactions WHERE from_account_id = ? OR to_account_id = ? ORDER BY transaction_ts DESC"
    rows, err := dbtx.Query(query, accountID, accountID)
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
func GetTransactionsWithCategory(dbtx DBTX, accountID int64) ([]models.TransactionWithCategory, error) {
    // SQL JOIN query
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

    rows, err := dbtx.Query(query, accountID, accountID)
    if err != nil {
        return nil, fmt.Errorf("GetTransactionsWithCategory: dbtx.Query failed: %w", err)
    }
    defer rows.Close()

    var results []models.TransactionWithCategory
    for rows.Next() {
        var twc models.TransactionWithCategory
        // Ensure your Transaction struct has a field for 'notes' if you've added it
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
func UpdateTransactionDescription(dbtx DBTX, transactionID int64, newDescription sql.NullString) (int64, error) {
    query := "UPDATE transactions SET description = ? WHERE transaction_id = ?"
    result, err := dbtx.Exec(query, newDescription, transactionID)
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
func DeleteTransaction(dbtx DBTX, transactionID int64) (int64, error) {
    query := "DELETE FROM transactions WHERE transaction_id = ?"
    result, err := dbtx.Exec(query, transactionID)
    if err != nil {
        return 0, fmt.Errorf("DeleteTransaction: %w", err)
    }
    rowsAffected, err := result.RowsAffected()
    if err != nil {
        return 0, fmt.Errorf("DeleteTransaction: RowsAffected failed: %w", err)
    }
    return rowsAffected, nil
}

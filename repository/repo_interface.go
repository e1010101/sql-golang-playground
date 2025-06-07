package repository

import (
	"database/sql"
	"sql-golang-playground/models"
)

// AccountRepository defines the interface for account-related database operations.
type AccountRepository interface {
	CreateAccount(holderName string, initialBalance float64) (int64, error)
	GetAccountByID(accountID int64) (models.Account, error)
	GetAllAccounts() ([]models.Account, error)
	UpdateAccountHolderName(accountID int64, newHolderName string) (int64, error)
	AdjustAccountBalance(accountID int64, amountChange float64) (int64, error)
	SoftDeleteAccount(accountID int64) (int64, error)
	CalculateTotalBalanceOfActiveAccounts() (float64, error)
}

// TransactionRepository defines the interface for transaction-related database operations.
type TransactionRepository interface {
	CreateTransaction(fromID, toID sql.NullInt64, txType string, amount float64, description sql.NullString) (int64, error)
	GetTransactionByID(transactionID int64) (models.Transaction, error)
	GetTransactionsForAccount(accountID int64) ([]models.Transaction, error)
	GetTransactionsWithCategory(accountID int64) ([]models.TransactionWithCategory, error)
	UpdateTransactionDescription(transactionID int64, newDescription sql.NullString) (int64, error)
	DeleteTransaction(transactionID int64) (int64, error)
	GetAllTransactionsForReconciliation() ([]models.Transaction, error)
}

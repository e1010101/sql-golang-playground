package models

import (
	"database/sql"
	"time"
)

type Transaction struct {
    TransactionID   int64
    FromAccountID   sql.NullInt64 // Nullable foreign key
    ToAccountID     sql.NullInt64 // Nullable foreign key
    TransactionType string
    Amount          float64
    TransactionTs   time.Time
    Description     sql.NullString // Assuming description can be NULL
    Notes           sql.NullString // New field for transaction notes
}

type TransactionWithCategory struct {
    Transaction              // Embed the original Transaction struct
    CategoryName sql.NullString // For category_name from the joined table
}

type ExternalTransaction struct {
    ExternalID string
    Amount     float64
    Type       string // e.g., DEPOSIT, WITHDRAWAL, TRANSFER_OUT, TRANSFER_IN
    Reference  string
}

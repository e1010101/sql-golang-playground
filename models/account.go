package models

import (
	"time"
)

type Account struct {
    AccountID     int64
    AccountHolder string
    Balance       float64
    LastUpdated   time.Time
    IsDeleted     bool // New field for soft deletion
}

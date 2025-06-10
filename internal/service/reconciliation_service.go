package service

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"sql-golang-playground/models"
	"sql-golang-playground/repository"
	"sql-golang-playground/internal/util"
)

// ReconciliationService defines the interface for reconciliation business logic.
type ReconciliationService interface {
	ReconcileTransactions(csvFilePath string)
}

// reconciliationServiceImpl implements ReconciliationService.
type reconciliationServiceImpl struct {
	transactionRepo repository.TransactionRepository
	dataLoader      util.DataLoader
}

// NewReconciliationService creates a new reconciliation service.
func NewReconciliationService(transactionRepo repository.TransactionRepository, dataLoader util.DataLoader) ReconciliationService {
	return &reconciliationServiceImpl{
		transactionRepo: transactionRepo,
		dataLoader:      dataLoader,
	}
}

// normalizeDBTransactionType standardizes DB transaction types for comparison.
func (s *reconciliationServiceImpl) normalizeDBTransactionType(dbType string, fromID, toID sql.NullInt64) string {
    dbType = strings.ToUpper(dbType)
    switch dbType {
    case "DEPOSIT":
        return "DEPOSIT"
    case "WITHDRAWAL":
        return "WITHDRAWAL"
    case "TRANSFER":
        if fromID.Valid && !toID.Valid { // Assuming transfer to external
            return "TRANSFER_OUT"
        } else if !fromID.Valid && toID.Valid { // Assuming transfer from external
            return "TRANSFER_IN"
        } else if fromID.Valid && toID.Valid { // Internal transfer
            return "INTERNAL_TRANSFER" // Or just TRANSFER if CSV doesn't distinguish internal
        }
    }
    return dbType // Fallback
}

// ReconcileTransactions performs reconciliation between database and external CSV transactions.
func (s *reconciliationServiceImpl) ReconcileTransactions(csvFilePath string) {
    fmt.Println("\n--- Reconciliation Report ---")

    csvTransactions, err := s.dataLoader.LoadExternalTransactions(csvFilePath)
    if err != nil {
        log.Fatalf("ReconciliationService: Failed to load external transactions: %v", err)
    }
    log.Printf("ReconciliationService: Loaded %d transactions from CSV.\n", len(csvTransactions))

    databaseTransactions, err := s.transactionRepo.GetAllTransactionsForReconciliation()
    if err != nil {
        log.Fatalf("ReconciliationService: Failed to fetch database transactions: %v", err)
    }
    log.Printf("ReconciliationService: Fetched %d transactions from Database.\n", len(databaseTransactions))

    // Using maps to track processed items to avoid double-counting in simple N*M comparison
    processedDBTx := make(map[int64]bool)
    processedCSVTx := make(map[string]bool)

    var foundInBoth []string
    var mismatchedAmounts []string // Records matched by type but amounts differ

    // Attempt to match DB transactions against CSV transactions
    for _, dbTx := range databaseTransactions {
        if processedDBTx[dbTx.TransactionID] {
            continue
        }
        matchedThisDBTx := false
        for _, csvTx := range csvTransactions {
            if processedCSVTx[csvTx.ExternalID] {
                continue
            }

            // Normalize DB type for comparison (e.g. your DB 'TRANSFER' might map to CSV 'TRANSFER_OUT' or 'TRANSFER_IN')
            normalizedDBType := s.normalizeDBTransactionType(dbTx.TransactionType, dbTx.FromAccountID, dbTx.ToAccountID)
            
            // Criteria 1: Type and Amount match
            if normalizedDBType == csvTx.Type && dbTx.Amount == csvTx.Amount {
                foundInBoth = append(foundInBoth, fmt.Sprintf("  MATCH: DB ID %d (%.2f %s) with CSV ID %s (%.2f %s, Ref: %s)",
                    dbTx.TransactionID, dbTx.Amount, normalizedDBType,
                    csvTx.ExternalID, csvTx.Amount, csvTx.Type, csvTx.Reference))
                processedDBTx[dbTx.TransactionID] = true
                processedCSVTx[csvTx.ExternalID] = true
                matchedThisDBTx = true
                break // Found a match for this DB transaction
            }
        }
        // If no exact match on amount and type, check for type match with different amount
        if !matchedThisDBTx {
             for _, csvTx := range csvTransactions {
                if processedCSVTx[csvTx.ExternalID] { // Skip already fully matched CSV
                    continue
                }
                normalizedDBType := s.normalizeDBTransactionType(dbTx.TransactionType, dbTx.FromAccountID, dbTx.ToAccountID)
                if normalizedDBType == csvTx.Type { // Type matches, amount must differ (otherwise caught above)
                    mismatchedAmounts = append(mismatchedAmounts, fmt.Sprintf("  MISMATCH_AMOUNT: DB ID %d (%.2f %s) vs CSV ID %s (%.2f %s, Ref: %s)",
                        dbTx.TransactionID, dbTx.Amount, normalizedDBType,
                        csvTx.ExternalID, csvTx.Amount, csvTx.Type, csvTx.Reference))
                    processedDBTx[dbTx.TransactionID] = true // Mark as processed even if mismatched, to avoid being "only in DB"
                    processedCSVTx[csvTx.ExternalID] = true // Mark CSV as processed to avoid being "only in CSV"
                    // Note: This simple logic might misclassify if multiple CSV entries have the same type.
                    // A more robust system would use more unique identifiers or a tolerance for amounts.
                    break 
                }
            }
        }
    }

    var onlyInDB []string
    for _, dbTx := range databaseTransactions {
        if !processedDBTx[dbTx.TransactionID] {
            onlyInDB = append(onlyInDB, fmt.Sprintf("  DB ID: %d, Type: %s, Amount: %.2f, Desc: %s",
                dbTx.TransactionID, dbTx.TransactionType, dbTx.Amount, dbTx.Description.String))
        }
    }

    var onlyInCSV []string
    for _, csvTx := range csvTransactions {
        if !processedCSVTx[csvTx.ExternalID] {
            onlyInCSV = append(onlyInCSV, fmt.Sprintf("  CSV ID: %s, Type: %s, Amount: %.2f, Ref: %s",
                csvTx.ExternalID, csvTx.Type, csvTx.Amount, csvTx.Reference))
        }
    }

    fmt.Println("\n[Transactions Found in Both Systems (Exact Match on Type & Amount)]")
    if len(foundInBoth) > 0 {
        for _, item := range foundInBoth { fmt.Println(item) }
    } else {
        fmt.Println("  None")
    }
    
    fmt.Println("\n[Potential Matches with Mismatched Amounts (Same Type)]")
    if len(mismatchedAmounts) > 0 {
        for _, item := range mismatchedAmounts { fmt.Println(item) }
    } else {
        fmt.Println("  None")
    }

    fmt.Println("\n[Transactions Only in Database]")
    if len(onlyInDB) > 0 {
        for _, item := range onlyInDB { fmt.Println(item) }
    } else {
        fmt.Println("  None")
    }

    fmt.Println("\n[Transactions Only in CSV File]")
    if len(onlyInCSV) > 0 {
        for _, item := range onlyInCSV { fmt.Println(item) }
    } else {
        fmt.Println("  None")
    }
    fmt.Println("\n--- End of Reconciliation Report ---")
}

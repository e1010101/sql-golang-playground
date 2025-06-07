package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sql-golang-playground/models" // Import your models package
)

// loadExternalTransactions reads transactions from a CSV file.
func loadExternalTransactions(filePath string) ([]models.ExternalTransaction, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return nil, fmt.Errorf("loadExternalTransactions: failed to open file %s: %w", filePath, err)
    }
    defer file.Close()

    reader := csv.NewReader(file)
    _, err = reader.Read() // Skip header row
    if err != nil {
        if err == io.EOF {
            return []models.ExternalTransaction{}, nil // Empty file after header
        }
        return nil, fmt.Errorf("loadExternalTransactions: failed to read header: %w", err)
    }

    var transactions []models.ExternalTransaction
    for {
        record, err := reader.Read()
        if err != nil {
            if err == io.EOF {
                break
            }
            return nil, fmt.Errorf("loadExternalTransactions: error reading record: %w", err)
        }
        if len(record) < 4 {
             log.Printf("WARN: Skipping malformed CSV record: %v", record)
             continue
        }

        amount, err := strconv.ParseFloat(strings.TrimSpace(record[1]), 64)
        if err != nil {
            log.Printf("WARN: Skipping record with invalid amount %s: %v", record[1], err)
            continue
        }

        transactions = append(transactions, models.ExternalTransaction{
            ExternalID: strings.TrimSpace(record[0]),
            Amount:     amount,
            Type:       strings.TrimSpace(strings.ToUpper(record[2])),
            Reference:  strings.TrimSpace(record[3]),
        })
    }
    return transactions, nil
}

// normalizeDBTransactionType standardizes DB transaction types for comparison.
func normalizeDBTransactionType(dbType string, fromID, toID sql.NullInt64) string {
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


func reconcileTransactions(dbTxs []models.Transaction, csvTxs []models.ExternalTransaction) {
    fmt.Println("\n--- Reconciliation Report ---")

    // Using maps to track processed items to avoid double-counting in simple N*M comparison
    processedDBTx := make(map[int64]bool)
    processedCSVTx := make(map[string]bool)

    var foundInBoth []string
    var mismatchedAmounts []string // Records matched by type but amounts differ

    // Attempt to match DB transactions against CSV transactions
    for _, dbTx := range dbTxs {
        if processedDBTx[dbTx.TransactionID] {
            continue
        }
        matchedThisDBTx := false
        for _, csvTx := range csvTxs {
            if processedCSVTx[csvTx.ExternalID] {
                continue
            }

            // Normalize DB type for comparison (e.g. your DB 'TRANSFER' might map to CSV 'TRANSFER_OUT' or 'TRANSFER_IN')
            normalizedDBType := normalizeDBTransactionType(dbTx.TransactionType, dbTx.FromAccountID, dbTx.ToAccountID)
            
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
             for _, csvTx := range csvTxs {
                if processedCSVTx[csvTx.ExternalID] { // Skip already fully matched CSV
                    continue
                }
                normalizedDBType := normalizeDBTransactionType(dbTx.TransactionType, dbTx.FromAccountID, dbTx.ToAccountID)
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
    for _, dbTx := range dbTxs {
        if !processedDBTx[dbTx.TransactionID] {
            onlyInDB = append(onlyInDB, fmt.Sprintf("  DB ID: %d, Type: %s, Amount: %.2f, Desc: %s",
                dbTx.TransactionID, dbTx.TransactionType, dbTx.Amount, dbTx.Description.String))
        }
    }

    var onlyInCSV []string
    for _, csvTx := range csvTxs {
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

package util

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sql-golang-playground/models"
)

// DataLoader defines the interface for loading external data.
type DataLoader interface {
	LoadExternalTransactions(filePath string) ([]models.ExternalTransaction, error)
}

// csvDataLoader implements DataLoader for CSV files.
type csvDataLoader struct{}

// NewCSVDataLoader creates a new CSV data loader.
func NewCSVDataLoader() DataLoader {
	return &csvDataLoader{}
}

// LoadExternalTransactions reads transactions from a CSV file.
func (l *csvDataLoader) LoadExternalTransactions(filePath string) ([]models.ExternalTransaction, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return nil, fmt.Errorf("LoadExternalTransactions: failed to open file %s: %w", filePath, err)
    }
    defer file.Close()

    reader := csv.NewReader(file)
    _, err = reader.Read() // Skip header row
    if err != nil {
        if err == io.EOF {
            return []models.ExternalTransaction{}, nil // Empty file after header
        }
        return nil, fmt.Errorf("LoadExternalTransactions: failed to read header: %w", err)
    }

    var transactions []models.ExternalTransaction
    for {
        record, err := reader.Read()
        if err != nil {
            if err == io.EOF {
                break
            }
            return nil, fmt.Errorf("LoadExternalTransactions: error reading record: %w", err)
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

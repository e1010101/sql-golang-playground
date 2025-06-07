package main

import "database/sql"

// DBTX interface for database operations, allowing *sql.DB or *sql.Tx
type DBTX interface {
    Exec(query string, args ...interface{}) (sql.Result, error)
    QueryRow(query string, args ...interface{}) *sql.Row
    Query(query string, args ...interface{}) (*sql.Rows, error)
    Prepare(query string) (*sql.Stmt, error)
}

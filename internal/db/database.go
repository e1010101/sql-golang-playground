package db

import (
	"database/sql"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/go-sql-driver/mysql"
)

// Connect establishes a connection to the database using the DSN from environment variables.
func Connect() *sql.DB {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("DB: Error loading .env file: %v", err)
	}

	dsn := os.Getenv("DATABASE_DSN")
	if dsn == "" {
		log.Fatal("DB: DATABASE_DSN environment variable not set in .env file or environment.")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("DB: Error opening database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("DB: Error connecting to database: %v", err)
	}
	log.Println("DB: Successfully connected to database!")

	return db
}

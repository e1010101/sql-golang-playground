package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	mysqlRootPassword := os.Getenv("DB_PASSWORD")
	if mysqlRootPassword == "" {
		log.Fatalf("DB_PASSWORD not set in .env file")
	}

	dbUser := "root"
	dbHost := "127.0.0.1"
	dbPort := 3306
	dbName := "fund_playground_db"

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbUser, mysqlRootPassword, dbHost, dbPort, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	log.Println("Successfully connected to MySQL database!")

	// Create a test table if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS binlog_test (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		value INT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	log.Println("Table 'binlog_test' ensured to exist.")

	// --- Test Operations ---

	// 1. INSERT
	log.Println("Performing INSERT operation...")
	insertSQL := "INSERT INTO binlog_test (name, value) VALUES (?, ?)"
	res, err := db.Exec(insertSQL, "Test Item 1", 100)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	id1, _ := res.LastInsertId()
	log.Printf("Inserted new row with ID: %d\n", id1)
	time.Sleep(2 * time.Second)

	// 2. INSERT another
	log.Println("Performing another INSERT operation...")
	res, err = db.Exec(insertSQL, "Test Item 2", 200)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	id2, _ := res.LastInsertId()
	log.Printf("Inserted new row with ID: %d\n", id2)
	time.Sleep(2 * time.Second)

	// 3. UPDATE
	log.Println("Performing UPDATE operation...")
	updateSQL := "UPDATE binlog_test SET value = ?, name = ? WHERE id = ?"
	_, err = db.Exec(updateSQL, 150, "Updated Item 1", id1)
	if err != nil {
		log.Fatalf("Failed to update data: %v", err)
	}
	log.Printf("Updated row with ID: %d\n", id1)
	time.Sleep(2 * time.Second)

	// 4. DELETE
	log.Println("Performing DELETE operation...")
	deleteSQL := "DELETE FROM binlog_test WHERE id = ?"
	_, err = db.Exec(deleteSQL, id2)
	if err != nil {
		log.Fatalf("Failed to delete data: %v", err)
	}
	log.Printf("Deleted row with ID: %d\n", id2)
	time.Sleep(2 * time.Second)

	// 5. INSERT one more time
	log.Println("Performing final INSERT operation...")
	res, err = db.Exec(insertSQL, "Final Item", 300)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	id3, _ := res.LastInsertId()
	log.Printf("Inserted new row with ID: %d\n", id3)
	time.Sleep(2 * time.Second)

	log.Println("Test operations completed. You should see corresponding events in the binlog consumer.")
}

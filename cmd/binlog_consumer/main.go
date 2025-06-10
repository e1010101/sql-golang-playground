package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"database/sql"
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"github.com/go-mysql-org/go-mysql/mysql" // Re-add for mysql.Position
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/joho/godotenv" // Import godotenv
)

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	mysqlPassword := os.Getenv("MYSQL_REPLICATOR_PASSWORD")
	if mysqlPassword == "" {
		log.Fatalf("MYSQL_REPLICATOR_PASSWORD not set in .env file")
	}

	// --- 1. Create a Binlog Syncer Configuration ---
	// This config tells the library how to connect to MySQL.
	cfg := replication.BinlogSyncerConfig{
		ServerID: 101, // Must be unique for each slave/client.
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "replicator", // The user you created
		Password: mysqlPassword, // Use password from .env
		// FilterTables: []string{"fund_playground_db.accounts", "fund_playground_db.transactions"}, // Commented out for now
	}

	// --- 2. Create a new Binlog Syncer ---
	// The syncer object manages the connection and the binlog stream.
	syncer := replication.NewBinlogSyncer(cfg)

	// --- 3. Define the starting position ---
	// You need to tell the syncer where to start reading the binlog.
	// We'll get the current master position to start from the "live" point.
	// In a real application, you would save the last processed position and start from there.
	// Use database/sql to get master status
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	var file string
	var position uint32
	err = db.QueryRow("SHOW MASTER STATUS").Scan(&file, &position, new(string), new(string), new(string))
	if err != nil {
		log.Fatalf("Failed to get master status: %v", err)
	}

	pos := mysql.Position{Name: file, Pos: position} // Use mysql.Position
	log.Printf("Starting binlog stream from position: File=%s, Pos=%d\n", pos.Name, pos.Pos)

	// --- 4. Start the sync and get a stream of events ---
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		log.Fatalf("Failed to start binlog sync: %v", err)
	}

	log.Println("Binlog streamer started, waiting for events...")

	// --- 5. The Main Event Loop ---
	// This loop will run forever, receiving events from MySQL.
	// We'll use a context for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown on Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutdown signal received, stopping syncer...")
		syncer.Close()
		cancel()
	}()

	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if err == context.Canceled {
				log.Println("Context canceled, exiting event loop.")
				return
			}
			log.Fatalf("Error getting event from stream: %v", err)
		}

		// Dump raw event for debugging
		fmt.Println("---")
		ev.Dump(os.Stdout)

		// First, handle the rotate and query events by type code:
		switch ev.Header.EventType {
		case replication.ROTATE_EVENT:
			rotate := ev.Event.(*replication.RotateEvent)
			log.Printf("Rotated to new binlog file: %s, Position: %d\n",
				rotate.NextLogName, rotate.Position)

		case replication.QUERY_EVENT:
			q := ev.Event.(*replication.QueryEvent)
			log.Printf("Query Event: Schema=%s, Query=%s\n", q.Schema, q.Query)

		// Now fall through to a second switch that type-switches on the event struct:
		default:
			switch e := ev.Event.(type) {
			case *replication.RowsEvent:
				// Pass the EventType code and the concrete RowsEvent pointer
				handleRowsEvent(ev.Header.EventType, e)
			}
		}
	}
}

// handleRowsEvent processes events related to data changes.
func handleRowsEvent(eventType replication.EventType, e *replication.RowsEvent) {
	tableName := string(e.Table.Table)
    dbName    := string(e.Table.Schema)

    var action string
    switch eventType {
    // match both v1 and v2 row event codes
    case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
        action = "INSERT"
    case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
        action = "UPDATE"
    case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
        action = "DELETE"
    default:
        action = "UNKNOWN"
    }

    log.Printf("Received RowsEvent for %s.%s, Action: %s", dbName, tableName, action)

	// e.Rows is a slice of rows. Each row is a slice of its column values.
	// For UPDATEs, e.Rows contains pairs of [before_image, after_image].
	step := 1
	if action == "UPDATE" {
		step = 2 // For updates, each change is represented by two rows.
	}

	for i := 0; i < len(e.Rows); i += step {
		if action == "UPDATE" {
			fmt.Println("  [UPDATE]")
			fmt.Printf("    Before: %v\n", e.Rows[i])   // The row data before the update
			fmt.Printf("    After:  %v\n", e.Rows[i+1]) // The row data after the update
		} else if action == "INSERT" {
			fmt.Println("  [INSERT]")
			fmt.Printf("    New Row: %v\n", e.Rows[i])
		} else if action == "DELETE" {
			fmt.Println("  [DELETE]")
			fmt.Printf("    Deleted Row: %v\n", e.Rows[i])
		}
	}
}

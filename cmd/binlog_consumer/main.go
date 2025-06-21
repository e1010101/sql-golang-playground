package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    _ "github.com/go-sql-driver/mysql"
    "github.com/go-mysql-org/go-mysql/mysql"
    "github.com/go-mysql-org/go-mysql/replication"
    "github.com/joho/godotenv"
)

func main() {
    // 1. Load .env and get credentials
    if err := godotenv.Load(); err != nil {
        log.Fatalf("Error loading .env: %v", err)
    }
    pwd := os.Getenv("MYSQL_REPLICATOR_PASSWORD")
    if pwd == "" {
        log.Fatal("MYSQL_REPLICATOR_PASSWORD not set")
    }

    // 2. Build syncer config (same as before)
    cfg := replication.BinlogSyncerConfig{
        ServerID: 101,
        Flavor:   "mysql",
        Host:     "localhost",
        Port:     3306,
        User:     "repl",
        Password: pwd,
    }
    syncer := replication.NewBinlogSyncer(cfg)

    // 3. Retrieve last GTID set from your checkpoint store
    //    Here we cheat by reading it from a file; you can replace with DB or KV.
    lastGtid, err := os.ReadFile("last_gtid.txt")
    if err != nil {
        log.Printf("No saved GTID found, starting from current master position")
        // fallback: fetch current executed GTID_SET from MySQL
        lastGtid, err = fetchMasterGTID(pwd)
        if err != nil {
            log.Fatalf("Failed to get master GTID: %v", err)
        }
    }
    gtidSet, err := mysql.ParseGTIDSet("mysql", string(lastGtid))
    if err != nil {
        log.Fatalf("Invalid GTID format: %v", err)
    }
    log.Printf("Resuming replication at GTID set: %s", gtidSet.String())

    // 4. Start GTID sync
    streamer, err := syncer.StartSyncGTID(gtidSet)
    if err != nil {
        log.Fatalf("Failed to start GTID sync: %v", err)
    }
    log.Println("GTID-based binlog streamer started...")

    // 5. Graceful shutdown setup
    ctx, cancel := context.WithCancel(context.Background())
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigCh
        log.Println("Shutdown signal received")
        syncer.Close()
        cancel()
    }()

    // 6. Event loop
    for {
        ev, err := streamer.GetEvent(ctx)
        if err != nil {
            if err == context.Canceled {
                return
            }
            log.Fatalf("Error fetching event: %v", err)
        }
        ev.Dump(os.Stdout)
    }
}

// fetchMasterGTID connects to MySQL and reads @@global.gtid_executed
func fetchMasterGTID(password string) ([]byte, error) {
    // Use "repl" user and "127.0.0.1" host for fetching GTID, consistent with binlog syncer config
    dsn := fmt.Sprintf("repl:%s@tcp(localhost:3306)/", password)
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        return nil, err
    }
    defer db.Close()

    var gtid string
    // @@global.gtid_executed shows all GTIDs the master has executed
    err = db.QueryRow("SELECT @@global.gtid_executed").Scan(&gtid)
    if err != nil {
        return nil, err
    }
    return []byte(gtid), nil
}
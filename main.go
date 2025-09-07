package sidb

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"path"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// This package is the Si(mple) DB library.

type Database struct {
	Path       string
	connection *sql.DB
	mutex      sync.RWMutex
}

type EntryInput struct {
	Type  string
	Key   string
	Value []byte
}

type DbEntry struct {
	Id        int64
	Timestamp int64
	Type      string
	Key       string
	Value     []byte
}

func RootPath() string {
	home, err := os.UserHomeDir()

	if err != nil {
		log.Fatal(err)
	}

	return path.Join(home, ".sidb")
}

var ErrNoDbConnection = errors.New("no database connection")

func Init(namespace []string, name string) (*Database, error) {
	dirPath := path.Join(append([]string{RootPath()}, namespace...)...)
	dbPath := path.Join(dirPath, name+".db")

	// Ensure parent directory exists
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	connection, err := sql.Open("sqlite3", dbPath)

	if err != nil {
		return nil, err
	}

	createTableSQL := `CREATE TABLE IF NOT EXISTS entries (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    	"timestamp" INTEGER NOT NULL,
		"type" VARCHAR(255),
		"value" BLOB,
		"key" VARCHAR(255) NOT NULL,
		UNIQUE("type", "key")
	);

		CREATE INDEX IF NOT EXISTS idx_entries_timestamp ON entries(timestamp);
		CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(type);
		CREATE INDEX IF NOT EXISTS idx_entries_type_key ON entries(type, key);
	`

	_, err = connection.Exec(createTableSQL)

	if err != nil {
		connection.Close()
		return nil, err
	}

	mutex := sync.RWMutex{}

	database := &Database{Path: dbPath, connection: connection, mutex: mutex}

	return database, nil
}

func (db *Database) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return nil
	}

	err := db.connection.Close()
	if err != nil {
		return err
	}
	db.connection = nil
	return nil
}

func (db *Database) Get(id int64) (*DbEntry, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return nil, ErrNoDbConnection
	}

	row := db.connection.QueryRow("SELECT id, timestamp, type, value, key FROM entries WHERE id = ?", id)

	var entry DbEntry
	err := row.Scan(&entry.Id, &entry.Timestamp, &entry.Type, &entry.Value, &entry.Key)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No entry found
		} else {
			return nil, err
		}
	}

	return &entry, nil
}

func (db *Database) GetByKey(key string, entryType string) (*DbEntry, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return nil, ErrNoDbConnection
	}

	row := db.connection.QueryRow("SELECT id, timestamp, type, value, key FROM entries WHERE key = ? AND type = ?", key, entryType)
	var entry DbEntry
	err := row.Scan(&entry.Id, &entry.Timestamp, &entry.Type, &entry.Value, &entry.Key)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No entry found
		}
		return nil, err
	}

	return &entry, nil
}

func (db *Database) Put(entry EntryInput) (int64, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return 0, ErrNoDbConnection
	}

	stmt, err := db.connection.Prepare("INSERT OR REPLACE INTO entries(type, value, timestamp, key) VALUES(?, ?, ?, ?)")
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(entry.Type, entry.Value, time.Now().UnixMilli(), entry.Key)
	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (db *Database) Update(entry EntryInput) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	stmt, err := db.connection.Prepare("UPDATE entries SET value = ? WHERE key = ? AND type = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(entry.Value, entry.Key, entry.Type)

	return err
}

func (db *Database) Delete(id int64) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	stmt, err := db.connection.Prepare("DELETE FROM entries WHERE id = ?")
	if err != nil {
		return err
	}

	defer stmt.Close()

	_, err = stmt.Exec(id)
	if err != nil {
		return err
	}

	return nil
}

func (db *Database) DeleteByKey(key string, entryType string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	stmt, err := db.connection.Prepare("DELETE FROM entries WHERE key = ? AND type = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(key, entryType)
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) BulkPutForget(entries []EntryInput) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	tx, err := db.connection.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO entries(type, value, timestamp, key) VALUES(?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, e := range entries {
		if _, err := stmt.Exec(e.Type, e.Value, time.Now().UnixMilli(), e.Key); err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()

	if err != nil {
		return err
	}

	return nil
}

func (db *Database) BulkLoad(limit int) ([]DbEntry, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return nil, ErrNoDbConnection
	}

	rows, err := db.connection.Query("SELECT id, timestamp, type, value, key FROM entries ORDER BY timestamp DESC LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []DbEntry
	for rows.Next() {
		var entry DbEntry
		if err := rows.Scan(&entry.Id, &entry.Timestamp, &entry.Type, &entry.Value, &entry.Key); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (db *Database) Count() (int64, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return 0, ErrNoDbConnection
	}

	row := db.connection.QueryRow("SELECT COUNT(*) FROM entries")

	var count int64
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

type QueryParams struct {
	From       *int64
	To         *int64
	Type       *string
	Limit      *int
	Descending bool
}

func (db *Database) Query(
	params QueryParams,
) ([]DbEntry, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return nil, ErrNoDbConnection
	}

	query := "SELECT id, timestamp, type, value, key FROM entries WHERE 1=1"

	var args []interface{}

	if params.Type != nil {
		query += " AND type = ?"
		args = append(args, *params.Type)
	}

	if params.From != nil {
		query += " AND timestamp >= ?"
		args = append(args, *params.From)
	}

	if params.To != nil {
		query += " AND timestamp <= ?"
		args = append(args, *params.To)
	}

	if params.Descending {
		query += " ORDER BY timestamp DESC"
	} else {
		query += " ORDER BY timestamp ASC"
	}

	if params.Limit != nil {
		query += " LIMIT ?"
		args = append(args, *params.Limit)
	}

	rows, err := db.connection.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []DbEntry
	for rows.Next() {
		var entry DbEntry
		if err := rows.Scan(&entry.Id, &entry.Timestamp, &entry.Type, &entry.Value, &entry.Key); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (db *Database) Drop() error {
	db.Close()

	db.mutex.Lock()
	defer db.mutex.Unlock()

	if err := os.Remove(db.Path); err != nil {
		return err
	}
	return nil
}

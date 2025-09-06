package sidb

import (
	"database/sql"
	"log"
	"os"
	"path"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// This package is the Si(mple) DB library.

type Database struct {
	Path string
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

func Init(namespace []string, name string) (*Database, error) {
	dirPath := path.Join(append([]string{RootPath()}, namespace...)...)
	dbPath := path.Join(dirPath, name+".db")

	// Ensure parent directory exists
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	sqliteDb, err := sql.Open("sqlite3", dbPath)

	if err != nil {
		return nil, err
	}

	defer sqliteDb.Close()

	createTableSQL := `CREATE TABLE IF NOT EXISTS entries (
		"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    	"timestamp" INTEGER NOT NULL,
		"type" VARCHAR(255),
		"value" BLOB,
		"key" VARCHAR(255) NOT NULL UNIQUE
	);

		CREATE INDEX IF NOT EXISTS idx_entries_timestamp ON entries(timestamp);
		CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(type);
		CREATE INDEX IF NOT EXISTS idx_entries_type_key ON entries(type, key);
	`

	_, err = sqliteDb.Exec(createTableSQL)

	if err != nil {
		return nil, err
	}

	database := &Database{Path: dbPath}

	return database, nil
}

func (db *Database) Get(id int64) (*DbEntry, error) {
	database, err := sql.Open("sqlite3", db.Path)

	if err != nil {
		return nil, err
	}
	defer database.Close()

	row := database.QueryRow("SELECT id, timestamp, type, value, key FROM entries WHERE id = ?", id)

	var entry DbEntry
	err = row.Scan(&entry.Id, &entry.Timestamp, &entry.Type, &entry.Value, &entry.Key)
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
	database, err := sql.Open("sqlite3", db.Path)

	if err != nil {
		return nil, err
	}

	defer database.Close()

	row := database.QueryRow("SELECT id, timestamp, type, value, key FROM entries WHERE key = ? AND type = ?", key, entryType)
	var entry DbEntry
	err = row.Scan(&entry.Id, &entry.Timestamp, &entry.Type, &entry.Value, &entry.Key)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No entry found
		}
		return nil, err
	}

	return &entry, nil
}

func (db *Database) Put(entry EntryInput) (int64, error) {
	database, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return 0, err
	}

	defer database.Close()

	stmt, err := database.Prepare("INSERT INTO entries(type, value, timestamp, key) VALUES(?, ?, ?, ?)")
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

func (db *Database) Delete(id int64) error {
	database, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return err
	}
	defer database.Close()

	stmt, err := database.Prepare("DELETE FROM entries WHERE id = ?")
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
	database, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return err
	}
	defer database.Close()

	stmt, err := database.Prepare("DELETE FROM entries WHERE key = ? AND type = ?")
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
	database, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return err
	}
	defer database.Close()

	tx, err := database.Begin()
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
	database, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return nil, err
	}
	defer database.Close()

	rows, err := database.Query("SELECT id, timestamp, type, value, key FROM entries ORDER BY timestamp DESC LIMIT ?", limit)
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
	database, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return 0, err
	}

	defer database.Close()

	row := database.QueryRow("SELECT COUNT(*) FROM entries")

	var count int64
	err = row.Scan(&count)
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
	database, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return nil, err
	}
	defer database.Close()

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

	rows, err := database.Query(query, args...)
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
	if err := os.Remove(db.Path); err != nil {
		return err
	}
	return nil
}

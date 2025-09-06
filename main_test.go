package sidb

import (
	"path"
	"testing"
)

func TestInit(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	expectedDir := path.Join(append([]string{RootPath()}, namespace...)...)
	expectedPath := path.Join(expectedDir, name+".db")
	if db.Path != expectedPath {
		t.Errorf("Expected database path %s, got %s", expectedPath, db.Path)
	}

	err = db.Drop()

	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}
}

func TestBulkPutForget(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	entryType := "test_type"
	data := []byte("test_data")
	err = db.BulkPutForget([]EntryInput{{Type: entryType, Value: data, Key: "test_key"}})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entries, err := db.BulkLoad(10)
	if err != nil {
		t.Fatalf("Failed to load entries: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Type != entryType {
		t.Errorf("Expected entry type %s, got %s", entryType, entries[0].Type)
	}
	if string(entries[0].Value) != string(data) {
		t.Errorf("Expected entry data %s, got %s", string(data), string(entries[0].Value))
	}

	err = db.Drop()

	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}
}

func TestGet(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	entryType := "test_type"
	data := []byte("test_data")
	id, err := db.Put(EntryInput{Type: entryType, Value: data, Key: "test_key"})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entry, err := db.GetById(id)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if entry.Id != id {
		t.Errorf("Expected entry ID %d, got %d", id, entry.Id)
	}
	if entry.Type != entryType {
		t.Errorf("Expected entry type %s, got %s", entryType, entry.Type)
	}
	if string(entry.Value) != string(data) {
		t.Errorf("Expected entry data %s, got %s", string(data), string(entry.Value))
	}

	err = db.Drop()
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}
}

func TestDelete(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	entryType := "test_type"
	data := []byte("test_data")
	id, err := db.Put(EntryInput{Type: entryType, Value: data, Key: "test_key"})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	err = db.Delete(id)
	if err != nil {
		t.Fatalf("Failed to delete entry: %v", err)
	}

	entry, err := db.GetById(id)

	if err != nil {
		t.Fatalf("Error occurred while getting deleted entry: %v", err)
	}

	if entry != nil {
		t.Errorf("Expected nil entry after deletion, got %+v", entry)
	}

	err = db.Drop()
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

}

func TestQuery(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	type_1 := "type_1"
	type_2 := "type_2"

	err = db.BulkPutForget([]EntryInput{
		{Type: type_1, Value: []byte("data_1"), Key: "key_1"},
		{Type: type_2, Value: []byte("data_2"), Key: "key_2"},
		{Type: type_1, Value: []byte("data_3"), Key: "key_3"},
	})

	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entries, err := db.Query(QueryParams{
		Type: &type_1,
	})

	if err != nil {
		t.Fatalf("Failed to query entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	if entries[0].Type != "type_1" || string(entries[0].Value) != "data_1" {
		t.Errorf("Unexpected entry: %+v", entries[0])
	}
	if entries[1].Type != "type_1" || string(entries[1].Value) != "data_3" {
		t.Errorf("Unexpected entry: %+v", entries[1])
	}

	entries, err = db.Query(QueryParams{
		Type: &type_2,
	})

	if err != nil {
		t.Fatalf("Failed to query entries: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Type != "type_2" || string(entries[0].Value) != "data_2" {
		t.Errorf("Unexpected entry: %+v", entries[0])
	}

	err = db.Drop()
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}
}

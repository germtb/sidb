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
	defer db.Drop()
	expectedDir := path.Join(append([]string{RootPath()}, namespace...)...)
	expectedPath := path.Join(expectedDir, name+".db")
	if db.Path != expectedPath {
		t.Errorf("Expected database path %s, got %s", expectedPath, db.Path)
	}
}

func TestBulkPutForget(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	err = db.BulkPutForget([]EntryInput{{Type: entryType, Value: data, Key: "test_key", Grouping: ""}})
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
}

func TestGetById(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	id, err := db.Upsert(EntryInput{Type: entryType, Value: data, Key: "test_key", Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entry, err := db.Get(id)
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
}

func TestGetByKey(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	key := "test_key"
	_, err = db.Upsert(EntryInput{Type: entryType, Value: data, Key: key, Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entry, err := db.GetByKey(key, entryType)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if entry.Key != key {
		t.Errorf("Expected entry key %s, got %s", key, entry.Key)
	}
	if entry.Type != entryType {
		t.Errorf("Expected entry type %s, got %s", entryType, entry.Type)
	}
	if string(entry.Value) != string(data) {
		t.Errorf("Expected entry data %s, got %s", string(data), string(entry.Value))
	}
}

func TestDelete(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	id, err := db.Upsert(EntryInput{Type: entryType, Value: data, Key: "test_key", Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	err = db.Delete(id)
	if err != nil {
		t.Fatalf("Failed to delete entry: %v", err)
	}

	entry, err := db.Get(id)

	if err != nil {
		t.Fatalf("Error occurred while getting deleted entry: %v", err)
	}

	if entry != nil {
		t.Errorf("Expected nil entry after deletion, got %+v", entry)
	}
}

func TestDeleteByKey(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	key := "test_key"
	_, err = db.Upsert(EntryInput{Type: entryType, Value: data, Key: key, Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	err = db.DeleteByKey(key, entryType)
	if err != nil {
		t.Fatalf("Failed to delete entry by key: %v", err)
	}

	entry, err := db.GetByKey(key, entryType)
	if err != nil {
		t.Fatalf("Error occurred while getting deleted entry: %v", err)
	}

	if entry != nil {
		t.Errorf("Expected nil entry after deletion, got %+v", entry)
	}
}

func TestQuery(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	type_1 := "type_1"
	type_2 := "type_2"

	err = db.BulkPutForget([]EntryInput{
		{Type: type_1, Value: []byte("data_1"), Key: "key_1", Grouping: ""},
		{Type: type_2, Value: []byte("data_2"), Key: "key_2", Grouping: ""},
		{Type: type_1, Value: []byte("data_3"), Key: "key_3", Grouping: ""},
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
}

func TestUpdate(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	key := "test_key"
	_, err = db.Upsert(EntryInput{Type: entryType, Value: data, Key: key, Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	newData := []byte("updated_data")
	err = db.Update(EntryInput{Type: entryType, Value: newData, Key: key, Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to update entry: %v", err)
	}

	entry, err := db.GetByKey(key, entryType)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if string(entry.Value) != string(newData) {
		t.Errorf("Expected entry data %s, got %s", string(newData), string(entry.Value))
	}
}

func TestClose(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Attempting to use the database after closing should result in an error
	_, err = db.BulkLoad(10)
	if err == nil {
		t.Fatalf("Expected error when using closed database, got nil")
	}
}

func TestGetByGrouping(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	grouping := "test_group"
	data1 := []byte("test_data_1")
	data2 := []byte("test_data_2")

	_, err = db.Upsert(EntryInput{Type: entryType, Value: data1, Key: "test_key_1", Grouping: grouping})
	if err != nil {
		t.Fatalf("Failed to put entry 1: %v", err)
	}

	_, err = db.Upsert(EntryInput{Type: entryType, Value: data2, Key: "test_key_2", Grouping: grouping})
	if err != nil {
		t.Fatalf("Failed to put entry 2: %v", err)
	}

	entries, err := db.GetByGrouping(grouping, entryType)
	if err != nil {
		t.Fatalf("Failed to get entries by grouping: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	if entries[0].Type != entryType || string(entries[0].Value) != string(data1) {
		t.Errorf("Unexpected entry: %+v", entries[0])
	}
	if entries[1].Type != entryType || string(entries[1].Value) != string(data2) {
		t.Errorf("Unexpected entry: %+v", entries[1])
	}
}

func TestDeleteByGrouping(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	grouping := "test_group"
	data1 := []byte("test_data_1")
	data2 := []byte("test_data_2")
	_, err = db.Upsert(EntryInput{Type: entryType, Value: data1, Key: "test_key_1", Grouping: grouping})
	if err != nil {
		t.Fatalf("Failed to put entry 1: %v", err)
	}
	_, err = db.Upsert(EntryInput{Type: entryType, Value: data2, Key: "test_key_2", Grouping: grouping})
	if err != nil {
		t.Fatalf("Failed to put entry 2: %v", err)
	}

	err = db.DeleteByGrouping(grouping, entryType)
	if err != nil {
		t.Fatalf("Failed to delete entries by grouping: %v", err)
	}

	entries, err := db.GetByGrouping(grouping, entryType)
	if err != nil {
		t.Fatalf("Failed to get entries by grouping: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("Expected 0 entries after deletion, got %d", len(entries))
	}
}

func TestBulkGetByKey(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data1 := []byte("test_data_1")
	data2 := []byte("test_data_2")
	key1 := "test_key_1"
	key2 := "test_key_2"
	nonExistentKey := "non_existent_key"

	_, err = db.Upsert(EntryInput{Type: entryType, Value: data1, Key: key1, Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry 1: %v", err)
	}
	_, err = db.Upsert(EntryInput{Type: entryType, Value: data2, Key: key2, Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry 2: %v", err)
	}

	keys := []string{key1, key2, nonExistentKey}
	entries, err := db.BulkGetByKey(keys, entryType)
	if err != nil {
		t.Fatalf("Failed to bulk get entries by keys: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	if entry, exists := entries[key1]; !exists || entry.Type != entryType || string(entry.Value) != string(data1) {
		t.Errorf("Unexpected or missing entry for key1: %+v", entry)
	}
	if entry, exists := entries[key2]; !exists || entry.Type != entryType || string(entry.Value) != string(data2) {
		t.Errorf("Unexpected or missing entry for key2: %+v", entry)
	}
	if _, exists := entries[nonExistentKey]; exists {
		t.Errorf("Expected no entry for non-existent key, but found one")
	}
}

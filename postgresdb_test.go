package postgreskvdb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresDB_SetSync(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	err = DropTable(db.db)
	require.NoError(t, err)
	// Set up test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Call the SetSync method
	err = db.Set(key, value)
	require.NoError(t, err)

	// Verify that the value is set correctly
	result, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, result)
}

func TestPostgresDB_SetGet(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	err = DropTable(db.db)
	require.NoError(t, err)
	// Set up test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Call the Set method
	err = db.Set(key, value)
	require.NoError(t, err)

	// Call the Get method
	result, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, result)
}

func TestPostgresDB_Delete(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	err = DropTable(db.db)
	require.NoError(t, err)
	// Set up test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Call the Set method
	err = db.Set(key, value)
	require.NoError(t, err)

	// Call the Delete method
	err = db.Delete(key)
	require.NoError(t, err)

	// Verify that the key is deleted
	result, err := db.Get(key)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestPostgresDB_Has(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	err = DropTable(db.db)
	require.NoError(t, err)
	// Set up test data
	key := []byte("test_key")
	value := []byte("test_value")

	// Call the Set method
	err = db.Set(key, value)
	require.NoError(t, err)

	// Call the Has method
	exists, err := db.Has(key)
	require.NoError(t, err)
	assert.True(t, exists)

	// Call the Has method with non-existent key
	exists, err = db.Has([]byte("non_existent_key"))
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestPostgresDB_Iterator(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	err = DropTable(db.db)
	require.NoError(t, err)
	items := []item{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
	}
	for _, item := range items {
		err = db.Set(item.key, item.value)
		require.NoError(t, err)
	}
	// Call the Iterator method
	iter, err := db.Iterator(items[0].key, items[len(items)-1].key)
	require.NoError(t, err)
	defer iter.Close()

	// Iterate over the results and verify
	idx := 0
	for {
		if err := iter.Error(); err != nil {
			iter.Next()
			v := iter.Value()
			k := iter.Key()
			assert.Equal(t, string(items[idx].key), string(k))
			assert.Equal(t, string(items[idx].value), string(v))
			idx++
		} else {
			break
		}
	}
	require.NoError(t, iter.Error())

}
func TestPostgresDB_ReverseIterator(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	err = DropTable(db.db)
	require.NoError(t, err)
	items := []item{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
	}
	for _, item := range items {
		err = db.Set(item.key, item.value)
		require.NoError(t, err)
	}
	// Call the Iterator method
	iter, err := db.ReverseIterator(items[0].key, items[len(items)-1].key)
	require.NoError(t, err)
	defer iter.Close()

	// Iterate over the results and verify
	idx := 3
	for {
		if err := iter.Error(); err != nil {
			iter.Next()
			v := iter.Value()
			k := iter.Key()
			assert.Equal(t, string(items[idx].key), string(k))
			assert.Equal(t, string(items[idx].value), string(v))
			idx--
		} else {
			break
		}
	}
	require.NoError(t, iter.Error())

}

// func TestPostgresDB_BatchWrite(t *testing.T) {
// 	// Create a new instance of PostgresDB
// 	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
// 	require.NoError(t, err)
// 	err = DropTable(db.db)
// 	require.NoError(t, err)
// 	// Call the NewBatch method
// 	batch := db.NewBatch()

// 	// Verify that the returned batch is of type PostgresDBBatch
// 	_, ok := batch.(*PostgresDBBatch)
// 	assert.True(t, ok)
// 	// Test the Set method of the batch
// 	items := []item{
// 		{key: []byte("key1"), value: []byte("value1")},
// 		{key: []byte("key2"), value: []byte("value2")},
// 		{key: []byte("key3"), value: []byte("value3")},
// 		{key: []byte("key4"), value: []byte("value4")},
// 	}
// 	for _, item := range items {
// 		err = batch.Set(item.key, item.value)
// 		require.NoError(t, err)
// 	}

// 	// Test the Delete method of the batch
// 	err = batch.Delete(items[0].key)
// 	require.NoError(t, err)

// 	// Test the Write method of the batch
// 	err = batch.Write()
// 	require.NoError(t, err)

//		// Test the Close method of the batch
//		err = batch.Close()
//		require.NoError(t, err)
//	}
func DropTable(db *pgx.Conn) error {
	_, err := db.Exec(context.Background(), "TRUNCATE TABLE kv_store;")
	return err
}
func TestPostgresDB_BatchWrite(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	defer db.Close()

	// Drop existing table to start fresh
	err = DropTable(db.db)
	require.NoError(t, err)

	// Call the NewBatch method
	batch := db.NewBatch()

	// Verify that the returned batch is of type PostgresDBBatch
	_, ok := batch.(*PostgresDBBatch)
	assert.True(t, ok)

	// Test the Set method of the batch
	items := []item{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
	}
	for _, item := range items {
		err = batch.Set(item.key, item.value)
		require.NoError(t, err)
	}

	// Test the Write method of the batch
	err = batch.Write()
	require.NoError(t, err)

	// Test the Close method of the batch
	err = batch.Close()
	require.NoError(t, err)

	// Retrieve values from the database to verify correctness
	for _, item := range items {
		value, err := GetValueFromDB(db.db, item.key)
		require.NoError(t, err)
		assert.Equal(t, item.value, value)
	}
}

func TestPostgresDB_BatchWrite_Concurrent(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	defer db.Close()

	// Drop existing table to start fresh
	err = DropTable(db.db)
	require.NoError(t, err)

	// Call the NewBatch method
	batch := db.NewBatch()

	// Verify that the returned batch is of type PostgresDBBatch
	_, ok := batch.(*PostgresDBBatch)
	assert.True(t, ok)

	// Define concurrent writer function
	concurrentWriter := func(batch Batch, key, value []byte) {
		err := batch.Set(key, value)
		require.NoError(t, err)
	}

	// Define concurrent delete function
	concurrentDelete := func(batch Batch, key []byte) {
		err := batch.Delete(key)
		require.NoError(t, err)
	}

	// Execute concurrent write and delete operations
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key%d", index))
			value := []byte(fmt.Sprintf("value%d", index))
			if index%2 == 0 {
				concurrentWriter(batch, key, value)
			} else {
				concurrentDelete(batch, key)
			}
		}(i)
	}
	wg.Wait()

	// Test the Write method of the batch
	err = batch.Write()
	require.NoError(t, err)

	// Test the Close method of the batch
	err = batch.Close()
	require.NoError(t, err)
}

func GetValueFromDB(db *pgx.Conn, key []byte) ([]byte, error) {
	var value []byte
	err := db.QueryRow(context.Background(), "SELECT value FROM kv_store WHERE key = $1", key).Scan(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}
func TestPostgresDB_BatchWrite_ConcurrentWritesProtected(t *testing.T) {
	// Create a new instance of PostgresDB
	db, err := NewPostgresDB("indexer:indexer123@localhost:5433", "indexer")
	require.NoError(t, err)
	defer db.Close()

	// Drop existing table to start fresh
	err = DropTable(db.db)
	require.NoError(t, err)

	// Call the NewBatch method
	batch := db.NewBatch()

	// Verify that the returned batch is of type PostgresDBBatch
	_, ok := batch.(*PostgresDBBatch)
	assert.True(t, ok)

	// Number of concurrent goroutines
	numGoroutines := 10

	// Channel to signal completion of goroutines
	done := make(chan struct{})
	defer close(done)

	// Function to perform concurrent writes
	concurrentWriter := func(batch Batch, keyPrefix string) {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("%s%d", keyPrefix, i))
			value := []byte(fmt.Sprintf("value%d", i))

			// Attempt to perform write operation
			err := batch.Set(key, value)
			require.NoError(t, err)
		}
		done <- struct{}{}
	}

	// Start concurrent writer goroutines
	for i := 0; i < numGoroutines; i++ {
		go concurrentWriter(batch, fmt.Sprintf("key%d_", i))
	}

	// Wait for all goroutines to finish
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Test the Write method of the batch
	err = batch.Write()
	require.NoError(t, err)

	// Test the Close method of the batch
	err = batch.Close()
	require.NoError(t, err)
}

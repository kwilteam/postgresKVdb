package postgreskvdb

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type postgresDBBatch struct {
	conn    *pgx.Conn
	queries []queriesBatch
}
type queriesBatch struct {
	key, value []byte
	operator   opType
}

var _ Batch = (*postgresDBBatch)(nil)

func NewPostgresDBBatch(conn *pgx.Conn) *postgresDBBatch {
	return &postgresDBBatch{
		conn:    conn,
		queries: make([]queriesBatch, 0),
	}
}

// Set implements Batch.
func (b *postgresDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.queries == nil {
		return errBatchClosed
	}
	b.queries = append(b.queries, queriesBatch{key: key, value: value, operator: opTypeSet})
	return nil
}

// Delete implements Batch.
func (b *postgresDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.queries == nil {
		return errBatchClosed
	}
	b.queries = append(b.queries, queriesBatch{key: key, operator: opTypeDelete})
	return nil
}

// Write implements Batch.
func (b *postgresDBBatch) Write() error {
	if b.queries == nil {
		return errBatchClosed
	}
	batch := &pgx.Batch{}
	for _, value := range b.queries {
		switch value.operator {
		case opTypeSet:
			id, err := generateUUIDv5FromBytes(value.key)
			if err != nil {
				return err
			}
			query := "INSERT INTO kv_store (id,key, value) VALUES ($1, $2,$3) ON CONFLICT (key) DO UPDATE SET value = $3;"
			batch.Queue(query, id, value.key, value.value)
		case opTypeDelete:
			query := "DELETE FROM kv_store WHERE key = $1;"
			batch.Queue(query, value.key)
		default:
			return fmt.Errorf("unknown operation type %v (%v)", value.operator, value)
		}
	}

	br := b.conn.SendBatch(context.Background(), batch)
	defer br.Close()

	_, err := br.Exec()
	if err != nil {
		return err
	}
	err = br.Close()
	if err != nil {
		return err
	}
	return b.Close()
}

// WriteSync implements Batch.
func (b *postgresDBBatch) WriteSync() error {
	return b.Write()
}

// Close is a no-op for PostgreSQL batch.
func (b *postgresDBBatch) Close() error {
	// Clear queries for reusability
	b.queries = nil
	return nil
}

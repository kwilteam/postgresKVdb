package postgreskvdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
)

type PostgresDBBatch struct {
	conn    *pgx.Conn
	queries []queryBatch
	mtx     sync.Mutex
}

type queryBatch struct {
	key, value []byte
	operator   opType
}

var _ Batch = (*PostgresDBBatch)(nil)

func NewPostgresDBBatch(conn *pgx.Conn) *PostgresDBBatch {
	return &PostgresDBBatch{
		conn:    conn,
		queries: make([]queryBatch, 0),
	}
}

func (b *PostgresDBBatch) Set(key, value []byte) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.queries == nil {
		return errBatchClosed
	}
	b.queries = append(b.queries, queryBatch{key: key, value: value, operator: opTypeSet})
	return nil
}

func (b *PostgresDBBatch) Delete(key []byte) error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.queries == nil {
		return errBatchClosed
	}
	b.queries = append(b.queries, queryBatch{key: key, operator: opTypeDelete})
	return nil
}

func (b *PostgresDBBatch) Write() error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

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
	return nil
}

func (b *PostgresDBBatch) WriteSync() error {
	return b.Write()
}

func (b *PostgresDBBatch) Close() error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	// Clear queries for reusability
	b.queries = nil
	return nil
}

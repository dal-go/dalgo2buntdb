package dalgo2buntdb

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo2buntdb/testing4buntdb"
	"github.com/tidwall/buntdb"
	"testing"
)

func TestInserter_Insert(t *testing.T) {
	bdb, err := buntdb.Open(testing4buntdb.MemoryPath)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	ctx := context.Background()
	key := dal.NewKeyWithID("TestKind", "test-id")
	data := new(testKind)
	record := dal.NewRecordWithData(key, data)
	db := NewDatabase(bdb)
	err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, record)
	})
	if err != nil {
		t.Errorf("expected to be successful, got error: %v", err)
	}
	if err := bdb.View(func(tx *buntdb.Tx) error {
		const id = "TestKind/test-id"
		if _, err := tx.Get(id); err != nil {
			t.Errorf("Inserted record is not found by ID: " + id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

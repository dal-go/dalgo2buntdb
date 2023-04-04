package dalgo2buntdb

import (
	"context"
	"github.com/strongo/dalgo/dal"
	"github.com/tidwall/buntdb"
	"testing"
)

type testKind struct {
	Str string
	Int int
}

func TestGetter_Get(t *testing.T) {
	ctx := context.Background()

	const k = "TestKind/test_1"
	db := openInMemoryDB(t)
	if err := db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(k, `{"Str":"s1", "Int":1}`, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	ddb := NewDatabase(db)

	key := dal.NewKeyWithID("TestKind", "test_1")
	data := new(testKind)
	record := dal.NewRecordWithData(key, data)
	err := ddb.Get(ctx, record)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if data.Str != "s1" {
		t.Errorf("expected 's1' for Str property, got: %v", data.Str)
	}
	if data.Int != 1 {
		t.Errorf("expected 1 for Int property, got: %v", data.Int)
	}
}

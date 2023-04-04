package dalgo2buntdb

import (
	"context"
	"github.com/strongo/dalgo/dal"
	"github.com/tidwall/buntdb"
	"testing"
)

func TestDeleter_Delete(t *testing.T) {
	db, err := buntdb.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx *buntdb.Tx) error {
		_, _, err = tx.Set("Test/t1", "", nil)
		return err
	})

	if err != nil {
		t.Fatal(err)
	}
	ddb := database{
		db: db,
	}

	ctx := context.Background()

	err = ddb.Delete(ctx, dal.NewKeyWithID("Test", "t1"))
	if err != nil {
		t.Errorf("failed to performa delete operation: %v", err)
	}
}

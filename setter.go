package dalgo2buntdb

import (
	"context"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"github.com/tidwall/buntdb"
)

func (dtb database) Set(ctx context.Context, record dal.Record) error {
	return dtb.db.Update(func(tx *buntdb.Tx) error {
		return transaction{tx: tx}.Set(ctx, record)
	})
}

func (dtb database) SetMulti(ctx context.Context, records []dal.Record) error {
	return dtb.db.Update(func(tx *buntdb.Tx) error {
		return transaction{tx: tx}.SetMulti(ctx, records)
	})
}

func (t transaction) Set(_ context.Context, record dal.Record) error {
	key := record.Key()
	k := key.String()
	record.SetError(nil)
	data := record.Data()
	s, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, _, err = t.tx.Set(k, string(s), nil)
	return err
}

func (t transaction) SetMulti(ctx context.Context, records []dal.Record) error {
	for _, record := range records {
		if err := t.Set(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

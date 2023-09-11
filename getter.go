package dalgo2buntdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/tidwall/buntdb"
)

func (dtb database) Get(ctx context.Context, record dal.Record) error {
	return dtb.db.View(func(tx *buntdb.Tx) error {
		return transaction{tx: tx}.Get(ctx, record)
	})
}

func (dtb database) GetMulti(ctx context.Context, records []dal.Record) error {
	return dtb.db.View(func(tx *buntdb.Tx) error {
		return transaction{tx: tx}.GetMulti(ctx, records)
	})
}

func (t transaction) Get(_ context.Context, record dal.Record) error {
	key := record.Key()
	keyPath := key.String()
	s, err := t.tx.Get(keyPath)
	if err == nil {
		record.SetError(nil)
	} else {
		if errors.Is(err, buntdb.ErrNotFound) {
			err = dal.NewErrNotFoundByKey(key, err)
			record.SetError(err)
		}
		return err
	}
	record.SetError(nil)
	data := record.Data()
	if data == nil {
		record.SetError(errors.New("no target data object has been provided for unmarshalling"))
		return nil
	}
	err = json.Unmarshal([]byte(s), data)
	if err != nil {
		err = fmt.Errorf("failed to unmarshall record data into target of type %T: %w", data, err)
		record.SetError(err)
		return err
	}
	return nil
}

func (t transaction) GetMulti(ctx context.Context, records []dal.Record) error {
	for _, record := range records {
		key := record.Key()
		keyPath := key.String()
		s, err := t.tx.Get(keyPath)
		if errors.Is(err, buntdb.ErrNotFound) {
			record.SetError(dal.NewErrNotFoundByKey(key, err))
			continue
		} else if err != nil {
			record.SetError(err)
			return err
		}
		record.SetError(nil)
		data := record.Data()
		if data == nil {
			panic("record.Data() returned null")
			//record.SetDataTo(func(target interface{}) error {
			//	return json.Unmarshal([]byte(s), target)
			//})
		}
		if err = json.Unmarshal([]byte(s), data); err != nil {
			record.SetError(err)
		}
	}
	return nil
}

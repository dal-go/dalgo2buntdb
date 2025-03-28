package dalgo2buntdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
	"github.com/tidwall/buntdb"
)

func (dtb database) Update(
	ctx context.Context,
	key *dal.Key,
	updates []update.Update,
	preconditions ...dal.Precondition,
) error {
	return dtb.db.Update(func(tx *buntdb.Tx) error {
		return transaction{tx: tx}.Update(ctx, key, updates, preconditions...)
	})
}

func (dtb database) UpdateMulti(
	ctx context.Context,
	keys []*dal.Key,
	updates []update.Update,
	preconditions ...dal.Precondition,
) error {
	return dtb.db.Update(func(tx *buntdb.Tx) error {
		return transaction{tx: tx}.UpdateMulti(ctx, keys, updates, preconditions...)
	})
}

func (t transaction) Update(
	ctx context.Context,
	key *dal.Key,
	updates []update.Update,
	preconditions ...dal.Precondition,
) error {
	return t.update(ctx, key, updates, preconditions...)
}

func (t transaction) UpdateMulti(
	ctx context.Context,
	keys []*dal.Key,
	updates []update.Update,
	preconditions ...dal.Precondition,
) error {
	for _, key := range keys {
		if err := t.update(ctx, key, updates, preconditions...); err != nil {
			return err
		}
	}
	return nil
}

func (t transaction) update(
	_ context.Context,
	key *dal.Key,
	updates []update.Update,
	preconditions ...dal.Precondition,
) error {
	k := key.String()
	s, err := t.tx.Get(k)
	if err != nil {
		if errors.Is(err, buntdb.ErrNotFound) {
			err = dal.NewErrNotFoundByKey(key, err)
		}
		return err
	}
	data := make(map[string]interface{})
	if err = json.Unmarshal([]byte(s), &data); err != nil {
		return fmt.Errorf("failed to unmarshal data as JSON object: %v", err)
	}
	for _, u := range updates {
		if fieldName := u.FieldName(); fieldName != "" {
			data[fieldName] = u.Value()
		}
	}
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data as JSON object: %v", err)
	}
	_, _, err = t.tx.Set(k, string(b), nil)
	if err != nil {
		return err
	}
	return nil
}

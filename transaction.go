package dalgo2buntdb

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
	"github.com/tidwall/buntdb"
)

func (dtb database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
	return dtb.db.View(func(tx *buntdb.Tx) error {
		return f(ctx, transaction{tx: tx, options: dal.NewTransactionOptions(options...)})
	})
}

func (dtb database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
	return dtb.db.Update(func(tx *buntdb.Tx) (err error) {
		return f(ctx, transaction{tx: tx, options: dal.NewTransactionOptions(options...)})
		// NOTE: managed tx rollback not allowed by buntdb
		//if err != nil {
		//	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		//		return fmt.Errorf("failed to rollbacktransaction: %v: original error: %w", rollbackErr, err)
		//	}
		//}
		//return tx.Commit()
	})
}

var _ dal.ReadwriteTransaction = (*transaction)(nil)

type transaction struct {
	tx      *buntdb.Tx
	options dal.TransactionOptions
}

func (t transaction) Exists(ctx context.Context, key *dal.Key) (bool, error) {
	r := dal.NewRecordWithData(key, &struct{}{})
	if err := t.Get(ctx, r); err != nil {
		if dal.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (t transaction) UpdateRecord(ctx context.Context, record dal.Record, updates []update.Update, preconditions ...dal.Precondition) error {
	return t.Update(ctx, record.Key(), updates, preconditions...)
}

func (t transaction) QueryReader(c context.Context, query dal.Query) (dal.Reader, error) {
	return getReader(t.tx, query)
}

func (t transaction) QueryAllRecords(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
	var reader buntdbReader
	if reader, err = getReader(t.tx, query); err != nil {
		return
	}
	limit := dal.WithLimit(query.Limit())
	return dal.SelectAllRecords(reader, limit)
}

func (t transaction) ID() string {
	return ""
}

func (t transaction) Options() dal.TransactionOptions {
	return t.options
}

func (t transaction) Upsert(ctx context.Context, record dal.Record) error {
	return t.Set(ctx, record)
}

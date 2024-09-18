package generic

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

// RowIterator includes generic methods for a row iterator.
type RowIterator interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// Execute is used for sql queries that don't have any return values.
func Execute[P types.AnyPool](ctx context.Context, db P, query string, args ...any) error {
	var err error
	switch t := any(db).(type) {
	case *types.SourcePool:
		_, err = t.ExecContext(ctx, query, args...)
	case *types.StagingPool:
		_, err = t.Exec(ctx, query, args...)
	case *types.TargetPool:
		_, err = t.ExecContext(ctx, query, args...)
	default:
		err = fmt.Errorf("unimplemented %T", t)
	}
	return err
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func Query[P types.AnyPool](ctx context.Context, db P, query string, args ...any) (any, error) {
	var err error
	var res any

	switch t := any(db).(type) {
	case *types.SourcePool:
		res, err = t.QueryContext(ctx, query, args...)
	case *types.StagingPool:
		res, err = t.Query(ctx, query, args...)
	case *types.TargetPool:
		res, err = t.QueryContext(ctx, query, args...)
	default:
		err = fmt.Errorf("unimplemented %T", t)
	}
	return res, err
}

// QueryRow executes a query that is expected to return at most one row.
func QueryRow[P types.AnyPool](ctx context.Context, db P, query string, args ...any) (any, error) {
	var err error
	var res any

	switch t := any(db).(type) {
	case *types.SourcePool:
		res = t.QueryRowContext(ctx, query, args...)
	case *types.StagingPool:
		res = t.QueryRow(ctx, query, args...)
	case *types.TargetPool:
		res = t.QueryRowContext(ctx, query, args...)
	default:
		err = fmt.Errorf("unimplemented %T", t)
	}
	return res, err
}

// QueryWithDynamicRes runs a query and scan it into a 2D array with
// {#rows} x {#cols}. The size of the 2D arrays is completed determined
// by the result of the query.
func QueryWithDynamicRes[P types.AnyPool](
	ctx context.Context, db P, query string, args ...any,
) ([][]any, error) {
	var err error
	res := make([][]any, 0)

	var columnCnt int
	var rows RowIterator
	defer func() {
		if rows != nil {
			switch r := rows.(type) {
			case *sql.Rows:
				if r != nil {
					r.Close()
				}
			case pgx.Rows:
				if r != nil {
					r.Close()
				}
			default:
				panic(fmt.Sprintf("unimpemented %T", r))
			}
		}
	}()
	switch t := any(db).(type) {
	case *types.SourcePool:
		rows, err = t.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		columns, err := rows.(*sql.Rows).Columns()
		if err != nil {
			return nil, err
		}
		columnCnt = len(columns)
	case *types.StagingPool:
		rows, err = t.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		columnCnt = len(rows.(pgx.Rows).FieldDescriptions())
	case *types.TargetPool:
		rows, err = t.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		columns, err := rows.(*sql.Rows).Columns()
		if err != nil {
			return nil, err
		}
		columnCnt = len(columns)
	default:
		return nil, errors.Errorf("unimplemented %T", t)
	}

	for rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, errors.Wrapf(err, "failed to get dynamic results from reading sql rows")
		}
		values := make([]any, columnCnt)
		valuePtrs := make([]any, columnCnt)

		// Assign the pointers to each interface{} for scanning.
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the value pointers.
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		res = append(res, values)
	}

	return res, err
}

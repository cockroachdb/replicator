package mutation

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4/pgxpool"
)

type factory struct {
	db        *pgxpool.Pool
	stagingDB ident.Ident

	mu struct {
		sync.RWMutex
		instances map[ident.Table]*store
	}
}

var _ types.MutationStores = (*factory)(nil)

// New returns an instance of types.MutationStores that stores
// temporary data in the given SQL database.
func New(db *pgxpool.Pool, stagingDB ident.Ident) types.MutationStores {
	f := &factory{
		db:        db,
		stagingDB: stagingDB,
	}
	f.mu.instances = make(map[ident.Table]*store)
	return f
}

// Get returns a memorized instance of a store for the given table.
func (f *factory) Get(ctx context.Context, target ident.Table) (types.MutationStore, error) {
	if ret := f.getUnlocked(target); ret != nil {
		return ret, nil
	}
	return f.createUnlocked(ctx, target)
}

func (f *factory) createUnlocked(ctx context.Context, table ident.Table) (*store, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances[table]; ret != nil {
		return ret, nil
	}

	ret, err := newStore(ctx, f.db, f.stagingDB, table)
	if err == nil {
		f.mu.instances[table] = ret
	}
	return ret, err
}

func (f *factory) getUnlocked(table ident.Table) *store {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances[table]
}

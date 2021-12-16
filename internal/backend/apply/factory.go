package apply

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// factory vends singleton instance of apply.
type factory struct {
	watchers types.Watchers
	mu       struct {
		sync.RWMutex
		cleanup   []func()
		instances map[ident.Table]*apply
	}
}

var _ types.Appliers = (*factory)(nil)

// New returns an instance of types.Appliers.
func New(watchers types.Watchers) (_ types.Appliers, cancel func()) {
	f := &factory{watchers: watchers}
	f.mu.instances = make(map[ident.Table]*apply)
	return f, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, fn := range f.mu.cleanup {
			fn()
		}
		f.mu.cleanup = nil
		f.mu.instances = nil
	}
}

// Get returns a memoized instance of the Applier for the table.
func (f *factory) Get(
	ctx context.Context, table ident.Table,
) (types.Applier, error) {
	if ret := f.getUnlocked(table); ret != nil {
		return ret, nil
	}
	return f.createUnlocked(ctx, table)
}

func (f *factory) createUnlocked(
	ctx context.Context, table ident.Table,
) (*apply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances[table]; ret != nil {
		return ret, nil
	}
	watcher, err := f.watchers.Get(ctx, table.Database())
	if err != nil {
		return nil, err
	}
	ret, cancel, err := newApply(watcher, table)
	if err == nil {
		f.mu.cleanup = append(f.mu.cleanup, cancel)
		f.mu.instances[table] = ret
	}
	return ret, err
}

func (f *factory) getUnlocked(table ident.Table) *apply {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances[table]
}

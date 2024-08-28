package schemawatch

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/staging/memo"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestBackup_BackupRestore(t *testing.T) {
	b := &memoBackup{
		memo:        &memo.Memory{},
		stagingPool: nil,
	}

	schema := ident.MustSchema(ident.New("test"))
	schemaData := &types.SchemaData{Order: make([][]ident.Table, 3)}

	ctx := stopper.Background()
	r := require.New(t)

	err := b.backup(ctx, schema, schemaData)
	r.NoError(err)

	restored, err := b.restore(ctx, schema)
	r.NoError(err)
	r.Equal(restored, schemaData)
}

func TestBackup_Update(t *testing.T) {
	b := &memoBackup{
		memo:        &memo.Memory{},
		stagingPool: nil,
	}

	schema := ident.MustSchema(ident.New("test"))
	schemaData := &types.SchemaData{Order: make([][]ident.Table, 3)}
	schemaData2 := &types.SchemaData{Order: make([][]ident.Table, 2)}

	ctx := stopper.WithContext(context.Background())
	defer ctx.Stop(0)
	r := require.New(t)

	err := b.backup(ctx, schema, schemaData)
	r.NoError(err)

	nv := notify.VarOf(schemaData)
	<-b.startUpdates(ctx, nv, schema)

	nv.Set(schemaData2)

	// await the backup call
	time.Sleep(100 * time.Millisecond)

	restored, err := b.restore(ctx, schema)
	r.NoError(err)
	r.Equal(schemaData2, restored)
}

package objstore

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/replicator/internal/types"
)

// state is used to persist the last entry we processed from the object storage.
type state struct {
	memo types.Memo
	key  string
}

// valuePayload represent the last entry processed from the object storage.
type valuePayload struct {
	Position string
}

// getLast retrieves the last entry processed from the object storage.
func (s *state) getLast(ctx context.Context, tx types.StagingQuerier) (string, error) {
	v, err := s.memo.Get(ctx, tx, s.key)
	if err != nil {
		return "", err
	}
	value := valuePayload{}
	if v != nil {
		err = json.Unmarshal(v, &value)
		if err != nil {
			return "", err
		}
	}
	return value.Position, nil
}

// setLast stores the last entry processed from the object storage.
func (s *state) setLast(ctx context.Context, tx types.StagingQuerier, last string) error {
	value, err := json.Marshal(valuePayload{Position: last})
	if err != nil {
		return err
	}
	return s.memo.Put(ctx, tx, s.key, value)
}

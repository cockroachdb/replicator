package cdc

import (
	"context"
	"encoding/json"
	"io"
	"regexp"

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/pkg/errors"
)

var (
	webhookRegex        = regexp.MustCompile(`^/(?P<targetDB>[^/]+)/(?P<targetSchema>[^/]+)$`)
	webhookTargetDB     = webhookRegex.SubexpIndex("targetDB")
	webhookTargetSchema = webhookRegex.SubexpIndex("targetSchema")
)

type webhookURL struct {
	target ident.Schema
}

func parseWebhookURL(url string) (webhookURL, error) {
	match := webhookRegex.FindStringSubmatch(url)
	if match == nil {
		return webhookURL{}, errors.Errorf("can't parse url %s", url)
	}

	db := ident.New(match[webhookTargetDB])
	schema := ident.New(match[webhookTargetSchema])

	return webhookURL{ident.NewSchema(db, schema)}, nil
}

// webhook responds to the v21.2 webhook scheme.
// https://www.cockroachlabs.com/docs/stable/create-changefeed.html#responses
func (h *Handler) webhook(ctx context.Context, u webhookURL, r io.Reader) error {
	var payload struct {
		Payload []struct {
			After   json.RawMessage `json:"after"`
			Key     json.RawMessage `json:"key"`
			Topic   string          `json:"topic"`
			Updated string          `json:"updated"`
		} `json:"payload"`
		Length   int    `json:"length"`
		Resolved string `json:"resolved"`
	}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return errors.Wrap(err, "could not decode payload")
	}

	if payload.Resolved != "" {
		timestamp, err := hlc.Parse(payload.Resolved)
		if err != nil {
			return err
		}

		return h.resolved(ctx, resolvedURL{
			target:    u.target,
			timestamp: timestamp,
		})
	}

	// First, we'll aggregate the mutations by target table. We know
	// that the default batch size for webhooks is reasonable.
	toProcess := make(map[ident.Table][]sinktypes.Mutation)

	for i := range payload.Payload {
		timestamp, err := hlc.Parse(payload.Payload[i].Updated)
		if err != nil {
			return err
		}

		target, _, err := ident.Relative(
			u.target.Database(), u.target.Schema(), payload.Payload[i].Topic)
		if err != nil {
			return err
		}

		mut := sinktypes.Mutation{
			Data: payload.Payload[i].After,
			Key:  payload.Payload[i].Key,
			Time: timestamp,
		}

		toProcess[target] = append(toProcess[target], mut)
	}

	return retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := h.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		for target, muts := range toProcess {
			if h.Immediate {
				applier, err := h.Appliers.Get(ctx, target)
				if err != nil {
					return err
				}
				if err := applier.Apply(ctx, tx, muts); err != nil {
					return err
				}
			} else {
				store, err := h.Stores.Get(ctx, target)
				if err != nil {
					return err
				}
				if err := store.Store(ctx, tx, muts); err != nil {
					return err
				}
			}
		}

		return tx.Commit(ctx)
	})
}

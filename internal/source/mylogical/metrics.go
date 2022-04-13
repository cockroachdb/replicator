package mylogical

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	dialFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mylogical_dial_failure_total",
		Help: "the number of times we failed to create a replication connection",
	})
	dialSuccessCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mylogical_dial_success_total",
		Help: "the number of times we successfully dialed a replication connection",
	})
	mutationCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mutation_total",
			Help: "Total number of mutations by type.",
		},
		[]string{"type"},
	)
)

package main

import (
	"time"

	"go.uber.org/atomic"
	log "github.com/sirupsen/logrus"
)

type Status struct {
	deletedCount   atomic.Uint64
	errorCount     atomic.Uint64
	lastDeletedKey atomic.String
}

func (s *Status) Update(addDeleted int, lastKey string) {
	if addDeleted > 0 {
		s.deletedCount.Add(uint64(addDeleted))
	}
	s.lastDeletedKey.Store(lastKey)
}

func (s *Status) IncrementErrors() uint64 {
	return s.errorCount.Inc()
}

func (s *Status) Display(period time.Duration, done <-chan bool) {
	ticker := time.NewTicker(period)
	keepGoing := true

	log.WithFields(log.Fields{
		"deleted": s.deletedCount.Load(),
		"errors": s.errorCount.Load(),
		"last_object": s.lastDeletedKey.Load(),
	}).Info("Status")

	var oldDelete, oldErr uint64

	for keepGoing {
		select {
		case <-ticker.C:
			newDelete, newErr := s.deletedCount.Load(), s.errorCount.Load()
			if newDelete == oldDelete && newErr == oldErr {
				continue
			}
			log.WithFields(log.Fields{
				"deleted":     newDelete,
				"errors":      newErr,
				"last_object": s.lastDeletedKey.Load(),
			}).Info("Status")
			oldDelete, oldErr = newDelete, newErr

		case <-done:
			ticker.Stop()
			keepGoing = false
		}
	}
	log.WithFields(log.Fields{
		"deleted": s.deletedCount.Load(),
		"errors": s.errorCount.Load(),
		"last_object": s.lastDeletedKey.Load(),
	}).Info("Done")
}

package cognitive

import "sync"

// LTPConfig configures Long-Term Potentiation behavior for the Hebbian worker.
// When nil, LTP is disabled and all behavior is unchanged (backward compatible).
type LTPConfig struct {
	// Threshold is the co-activation count at which an association becomes potentiated.
	// 0 = disabled.
	Threshold int
	// DecayFactor is the reduced decay multiplier for potentiated associations.
	// E.g., 0.5 means potentiated edges decay at half the normal rate.
	// 0 = disabled (no decay reduction).
	DecayFactor float64
	// WeightFloor is the minimum weight for potentiated associations.
	// The Hebbian worker enforces this floor during weight updates.
	// 0 = disabled (no floor enforcement).
	WeightFloor float32
}

// ltpState tracks per-workspace per-pair potentiation status in memory.
// The authoritative co-activation count is in the storage layer (CoActivationCount);
// this is a session-local cache for fast lookups during processBatch.
type ltpState struct {
	mu          sync.RWMutex
	potentiated map[ltpKey]struct{} // set of potentiated pairs
	counts      map[ltpKey]uint32   // session-local co-activation count tracker
}

// ltpKey is a composite key of workspace + canonical pair.
type ltpKey struct {
	ws   [8]byte
	pair pairKey
}

func newLTPState() *ltpState {
	return &ltpState{
		potentiated: make(map[ltpKey]struct{}),
		counts:      make(map[ltpKey]uint32),
	}
}

// addCount increments the session-local count for a pair and returns whether
// the pair has become potentiated (count crossed the threshold in this call).
func (s *ltpState) addCount(ws [8]byte, pair pairKey, delta uint32, threshold int) bool {
	if threshold <= 0 {
		return false
	}
	key := ltpKey{ws: ws, pair: pair}
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.counts[key]
	newCount := old + delta
	// Saturation
	if newCount < old {
		newCount = ^uint32(0)
	}
	s.counts[key] = newCount

	if _, already := s.potentiated[key]; already {
		return false // was already potentiated
	}
	if newCount >= uint32(threshold) {
		s.potentiated[key] = struct{}{}
		return true // newly potentiated
	}
	return false
}

// isPotentiated checks if a pair is potentiated.
func (s *ltpState) isPotentiated(ws [8]byte, pair pairKey) bool {
	key := ltpKey{ws: ws, pair: pair}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.potentiated[key]
	return ok
}

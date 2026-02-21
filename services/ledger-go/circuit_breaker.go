package main

import (
	"errors"
	"sync"
	"time"
)

type BreakerState string

const (
	StateClosed   BreakerState = "closed"
	StateOpen     BreakerState = "open"
	StateHalfOpen BreakerState = "half_open"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type CircuitBreaker struct {
	mu sync.Mutex

	state BreakerState

	failures         int
	failureThreshold int

	openTimeout   time.Duration
	lastStateTime time.Time

	halfOpenInFlight bool
}

func NewCircuitBreaker(failureThreshold int, openTimeout time.Duration) *CircuitBreaker {
	cb := &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: failureThreshold,
		openTimeout:      openTimeout,
		lastStateTime:    time.Now(),
	}

	// initialize metric state
	KafkaCircuitBreakerState.Set(0)

	return cb
}

func (cb *CircuitBreaker) State() BreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
	if err := cb.beforeCall(); err != nil {
		return err
	}

	err := fn()
	cb.afterCall(err)
	return err
}

func (cb *CircuitBreaker) beforeCall() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateOpen:
		// Cooldown finished → move to half-open
		if time.Since(cb.lastStateTime) >= cb.openTimeout {
			cb.state = StateHalfOpen
			cb.halfOpenInFlight = false
			cb.lastStateTime = time.Now()

			KafkaCircuitBreakerState.Set(2)
		} else {
			return ErrCircuitOpen
		}
	}

	if cb.state == StateHalfOpen {
		if cb.halfOpenInFlight {
			return ErrCircuitOpen
		}
		cb.halfOpenInFlight = true
	}

	return nil
}

func (cb *CircuitBreaker) afterCall(callErr error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {

	case StateClosed:
		if callErr == nil {
			cb.failures = 0
			return
		}

		cb.failures++
		if cb.failures >= cb.failureThreshold {
			cb.tripOpenLocked()
		}

	case StateHalfOpen:
		cb.halfOpenInFlight = false

		if callErr == nil {
			// Success → close breaker
			cb.state = StateClosed
			cb.failures = 0
			cb.lastStateTime = time.Now()

			KafkaCircuitBreakerState.Set(0)
			return
		}

		// Failure → reopen
		cb.tripOpenLocked()

	case StateOpen:
		// No-op
	}
}

func (cb *CircuitBreaker) tripOpenLocked() {
	cb.state = StateOpen
	cb.lastStateTime = time.Now()

	KafkaCircuitBreakerOpenTotal.Inc()
	KafkaCircuitBreakerState.Set(1)
}

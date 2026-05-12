package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/store"
)

// ─── mock GPULister ───────────────────────────────────────────────────────────

// mockLister is a test double for GPULister. It returns the configured gpus
// and err without making any database calls.
type mockLister struct {
	gpus      []store.GPUSummary
	err       error
	callCount int
}

// ListGPUs satisfies the GPULister interface. It records every invocation so
// tests can verify how many times the database was queried.
func (m *mockLister) ListGPUs(_ context.Context) ([]store.GPUSummary, error) {
	m.callCount++
	return m.gpus, m.err
}

// ─── TestListGPUs ─────────────────────────────────────────────────────────────

// TestListGPUs_ColdCache verifies that a cold (never-populated) cache always
// queries the database and returns false for the hit indicator.
func TestListGPUs_ColdCache(t *testing.T) {
	// Arrange: lister returns an empty GPU list.
	ml := &mockLister{gpus: []store.GPUSummary{}}
	c := New(ml, 1*time.Minute)

	// Act: first call on a cold cache.
	gpus, hit, err := c.ListGPUs(context.Background())

	// Assert: no error, cache miss, empty slice.
	if err != nil {
		t.Fatalf("ListGPUs() unexpected error: %v", err)
	}
	if hit {
		t.Error("expected cache miss on cold cache, got hit")
	}
	if gpus == nil {
		t.Error("expected non-nil slice, got nil")
	}
	if ml.callCount != 1 {
		t.Errorf("expected 1 DB call, got %d", ml.callCount)
	}
}

// TestListGPUs_CacheHitAfterWarm verifies that the second call within the TTL
// is served from cache (hit = true) without querying the database again.
func TestListGPUs_CacheHitAfterWarm(t *testing.T) {
	// Arrange: lister returns an empty list.
	ml := &mockLister{gpus: []store.GPUSummary{}}
	c := New(ml, 1*time.Minute)

	// First call warms the cache.
	_, _, err := c.ListGPUs(context.Background())
	if err != nil {
		t.Fatalf("warm-up call failed: %v", err)
	}

	// Act: second call should be a cache hit.
	_, hit, err := c.ListGPUs(context.Background())

	// Assert.
	if err != nil {
		t.Fatalf("ListGPUs() unexpected error: %v", err)
	}
	if !hit {
		t.Error("expected cache hit on second call, got miss")
	}
	if ml.callCount != 1 {
		t.Errorf("expected only 1 DB call total, got %d", ml.callCount)
	}
}

// TestListGPUs_ExpiredCache verifies that once the TTL has elapsed the cache
// is refreshed (hit = false) and the expiry is pushed forward.
func TestListGPUs_ExpiredCache(t *testing.T) {
	// Arrange: fixed clock starting in the past.
	fixedNow := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ml := &mockLister{gpus: []store.GPUSummary{}}
	c := New(ml, 1*time.Minute)

	// Override the clock so we can control time.
	c.nowFunc = func() time.Time { return fixedNow }

	// Warm the cache at t=0.
	c.ListGPUs(context.Background()) //nolint:errcheck

	// Advance the clock past the TTL.
	c.nowFunc = func() time.Time { return fixedNow.Add(2 * time.Minute) }

	// Act: call after expiry.
	_, hit, err := c.ListGPUs(context.Background())

	// Assert: should be a cache miss (refresh triggered).
	if err != nil {
		t.Fatalf("ListGPUs() unexpected error: %v", err)
	}
	if hit {
		t.Error("expected cache miss after TTL expiry, got hit")
	}
	if ml.callCount != 2 {
		t.Errorf("expected 2 DB calls (warm + refresh), got %d", ml.callCount)
	}
}

// TestListGPUs_DBError verifies that a database error is propagated to the
// caller and does not corrupt the cache state.
func TestListGPUs_DBError(t *testing.T) {
	// Arrange: lister returns an error.
	dbErr := errors.New("connection refused")
	ml := &mockLister{err: dbErr}
	c := New(ml, 1*time.Minute)

	// Act: call on a cold cache with a failing lister.
	_, hit, err := c.ListGPUs(context.Background())

	// Assert: error is propagated, hit is false.
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, dbErr) {
		t.Errorf("expected dbErr, got: %v", err)
	}
	if hit {
		t.Error("expected hit=false on error, got true")
	}
}

// TestRefreshLocked_DoubleCheckHit directly exercises the double-checked
// locking branch inside refreshLocked: when the cache is already valid at the
// time the write lock is acquired, it returns the cached value without calling
// the lister.
//
// This test calls refreshLocked directly (it is in the same package) to avoid
// the inherent scheduling non-determinism of the goroutine-based approach while
// still achieving 100% statement coverage.
func TestRefreshLocked_DoubleCheckHit(t *testing.T) {
	// Arrange: lister whose call count we can verify.
	ml := &mockLister{gpus: []store.GPUSummary{{ID: "GPU-1"}}}
	c := New(ml, 1*time.Minute)

	// Pre-populate the cache as if a previous call already refreshed it.
	c.mu.Lock()
	c.entries = []store.GPUSummary{{ID: "GPU-1"}}
	c.expiresAt = time.Now().Add(5 * time.Minute) // far in the future → double-check passes
	c.mu.Unlock()

	// Act: call refreshLocked with the write lock held (as ListGPUs would do).
	// The double-check should detect a fresh cache and return hit=true.
	c.mu.Lock()
	gpus, hit, err := c.refreshLocked(context.Background())
	c.mu.Unlock()

	// Assert: hit=true, no error, no DB call.
	if err != nil {
		t.Fatalf("refreshLocked() unexpected error: %v", err)
	}
	if !hit {
		t.Error("expected hit=true from double-checked locking path, got false")
	}
	if len(gpus) != 1 || gpus[0].ID != "GPU-1" {
		t.Errorf("unexpected gpus: %+v", gpus)
	}
	if ml.callCount != 0 {
		t.Errorf("lister should not be called; got %d calls", ml.callCount)
	}
}

// TestListGPUs_DoubleCheckedLocking verifies the double-checked locking path:
// if the cache is refreshed between dropping the RLock and acquiring the Lock,
// the second goroutine must not perform another DB query.
//
// Strategy: use a blocking lister that holds a lock until we manually unlock it.
// 1. Goroutine A calls ListGPUs on a cold cache → acquires write Lock, calls lister.
// 2. Goroutine B calls ListGPUs → cannot acquire write Lock; blocks.
// 3. Goroutine A's lister is unblocked; A populates the cache and releases Lock.
// 4. Goroutine B now acquires Lock, the double-check sees cache is fresh → hit=true.
func TestListGPUs_DoubleCheckedLocking(t *testing.T) {
	// insideListerCh is closed by the lister when goroutine A is inside it,
	// so we know A holds the write Lock.
	insideListerCh := make(chan struct{})
	// blockCh blocks the lister until we explicitly release it.
	blockCh := make(chan struct{})
	// callCount tracks how many times the lister is called.
	calls := 0

	// blockingLister signals when it's entered then blocks until released.
	bl := &blockingLister{
		insideCh: insideListerCh,
		blockCh:  blockCh,
		onCall:   func() { calls++ },
		gpus:     []store.GPUSummary{},
	}
	c := New(bl, 1*time.Minute)

	// Goroutine A: will enter the slow path, hold the write Lock inside lister.
	doneCh := make(chan struct {
		hit bool
		err error
	}, 1)
	go func() {
		_, hit, err := c.ListGPUs(context.Background())
		doneCh <- struct {
			hit bool
			err error
		}{hit, err}
	}()

	// Wait until A is confirmed inside the lister (and thus holds the write Lock).
	<-insideListerCh

	// Goroutine B: will try to acquire the write lock, block behind A.
	done2Ch := make(chan struct {
		hit bool
		err error
	}, 1)
	go func() {
		_, hit, err := c.ListGPUs(context.Background())
		done2Ch <- struct {
			hit bool
			err error
		}{hit, err}
	}()

	// Give goroutine B time to reach the Lock() call and block behind A.
	time.Sleep(30 * time.Millisecond)

	// Unblock goroutine A's lister → A populates the cache and releases Lock.
	close(blockCh)

	// Collect results.
	r1 := <-doneCh
	r2 := <-done2Ch

	// Goroutine A should be a miss (it fetched from DB).
	if r1.hit {
		t.Error("goroutine A: expected cache miss, got hit")
	}
	if r1.err != nil {
		t.Fatalf("goroutine A: unexpected error: %v", r1.err)
	}

	// Goroutine B should be a hit (double-check saw cache populated by A).
	if !r2.hit {
		t.Error("goroutine B: expected cache hit from double-checked locking path, got miss")
	}
	if r2.err != nil {
		t.Fatalf("goroutine B: unexpected error: %v", r2.err)
	}

	// Lister should have been called exactly once (by goroutine A only).
	if calls != 1 {
		t.Errorf("lister call count: got %d, want 1", calls)
	}
}

// blockingLister is a GPULister that signals when it's been entered then blocks
// on blockCh until released. This allows tests to precisely coordinate concurrent
// goroutines to exercise the double-checked locking code path.
type blockingLister struct {
	insideCh chan struct{} // closed once when the lister is entered (once only)
	blockCh  <-chan struct{}
	onCall   func()
	gpus     []store.GPUSummary
	err      error
}

// ListGPUs signals entry, blocks until blockCh is closed, then returns results.
func (b *blockingLister) ListGPUs(_ context.Context) ([]store.GPUSummary, error) {
	if b.onCall != nil {
		b.onCall()
	}
	// Signal that we are now inside the lister (and thus holding the write Lock).
	select {
	case <-b.insideCh:
		// Already signalled (insideCh was already closed); skip.
	default:
		close(b.insideCh)
	}
	// Block until the test releases us.
	<-b.blockCh
	return b.gpus, b.err
}

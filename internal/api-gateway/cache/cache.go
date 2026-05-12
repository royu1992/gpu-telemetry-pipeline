package cache

import (
	"context"
	"sync"
	"time"

	"github.com/royu1992/gpu-telemetry-pipeline/internal/store"
)

// GPULister is the dependency the Cache needs to refresh its GPU list.
// In production it is implemented by *store.Store; in tests it is a simple mock.
type GPULister interface {
	// ListGPUs returns the current set of distinct GPUs from the database.
	ListGPUs(ctx context.Context) ([]store.GPUSummary, error)
}

// Cache wraps the GPU list query result with a time-based expiry.
// It is safe for concurrent use by multiple Gin goroutines: reads acquire a
// shared RLock and writes acquire an exclusive Lock with double-checked locking
// to avoid redundant database queries when many goroutines race on an expired cache.
type Cache struct {
	// lister provides the raw GPU list on cache misses.
	lister GPULister

	// ttl is the maximum age of a cached GPU list before the next request
	// triggers a fresh database query.
	ttl time.Duration

	// mu guards entries and expiresAt. Use RLock for reads and Lock for writes.
	mu sync.RWMutex

	// entries is the last-known GPU list fetched from the database.
	// It is never nil after the first successful refresh.
	entries []store.GPUSummary

	// expiresAt is the wall-clock time after which entries are considered stale.
	// The zero value causes the first request to always refresh.
	expiresAt time.Time

	// nowFunc is the source of wall-clock time. Production code uses time.Now;
	// tests may substitute a fixed clock to exercise expiry logic deterministically.
	nowFunc func() time.Time
}

// New creates a Cache backed by the given GPULister with the configured TTL.
// The cache starts empty (cold); the first call to ListGPUs will always query
// the database.
func New(lister GPULister, ttl time.Duration) *Cache {
	return &Cache{
		lister:  lister,
		ttl:     ttl,
		nowFunc: time.Now,
	}
}

// ListGPUs returns the cached GPU list if it is still valid, or refreshes it
// from the database otherwise.
//
// The returned bool is true when the result was served from cache (no DB query
// was made) and false when the database was queried. Callers use this to update
// cache-hit and cache-miss observability counters.
//
// Thread-safety: multiple goroutines may call ListGPUs concurrently. Only one
// will perform the DB refresh; all others wait for the lock and then read the
// newly cached value.
func (c *Cache) ListGPUs(ctx context.Context) ([]store.GPUSummary, bool, error) {
	// ── Fast path: read lock ─────────────────────────────────────────────────
	// Acquiring an RLock allows concurrent readers to proceed simultaneously
	// without blocking each other. If the cache is still valid, we return here.
	c.mu.RLock()
	if c.nowFunc().Before(c.expiresAt) {
		// Cache hit: entries is valid, return a copy to avoid data races.
		result := make([]store.GPUSummary, len(c.entries))
		copy(result, c.entries)
		c.mu.RUnlock()
		return result, true, nil
	}
	c.mu.RUnlock()

	// ── Slow path: exclusive write lock ──────────────────────────────────────
	// The cache is expired or cold. Acquire an exclusive lock before refreshing.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-checked locking: another goroutine may have refreshed the cache
	// while this goroutine was waiting for the write lock. Re-check expiry to
	// avoid a redundant database round-trip.
	return c.refreshLocked(ctx)
}

// refreshLocked performs the double-checked locking and, when necessary, the
// database refresh. It must be called with c.mu held for writing.
//
// Returning from here releases the deferred Unlock in ListGPUs.
func (c *Cache) refreshLocked(ctx context.Context) ([]store.GPUSummary, bool, error) {
	// Re-check: if another goroutine already refreshed while we waited for the
	// write lock, the cache is now valid and we can skip the DB query.
	if c.nowFunc().Before(c.expiresAt) {
		// Another goroutine already refreshed; return the fresh cached value.
		result := make([]store.GPUSummary, len(c.entries))
		copy(result, c.entries)
		return result, true, nil
	}

	// Query the database for the current GPU list.
	gpus, err := c.lister.ListGPUs(ctx)
	if err != nil {
		// On error, do NOT update the cache. The caller will receive an error
		// and the cache retains its previous (possibly stale) state so the
		// next request can try again.
		return nil, false, err
	}

	// Commit the fresh result to the cache and advance the expiry deadline.
	c.entries = gpus
	c.expiresAt = c.nowFunc().Add(c.ttl)

	// Return a copy so the caller cannot mutate the cached slice.
	result := make([]store.GPUSummary, len(gpus))
	copy(result, gpus)
	return result, false, nil
}

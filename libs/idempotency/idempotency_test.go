package idempotency_test

// Unit tests for the deduplication primitives.
//
// These need a real PostgreSQL, because the whole mechanism IS database behaviour: the
// atomicity of the upsert, the row lock that makes a concurrent duplicate wait, and the
// visibility rules that decide what the loser of that race sees. A fake would test the
// wrapper and skip the part that can actually be wrong.
//
// They skip when SW_EDM_TEST_POSTGRES_DSN is unset, so `go test ./...` stays hermetic:
//
//	SW_EDM_TEST_POSTGRES_DSN='postgres://orders:orders@localhost:55432/orders?sslmode=disable' \
//	  go test ./libs/idempotency/...

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/idempotency"
)

type request struct {
	Order  string `json:"order"`
	Amount int64  `json:"amount"`
}

type response struct {
	Outcome string `json:"outcome"`
	ID      string `json:"id"`
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("SW_EDM_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SW_EDM_TEST_POSTGRES_DSN is not set; skipping database-backed idempotency tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pinging: %v", err)
	}
	// The table comes from the service's own migrations when run against a live database;
	// created here too so the test stands alone against an empty one.
	if _, err := pool.Exec(ctx, idempotency.SchemaSQL); err != nil {
		t.Fatalf("creating schema: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// freshKey keeps tests independent of each other and of whatever a demo left behind.
func freshKey(t *testing.T) string {
	t.Helper()
	return "test:" + uuid.NewString()
}

func inTx(t *testing.T, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// TestDoRunsOnceAndReplaysTheAnswer is the baseline: the second call must get the FIRST
// call's answer, not an acknowledgement. The caller retried because it never learned the
// answer, so an acknowledgement is useless to it.
func TestDoRunsOnceAndReplaysTheAnswer(t *testing.T) {
	pool := testPool(t)
	key := freshKey(t)
	req := request{Order: "order-1", Amount: 1250}

	calls := 0
	var first, second idempotency.Result

	for i := 0; i < 2; i++ {
		err := inTx(t, pool, func(tx pgx.Tx) error {
			res, err := idempotency.Do(context.Background(), tx, "test.scope", key, req,
				func(context.Context) (any, error) {
					calls++
					return response{Outcome: "AUTHORIZED", ID: "payment-1"}, nil
				})
			if err != nil {
				return err
			}
			if i == 0 {
				first = *res
			} else {
				second = *res
			}
			return nil
		})
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}

	if calls != 1 {
		t.Errorf("the work ran %d times, want exactly 1", calls)
	}
	if first.Deduplicated {
		t.Error("the first call was reported as deduplicated")
	}
	if !second.Deduplicated {
		t.Error("the second call was NOT reported as deduplicated")
	}

	var got response
	if err := second.Unmarshal(&got); err != nil {
		t.Fatalf("decoding replayed response: %v", err)
	}
	if got.Outcome != "AUTHORIZED" || got.ID != "payment-1" {
		t.Errorf("replayed response = %+v, want the first call's answer", got)
	}
}

// TestConcurrentDuplicatesSerialise is the race the mechanism exists for.
//
// Two attempts claim the same key at the same time in separate transactions. The upsert
// takes a row lock, so the loser BLOCKS until the winner's transaction resolves rather than
// both proceeding — which is exactly what ON CONFLICT DO NOTHING would allow, since it
// neither returns a row nor locks.
//
// The loser must not run the work. Whether it reports in-progress or replays the completed
// answer depends on the interleaving, and both are correct; running the work twice is not.
func TestConcurrentDuplicatesSerialise(t *testing.T) {
	pool := testPool(t)
	key := freshKey(t)
	req := request{Order: "order-2", Amount: 999}

	const attempts = 8
	var (
		mu        sync.Mutex
		ran       int
		succeeded int
		inflight  int
		other     []error
		wg        sync.WaitGroup
		start     = make(chan struct{})
	)

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release them together, to make the race as tight as possible
			err := inTx(t, pool, func(tx pgx.Tx) error {
				_, err := idempotency.Do(context.Background(), tx, "test.scope", key, req,
					func(context.Context) (any, error) {
						mu.Lock()
						ran++
						mu.Unlock()
						// Held briefly so the other attempts are genuinely contending
						// rather than arriving after the winner has already committed.
						time.Sleep(50 * time.Millisecond)
						return response{Outcome: "AUTHORIZED", ID: "payment-2"}, nil
					})
				return err
			})

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, idempotency.ErrInProgress):
				inflight++
			default:
				other = append(other, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(other) > 0 {
		t.Fatalf("unexpected errors: %v", other)
	}
	// THE assertion. Anything above 1 is a double charge.
	if ran != 1 {
		t.Errorf("the work ran %d times across %d concurrent duplicates, want exactly 1", ran, attempts)
	}
	if succeeded+inflight != attempts {
		t.Errorf("accounted for %d of %d attempts", succeeded+inflight, attempts)
	}
	t.Logf("%d concurrent duplicates: work ran %d time(s), %d completed, %d told to retry",
		attempts, ran, succeeded, inflight)
}

// TestKeyReuseWithADifferentRequestIsRejected: replaying the first response for a materially
// different request would silently discard the caller's second request. That is a caller
// bug, and it is reported rather than absorbed.
func TestKeyReuseWithADifferentRequestIsRejected(t *testing.T) {
	pool := testPool(t)
	key := freshKey(t)

	err := inTx(t, pool, func(tx pgx.Tx) error {
		_, err := idempotency.Do(context.Background(), tx, "test.scope", key,
			request{Order: "order-3", Amount: 100},
			func(context.Context) (any, error) { return response{Outcome: "OK"}, nil })
		return err
	})
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	ran := false
	err = inTx(t, pool, func(tx pgx.Tx) error {
		_, err := idempotency.Do(context.Background(), tx, "test.scope", key,
			request{Order: "order-3", Amount: 999}, // same key, different amount
			func(context.Context) (any, error) { ran = true; return response{}, nil })
		return err
	})
	if !errors.Is(err, idempotency.ErrKeyReuse) {
		t.Fatalf("error = %v, want ErrKeyReuse", err)
	}
	if ran {
		t.Error("the work ran for a reused key")
	}
}

// TestFailedAttemptLeavesNoTrace: a rolled-back attempt must be retryable cleanly, because
// nothing happened. A claim that survived a rollback would block its key forever.
func TestFailedAttemptLeavesNoTrace(t *testing.T) {
	pool := testPool(t)
	key := freshKey(t)
	req := request{Order: "order-4", Amount: 500}
	boom := errors.New("the work failed")

	err := inTx(t, pool, func(tx pgx.Tx) error {
		_, err := idempotency.Do(context.Background(), tx, "test.scope", key, req,
			func(context.Context) (any, error) { return nil, boom })
		return err
	})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want the work's own error", err)
	}

	ran := false
	err = inTx(t, pool, func(tx pgx.Tx) error {
		_, err := idempotency.Do(context.Background(), tx, "test.scope", key, req,
			func(context.Context) (any, error) { ran = true; return response{Outcome: "OK"}, nil })
		return err
	})
	if err != nil {
		t.Fatalf("retry after a failed attempt: %v", err)
	}
	if !ran {
		t.Error("the retry did not run the work; the failed attempt left a claim behind")
	}
}

// TestReleaseAllowsAGenuineRetry covers the bug this repository actually had.
//
// A retryable failure recorded under the key replays "unavailable" forever, and a
// thirty-second outage becomes a permanently failed order. From the outside it looks exactly
// like a working retry loop, which is what makes it expensive to find.
func TestReleaseAllowsAGenuineRetry(t *testing.T) {
	pool := testPool(t)
	key := freshKey(t)
	req := request{Order: "order-5", Amount: 750}
	ctx := context.Background()

	// Attempt 1: claim, get a transient failure, release, and COMMIT — the commit matters,
	// because the real caller commits its diagnostic state alongside the release.
	err := inTx(t, pool, func(tx pgx.Tx) error {
		status, _, err := idempotency.Claim(ctx, tx, "test.scope", key, req, 0)
		if err != nil {
			return err
		}
		if status != idempotency.StatusNew {
			t.Fatalf("first claim = %v, want new", status)
		}
		return idempotency.Release(ctx, tx, "test.scope", key)
	})
	if err != nil {
		t.Fatalf("attempt 1: %v", err)
	}

	// Attempt 2 must see a clean slate, not a replayed failure or an in-progress error.
	err = inTx(t, pool, func(tx pgx.Tx) error {
		status, _, err := idempotency.Claim(ctx, tx, "test.scope", key, req, 0)
		if err != nil {
			return err
		}
		if status != idempotency.StatusNew {
			t.Errorf("claim after release = %v, want new — a transient failure was memoised", status)
		}
		return idempotency.Complete(ctx, tx, "test.scope", key, response{Outcome: "AUTHORIZED"})
	})
	if err != nil {
		t.Fatalf("attempt 2: %v", err)
	}

	// And the terminal outcome IS memoised.
	err = inTx(t, pool, func(tx pgx.Tx) error {
		status, stored, err := idempotency.Claim(ctx, tx, "test.scope", key, req, 0)
		if err != nil {
			return err
		}
		if status != idempotency.StatusCompleted {
			t.Errorf("third claim = %v, want completed", status)
		}
		var got response
		if err := (&idempotency.Result{Response: stored}).Unmarshal(&got); err != nil {
			return err
		}
		if got.Outcome != "AUTHORIZED" {
			t.Errorf("replayed outcome = %q, want AUTHORIZED", got.Outcome)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("attempt 3: %v", err)
	}
}

// TestStaleClaimIsResumable: a claim whose process died must not block its key forever.
// resumeAfter is passed as 1ns so the test does not sleep for the 30s default.
func TestStaleClaimIsResumable(t *testing.T) {
	pool := testPool(t)
	key := freshKey(t)
	req := request{Order: "order-6", Amount: 300}
	ctx := context.Background()

	// A claim that commits and is then abandoned — the process died between claiming and
	// completing, which is exactly the case Claim/Complete exists to survive.
	err := inTx(t, pool, func(tx pgx.Tx) error {
		status, _, err := idempotency.Claim(ctx, tx, "test.scope", key, req, time.Hour)
		if status != idempotency.StatusNew {
			t.Fatalf("first claim = %v, want new", status)
		}
		return err
	})
	if err != nil {
		t.Fatalf("abandoned claim: %v", err)
	}

	// While it is still fresh, another attempt must be told to wait rather than take over.
	err = inTx(t, pool, func(tx pgx.Tx) error {
		_, _, err := idempotency.Claim(ctx, tx, "test.scope", key, req, time.Hour)
		return err
	})
	if !errors.Is(err, idempotency.ErrInProgress) {
		t.Errorf("claim against a FRESH in-progress claim = %v, want ErrInProgress", err)
	}

	// Once stale, it may be resumed.
	err = inTx(t, pool, func(tx pgx.Tx) error {
		status, _, err := idempotency.Claim(ctx, tx, "test.scope", key, req, time.Nanosecond)
		if err != nil {
			return err
		}
		if status != idempotency.StatusResume {
			t.Errorf("claim against a STALE claim = %v, want resume", status)
		}
		return idempotency.Complete(ctx, tx, "test.scope", key, response{Outcome: "AUTHORIZED"})
	})
	if err != nil {
		t.Fatalf("resuming: %v", err)
	}
}

// TestSeenDetectsRedelivery is the event-consumer path: the first sighting reports false,
// every later one reports true.
func TestSeenDetectsRedelivery(t *testing.T) {
	pool := testPool(t)
	eventID := uuid.NewString()
	ctx := context.Background()

	var results []bool
	for i := 0; i < 3; i++ {
		err := inTx(t, pool, func(tx pgx.Tx) error {
			already, err := idempotency.Seen(ctx, tx, eventID)
			results = append(results, already)
			return err
		})
		if err != nil {
			t.Fatalf("sighting %d: %v", i, err)
		}
	}

	if results[0] {
		t.Error("the first sighting reported the event as already seen")
	}
	if !results[1] || !results[2] {
		t.Errorf("redeliveries reported as unseen: %v", results)
	}
}

// TestSeenRollsBackWithItsTransaction: if the handler's transaction fails, the event must
// NOT be recorded as processed, or it would never be retried and the work would be lost.
func TestSeenRollsBackWithItsTransaction(t *testing.T) {
	pool := testPool(t)
	eventID := uuid.NewString()
	ctx := context.Background()
	boom := errors.New("handler failed")

	err := inTx(t, pool, func(tx pgx.Tx) error {
		if _, err := idempotency.Seen(ctx, tx, eventID); err != nil {
			return err
		}
		return boom // rolls the transaction back, taking the record with it
	})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want the handler's error", err)
	}

	err = inTx(t, pool, func(tx pgx.Tx) error {
		already, err := idempotency.Seen(ctx, tx, eventID)
		if already {
			t.Error("the event was recorded as seen despite its transaction rolling back; " +
				"it would never be retried")
		}
		return err
	})
	if err != nil {
		t.Fatalf("second sighting: %v", err)
	}
}

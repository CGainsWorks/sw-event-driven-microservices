// Package idempotency makes an operation safe to repeat.
//
// Three different problems share one mechanism here:
//
//   - A *command* arrives twice, because the caller's first attempt timed out and it
//     retried. The second call must return the first call's answer and must not do the
//     work again.
//   - An *event* arrives twice, because the outbox publishes at least once and Kafka
//     redelivers on rebalance. The handler must run once.
//   - A command involves an *external call* — a payment provider — that must happen at
//     most once even if this process dies in the middle of it.
//
// All three are "I have seen this key before, here is what happened". The difference is
// only what the key is: a caller-chosen idempotency_key for commands, the event_id for
// events.
//
// # Why this is in the database and not in a cache
//
// The record of "this was done" must commit in the same transaction as the work itself. If
// deduplication lives in Redis and the work lives in PostgreSQL, there is a window where
// one has committed and the other has not, and that window is exactly the crash that
// causes a double charge. Same database, same transaction, no window.
//
// # Why the response is stored
//
// Returning "already done" is not enough. The caller retried because it never learned the
// answer, so it needs the answer, not an acknowledgement. Storing the serialised response
// makes the second call indistinguishable from the first as far as the caller is
// concerned, which is what idempotent actually means.
//
// # Two APIs
//
// [Do] is for work that fits in one transaction: claim, do, complete, commit. Use it
// whenever nothing outside the database is called.
//
// [Claim] and [Complete] are for work that cannot, because it makes an external call.
// The attempt is recorded and committed BEFORE the call goes out, so that a crash
// mid-call leaves evidence the request may have been sent. Without that record, a retry
// cannot tell "never sent" from "sent, answer lost" — and those need opposite handling.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/pgmigrate"
)

// SchemaSQL is identical in every service that deduplicates anything.
//
//   - (scope, key) is the primary key. `scope` namespaces the key space so an inventory
//     reservation key and a payment authorization key cannot collide, and so one table
//     serves both command and event deduplication.
//   - request_hash detects key reuse. A caller sending the same key with a different
//     request has a bug, and replaying the first response for a different request would
//     hide it.
//   - claimed_at supports resumption of a claim whose process died. See [Claim].
const SchemaSQL = `
CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope         TEXT        NOT NULL,
    key           TEXT        NOT NULL,
    request_hash  TEXT        NOT NULL,
    state         TEXT        NOT NULL CHECK (state IN ('in_progress', 'completed')),
    response      JSONB,
    claimed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    PRIMARY KEY (scope, key)
);

-- Supports the retention sweep. Records are kept far longer than any caller will retry,
-- but not forever; see scripts/prune_idempotency.sql.
CREATE INDEX IF NOT EXISTS idempotency_keys_created_at_idx
    ON idempotency_keys (created_at);

-- Supports finding claims that were never completed, for the stale-claim resume path.
CREATE INDEX IF NOT EXISTS idempotency_keys_stale_idx
    ON idempotency_keys (claimed_at) WHERE state = 'in_progress';
`

// Migration returns the DDL as a migration for pgmigrate.Apply.
func Migration() pgmigrate.Migration {
	return pgmigrate.Migration{Name: "0002_idempotency", SQL: SchemaSQL}
}

// Scopes used across the system. Constants rather than free strings so that a typo is a
// compile error instead of a silently separate key space in which nothing deduplicates.
const (
	ScopeAuthorizePayment  = "payments.authorize"
	ScopeCapturePayment    = "payments.capture"
	ScopeVoidAuthorization = "payments.void"
	ScopeRefundPayment     = "payments.refund"
	ScopeReserveStock      = "inventory.reserve"
	ScopeReleaseStock      = "inventory.release"
	ScopeCommitReservation = "inventory.commit"

	// ScopeEvent deduplicates consumed Kafka events. The key is the event_id.
	ScopeEvent = "event"
)

// DefaultResumeAfter is how long an in-progress claim must have sat untouched before
// another attempt may take it over.
//
// The value is a trade. Too short and two live attempts run concurrently; too long and a
// caller whose process crashed waits pointlessly. Thirty seconds comfortably exceeds every
// deadline in this system (the longest is the 10s payment authorize deadline plus retries),
// so a claim older than this belongs to a process that is not coming back.
//
// Resumption is only safe because the external call being resumed is itself keyed by the
// same idempotency key, so the provider deduplicates it. That is the assumption this
// number rests on, and it is the first thing to check if it is ever changed.
const DefaultResumeAfter = 30 * time.Second

// Status is what Claim found.
type Status int

const (
	// StatusNew: no previous attempt. Do the work.
	StatusNew Status = iota

	// StatusResume: a previous attempt claimed the key and never finished, and it is old
	// enough to be considered dead. Do the work again; the external call is keyed, so the
	// provider will deduplicate it.
	StatusResume

	// StatusCompleted: an earlier attempt finished. Return its stored response and do
	// nothing else.
	StatusCompleted
)

func (s Status) String() string {
	switch s {
	case StatusNew:
		return "new"
	case StatusResume:
		return "resume"
	case StatusCompleted:
		return "completed"
	default:
		return "unknown"
	}
}

// ErrKeyReuse is returned when a key has already been used for a materially different
// request. Always a caller bug, and reported rather than absorbed: silently replaying the
// earlier response would discard the caller's second, different request without anyone
// noticing.
var ErrKeyReuse = errors.New("idempotency: key reused with a different request")

// ErrInProgress is returned when another attempt holds this key and is recent enough to
// still be alive. The correct response to a caller is "try again shortly" — never a guess
// at what the other attempt will decide.
var ErrInProgress = errors.New("idempotency: an identical request is in progress")

// Claim reserves (scope, key) for this attempt.
//
// Call it in its own short transaction and COMMIT before making the external call. That
// commit is the whole point: it is the durable evidence that the request may have gone
// out, and it is what lets a later GetPaymentByIdempotencyKey answer "yes, that request
// reached us" rather than "no idea".
//
// resumeAfter of zero means [DefaultResumeAfter].
func Claim(ctx context.Context, tx pgx.Tx, scope, key string, request any, resumeAfter time.Duration) (Status, []byte, error) {
	if key == "" {
		return StatusNew, nil, fmt.Errorf("idempotency: empty key for scope %q", scope)
	}
	if resumeAfter <= 0 {
		resumeAfter = DefaultResumeAfter
	}
	hash, err := HashRequest(request)
	if err != nil {
		return StatusNew, nil, err
	}

	// ON CONFLICT DO UPDATE rather than DO NOTHING, for two reasons:
	//
	//  1. It always returns a row, so the existing record is read in one round trip.
	//  2. It takes a row lock, so a concurrent duplicate blocks here until this
	//     transaction commits and then sees the true state. DO NOTHING neither returns a
	//     row nor locks, and loses that race.
	//
	// The conditional SET is what implements resumption: an in-progress claim older than
	// the threshold has its claimed_at refreshed, which both marks the takeover and
	// re-arms the threshold for whoever comes next. A fresh claim is left untouched so
	// that it is reported as in-progress rather than stolen.
	//
	// (xmax = 0) distinguishes an insert from an update: xmax is zero on a freshly
	// inserted tuple and non-zero on one ON CONFLICT has just locked.
	const upsert = `
		INSERT INTO idempotency_keys (scope, key, request_hash, state)
		VALUES ($1, $2, $3, 'in_progress')
		ON CONFLICT (scope, key) DO UPDATE
		   SET claimed_at = CASE
		         WHEN idempotency_keys.state = 'in_progress'
		          AND idempotency_keys.claimed_at < now() - $4::interval
		         THEN now()
		         ELSE idempotency_keys.claimed_at
		       END
		RETURNING request_hash, state, response, claimed_at, (xmax = 0) AS inserted`

	var (
		storedHash string
		state      string
		response   []byte
		claimedAt  time.Time
		inserted   bool
	)
	err = tx.QueryRow(ctx, upsert, scope, key, hash, resumeAfter.String()).
		Scan(&storedHash, &state, &response, &claimedAt, &inserted)
	if err != nil {
		return StatusNew, nil, fmt.Errorf("idempotency: claiming %s/%s: %w", scope, key, err)
	}

	if inserted {
		return StatusNew, nil, nil
	}
	if storedHash != hash {
		return StatusNew, nil, fmt.Errorf("%w: scope=%s key=%s", ErrKeyReuse, scope, key)
	}
	if state == "completed" {
		return StatusCompleted, response, nil
	}

	// Still in progress. The UPDATE above refreshed claimed_at if and only if the previous
	// claim was stale, so a claimed_at within the last second means this attempt took it
	// over. Comparing against the returned timestamp avoids a second round trip.
	if time.Since(claimedAt) < time.Second {
		return StatusResume, nil, nil
	}
	return StatusNew, nil, fmt.Errorf("%w: scope=%s key=%s claimed %s ago",
		ErrInProgress, scope, key, time.Since(claimedAt).Truncate(time.Millisecond))
}

// Complete records the outcome of a claimed operation.
//
// Call it in the transaction that also writes the domain change and the outbox row, so
// that "it happened", "here is what happened" and "tell everyone it happened" commit
// together.
func Complete(ctx context.Context, tx pgx.Tx, scope, key string, response any) error {
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("idempotency: marshalling response for %s/%s: %w", scope, key, err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE idempotency_keys
		   SET state = 'completed', response = $3, completed_at = now()
		 WHERE scope = $1 AND key = $2`, scope, key, encoded)
	if err != nil {
		return fmt.Errorf("idempotency: completing %s/%s: %w", scope, key, err)
	}
	if tag.RowsAffected() == 0 {
		// The claim vanished. Either the claiming transaction rolled back, or someone
		// deleted the row. Either way the caller is about to commit domain state for an
		// operation with no dedup record, which would let it run twice.
		return fmt.Errorf("idempotency: no claim found for %s/%s; refusing to complete", scope, key)
	}
	return nil
}

// Release drops a claim so that the operation can genuinely be attempted again.
//
// Call it when an attempt failed in a way that is worth retrying — a provider that was
// unreachable, a transient error. Without it the claim stays in progress, the stored
// answer (or the ErrInProgress) is replayed on every retry, and a thirty-second outage
// turns into a permanently failed order. That is a subtle and expensive bug: everything
// looks like it is retrying, and nothing is.
//
// The rule is that only TERMINAL outcomes are memoised. A declined card is terminal and
// must be replayed identically forever. "The provider did not answer" is not an outcome at
// all and must not be recorded as one.
//
// Safe to call on a claim that does not exist.
func Release(ctx context.Context, tx pgx.Tx, scope, key string) error {
	if _, err := tx.Exec(ctx,
		`DELETE FROM idempotency_keys WHERE scope = $1 AND key = $2 AND state = 'in_progress'`,
		scope, key); err != nil {
		return fmt.Errorf("idempotency: releasing %s/%s: %w", scope, key, err)
	}
	return nil
}

// Result reports what happened in a [Do] call.
type Result struct {
	// Response is the serialised reply — freshly produced, or replayed from an earlier
	// identical call.
	Response []byte

	// Deduplicated is true when Response came from an earlier call and fn was not run.
	Deduplicated bool
}

// Do runs fn at most once for a given (scope, key), entirely inside the caller's
// transaction.
//
// Use it when fn touches nothing outside this database. If fn calls a payment provider or
// any other external system, use [Claim] and [Complete] instead: a single transaction that
// wraps an external call cannot record that the call was attempted, because a crash rolls
// the record back while the call has already gone out.
func Do(ctx context.Context, tx pgx.Tx, scope, key string, request any, fn func(context.Context) (any, error)) (*Result, error) {
	status, stored, err := Claim(ctx, tx, scope, key, request, DefaultResumeAfter)
	if err != nil {
		return nil, err
	}
	if status == StatusCompleted {
		return &Result{Response: stored, Deduplicated: true}, nil
	}

	value, err := fn(ctx)
	if err != nil {
		// The caller will roll back, which removes the claim. A failed attempt leaves no
		// trace and retries cleanly, which is correct: nothing happened.
		//
		// A *business* failure — a declined card, insufficient stock — is NOT an error
		// here. It is a legitimate outcome that must be recorded so the retry sees the
		// same answer. Handlers return those as a value with a failure field.
		return nil, err
	}
	if err := Complete(ctx, tx, scope, key, value); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("idempotency: marshalling response for %s/%s: %w", scope, key, err)
	}
	return &Result{Response: encoded, Deduplicated: false}, nil
}

// Seen records an event_id as processed and reports whether it had already been seen.
//
// The event-consumer counterpart of Do, for handlers whose work has no response worth
// replaying. Call it inside the transaction that does the handler's work: if that
// transaction rolls back the event is not marked seen and will be retried, which is what
// makes redelivery safe.
func Seen(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	var inserted bool
	err := tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (scope, key, request_hash, state, completed_at)
		VALUES ($1, $2, '', 'completed', now())
		ON CONFLICT (scope, key) DO UPDATE SET scope = idempotency_keys.scope
		RETURNING (xmax = 0) AS inserted`, ScopeEvent, eventID).Scan(&inserted)
	if err != nil {
		return false, fmt.Errorf("idempotency: recording event %s: %w", eventID, err)
	}
	// inserted == true means this is the first sighting, so it has NOT been seen before.
	return !inserted, nil
}

// HashRequest produces the fingerprint stored alongside a key.
//
// JSON marshalling of a Go struct is deterministic (field order is declaration order), so
// this is stable for a given type. It is NOT stable across a change to that type's shape —
// adding a field changes every hash, and every in-flight key would then look like reuse.
// Acceptable because records are pruned well within a deployment cycle, and written down
// here so that it is not discovered during an upgrade.
//
// Callers should hash only the business-meaningful fields of a request. Including
// correlation or trace ids would make two genuine retries of one logical request look like
// two different requests, which defeats the entire mechanism.
func HashRequest(request any) (string, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("idempotency: hashing request: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// Unmarshal decodes a stored response into a typed value.
func (r *Result) Unmarshal(into any) error {
	if len(r.Response) == 0 {
		return nil
	}
	if err := json.Unmarshal(r.Response, into); err != nil {
		return fmt.Errorf("idempotency: decoding stored response: %w", err)
	}
	return nil
}

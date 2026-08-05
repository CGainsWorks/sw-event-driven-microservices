//go:build contract

// Provider verification for the message pacts.
//
//	docker compose up -d --build
//	npm test --workspace @sw-edm/contracts      # generate the pacts
//	go test -tags=contract ./tests/contract/... -v
//
// # Why this is not `pact-go`
//
// The usual provider verification hands Pact a function that RETURNS the message the
// provider would produce. For a Go producer that means pact-go, whose FFI shared library is
// a separate download — and, more importantly, it means verifying a function written for
// the test rather than the bytes the service actually publishes. A hand-written provider
// state that constructs an event is one more thing that can drift from the real producer,
// which is precisely the drift Pact exists to catch.
//
// So this verifies the real thing: it drives real orders through the running system, reads
// the events the producers ACTUALLY published to Kafka, and checks them against the
// consumer's expectations and matching rules. If orders stopped putting `customer_email` on
// `OrderConfirmed`, this fails with the name of the consumer that needed it.
//
// The cost is that it needs the stack up, so it is build-tagged and not in CI's default
// path. That is a real limitation and it is stated in contracts/README.md rather than
// glossed.
package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	ordersBase    = "http://localhost:18080"
	inventoryBase = "http://localhost:18082"
	kafkaExternal = "localhost:29092"
	partitions    = 6
)

// --- the pact file ----------------------------------------------------------

type pactFile struct {
	Consumer struct{ Name string } `json:"consumer"`
	Provider struct{ Name string } `json:"provider"`
	Messages []pactMessage         `json:"messages"`
}

type pactMessage struct {
	Description   string          `json:"description"`
	Contents      json.RawMessage `json:"contents"`
	MatchingRules struct {
		Body map[string]struct {
			Matchers []struct {
				Match string `json:"match"`
				Regex string `json:"regex"`
			} `json:"matchers"`
		} `json:"body"`
	} `json:"matchingRules"`
}

func loadPacts(t *testing.T) []pactFile {
	t.Helper()
	dir := filepath.Join("..", "..", "contracts", "pacts")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v\nrun `npm test --workspace @sw-edm/contracts` first to generate the pacts", dir, err)
	}
	var out []pactFile
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", e.Name(), err)
		}
		var p pactFile
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatalf("parsing %s: %v", e.Name(), err)
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		t.Fatal("no pact files found; run `npm test --workspace @sw-edm/contracts` first")
	}
	return out
}

// --- matching ---------------------------------------------------------------

// verify checks one actual event against one expectation, applying the pact's matching
// rules. Returns every mismatch rather than the first, because a producer that broke three
// fields should learn all three at once.
func verify(expected, actual any, rules map[string][]matcher, path string, problems *[]string) {
	switch exp := expected.(type) {
	case map[string]any:
		act, ok := actual.(map[string]any)
		if !ok {
			*problems = append(*problems, fmt.Sprintf("%s: expected an object, got %T", path, actual))
			return
		}
		keys := make([]string, 0, len(exp))
		for k := range exp {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			child, present := act[k]
			if !present {
				// The consumer asked for this field and the producer did not send it.
				// This is the failure the whole exercise exists to catch.
				*problems = append(*problems, fmt.Sprintf("%s.%s: MISSING — the consumer requires it", path, k))
				continue
			}
			verify(exp[k], child, rules, path+"."+k, problems)
		}

	case []any:
		act, ok := actual.([]any)
		if !ok {
			*problems = append(*problems, fmt.Sprintf("%s: expected an array, got %T", path, actual))
			return
		}
		if len(act) < len(exp) {
			*problems = append(*problems, fmt.Sprintf("%s: expected at least %d items, got %d", path, len(exp), len(act)))
			return
		}
		for i := range exp {
			verify(exp[i], act[i], rules, fmt.Sprintf("%s[%d]", path, i), problems)
		}

	default:
		applyLeaf(exp, actual, rules[path], path, problems)
	}
}

type matcher struct {
	Match string
	Regex string
}

// applyLeaf checks a scalar.
//
// With no matching rule, the value must be EXACTLY equal — that is Pact's default and it is
// what pins `event_type` and `producer`, the two fields a consumer dispatches on. With a
// rule, only the rule applies.
func applyLeaf(expected, actual any, ms []matcher, path string, problems *[]string) {
	if len(ms) == 0 {
		if fmt.Sprint(expected) != fmt.Sprint(actual) {
			*problems = append(*problems,
				fmt.Sprintf("%s: expected exactly %v, got %v", path, expected, actual))
		}
		return
	}
	for _, m := range ms {
		switch m.Match {
		case "regex":
			re, err := regexp.Compile(m.Regex)
			if err != nil {
				*problems = append(*problems, fmt.Sprintf("%s: unparseable regex in pact: %v", path, err))
				return
			}
			if !re.MatchString(fmt.Sprint(actual)) {
				*problems = append(*problems,
					fmt.Sprintf("%s: %q does not match %s", path, fmt.Sprint(actual), m.Regex))
			}
		case "type":
			if fmt.Sprintf("%T", expected) != fmt.Sprintf("%T", actual) {
				*problems = append(*problems,
					fmt.Sprintf("%s: expected type %T, got %T", path, expected, actual))
			}
		default:
			// An unrecognised matcher must not silently pass. A contract check that
			// quietly ignores rules it does not understand is worse than no check.
			*problems = append(*problems, fmt.Sprintf("%s: unsupported matcher %q", path, m.Match))
		}
	}
}

func rulesFor(m pactMessage) map[string][]matcher {
	out := map[string][]matcher{}
	for path, rule := range m.MatchingRules.Body {
		for _, x := range rule.Matchers {
			out[path] = append(out[path], matcher{Match: x.Match, Regex: x.Regex})
		}
	}
	return out
}

// --- the test ---------------------------------------------------------------

// TestProvidersSatisfyConsumerContracts drives real traffic, collects what was actually
// published, and verifies it against every consumer expectation.
func TestProvidersSatisfyConsumerContracts(t *testing.T) {
	pacts := loadPacts(t)

	t.Log("generating real traffic so there is something genuine to verify against")
	generateTraffic(t)

	// Collected once and shared: reading four topics from the beginning is the slow part,
	// and every pact interaction verifies against the same corpus.
	published := collectEvents(t)
	for typ, evts := range published {
		t.Logf("  collected %2d × %s", len(evts), typ)
	}

	for _, p := range pacts {
		t.Run(fmt.Sprintf("%s satisfies %s", p.Provider.Name, p.Consumer.Name), func(t *testing.T) {
			for _, msg := range p.Messages {
				t.Run(msg.Description, func(t *testing.T) {
					var expected map[string]any
					if err := json.Unmarshal(msg.Contents, &expected); err != nil {
						t.Fatalf("decoding expectation: %v", err)
					}

					eventType, _ := expected["event_type"].(string)
					candidates := published[eventType]
					if len(candidates) == 0 {
						t.Fatalf("the consumer expects %s but the provider never published one; "+
							"either the contract is stale or the producer stopped emitting it", eventType)
					}

					// Any real event of that type must satisfy the contract, so verify them
					// all rather than the first. A producer that emits a good one and a bad
					// one has still broken the consumer.
					rules := rulesFor(msg)
					for i, actual := range candidates {
						var problems []string
						verify(expected, actual, rules, "$", &problems)
						if len(problems) > 0 {
							t.Errorf("published %s #%d does not satisfy %s's contract:\n  %s",
								eventType, i, p.Consumer.Name, strings.Join(problems, "\n  "))
						}
					}
					t.Logf("%d published %s satisfy the contract", len(candidates), eventType)
				})
			}
		})
	}
}

// generateTraffic places orders that exercise both the confirmed and cancelled paths, so
// that every event type the contracts mention actually gets published.
func generateTraffic(t *testing.T) {
	t.Helper()
	setStock(t, "SKU-CONTRACT", 20)
	setStock(t, "SKU-CONTRACT-SHORT", 1)

	confirmed := place(t, "pm_ok", "SKU-CONTRACT", 2)
	cancelled := place(t, "pm_declined", "SKU-CONTRACT", 1)
	refused := place(t, "pm_ok", "SKU-CONTRACT-SHORT", 5)

	for _, id := range []string{confirmed, cancelled, refused} {
		awaitTerminal(t, id, 60*time.Second)
	}
	// The outbox relay and the broker are asynchronous; without a pause the topics may not
	// yet hold what was just committed.
	time.Sleep(3 * time.Second)
}

// collectEvents reads every topic from the beginning and groups events by type.
func collectEvents(t *testing.T) map[string][]map[string]any {
	t.Helper()
	out := map[string][]map[string]any{}

	for _, topic := range []string{"orders.v1.events", "payments.v1.events", "inventory.v1.events"} {
		for p := 0; p < partitions; p++ {
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   []string{kafkaExternal},
				Topic:     topic,
				Partition: p,
				MinBytes:  1,
				MaxBytes:  10e6,
			})
			if err := r.SetOffset(kafka.FirstOffset); err != nil {
				r.Close()
				continue
			}
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				msg, err := r.ReadMessage(ctx)
				cancel()
				if err != nil {
					break
				}
				var env map[string]any
				if json.Unmarshal(msg.Value, &env) != nil {
					continue
				}
				if typ, ok := env["event_type"].(string); ok {
					out[typ] = append(out[typ], env)
				}
			}
			r.Close()
		}
	}
	return out
}

// --- small HTTP helpers -----------------------------------------------------

var client = &http.Client{Timeout: 10 * time.Second}

func place(t *testing.T, token, sku string, qty int) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"customer_id":          "8c1d4e2a-5b6f-4a7c-8d9e-0f1a2b3c4d5e",
		"customer_email":       "contract@example.com",
		"payment_method_token": token,
		"lines": []map[string]any{{
			"sku": sku, "quantity": qty,
			"unit_amount": map[string]any{"amount_minor": 1250, "currency": "GBP"},
		}},
	})
	resp, err := client.Post(ordersBase+"/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("placing order: %v (is the stack up?)", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("placing order: status %d", resp.StatusCode)
	}
	var out struct {
		OrderID string `json:"order_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.OrderID
}

func setStock(t *testing.T, sku string, onHand int) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"items": []map[string]any{{"sku": sku, "on_hand": onHand, "reserved": 0}},
	})
	req, _ := http.NewRequest(http.MethodPut, inventoryBase+"/admin/stock", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("setting stock: %v", err)
	}
	resp.Body.Close()
}

func awaitTerminal(t *testing.T, id string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		resp, err := client.Get(ordersBase + "/orders/" + id)
		if err == nil {
			var o struct {
				State string `json:"state"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&o)
			resp.Body.Close()
			if o.State == "CONFIRMED" || o.State == "CANCELLED" {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("order %s never reached a terminal state", id)
}

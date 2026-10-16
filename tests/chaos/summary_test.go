package chaos

// The README's failure-injection table must match the committed results.
//
// The chaos scenarios themselves need a live stack and the `chaos` build tag, so they do
// not run on every push. Their output, docs/chaos-results.json, is committed (make chaos
// refreshes it), and this test, which needs no tag and no stack, fails if a figure the
// README quotes differs from that file or if any recorded scenario did not pass.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type summaryScenario struct {
	Scenario string             `json:"scenario"`
	Passed   bool               `json:"passed"`
	Measured map[string]float64 `json:"measured"`
	Timings  map[string]string  `json:"timings"`
}

type summaryFile struct {
	GeneratedAt string            `json:"generated_at"`
	Scenarios   []summaryScenario `json:"scenarios"`
}

func TestReadmeChaosTableMatchesCommittedResults(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "docs", "chaos-results.json"))
	if err != nil {
		t.Fatalf("reading docs/chaos-results.json: %v", err)
	}
	readmeRaw, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}
	readme := string(readmeRaw)

	var s summaryFile
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("parsing docs/chaos-results.json: %v", err)
	}
	by := map[string]summaryScenario{}
	for _, sc := range s.Scenarios {
		if !sc.Passed {
			t.Errorf("scenario %q did not pass in the committed results", sc.Scenario)
		}
		by[sc.Scenario] = sc
	}
	for _, name := range []string{"broker down", "payments unavailable", "duplicate delivery", "network partition"} {
		if _, ok := by[name]; !ok {
			t.Fatalf("scenario %q missing from docs/chaos-results.json", name)
		}
	}

	n := func(scenario, key string) int {
		v, ok := by[scenario].Measured[key]
		if !ok {
			t.Fatalf("%s: no measured %q", scenario, key)
		}
		return int(v)
	}
	broker, payments, dup, part := "broker down", "payments unavailable", "duplicate delivery", "network partition"

	want := []string{
		fmt.Sprintf("| %d events lost, backlog drained in **%s** after recovery |",
			n(broker, "events_lost"), by[broker].Timings["outbox_drain_after_recovery"]),
		fmt.Sprintf("| **payments stopped, %ds** |", n(payments, "outage_seconds")),
		fmt.Sprintf("| **%d** orders wrongly cancelled, **%d** stock wrongly released |",
			n(payments, "orders_wrongly_cancelled"), n(payments, "stock_wrongly_released")),
		fmt.Sprintf("| **Event republished ×%d** |", n(dup, "duplicate_deliveries")),
		fmt.Sprintf("| **%d** notification, same id |", n(dup, "notifications_sent")),
		fmt.Sprintf("| **payments partitioned, %ds** |", n(part, "partition_seconds")),
		fmt.Sprintf("| **%d** wrongly cancelled, **%d** duplicate payments |",
			n(part, "orders_wrongly_cancelled"), n(part, "duplicate_payments")),
		"from the run at " + s.GeneratedAt,
	}
	for _, w := range want {
		if !strings.Contains(readme, w) {
			t.Errorf("README does not contain %q (from docs/chaos-results.json)", w)
		}
	}
}

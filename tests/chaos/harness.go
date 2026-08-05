//go:build chaos

// Package chaos holds failure injection tests.
//
// These are destructive: they stop containers, sever networks and republish messages. They
// run against a live compose stack and leave it in a working state, but they will disrupt
// anything else using it while they run — hence the separate build tag.
//
//	docker compose up -d --build
//	go test -tags=chaos ./tests/chaos/... -v -timeout 20m
//
// Every scenario asserts a concrete recovery PROPERTY, not merely that nothing crashed.
// "The system survived" is not a test; "no order was lost, no stock leaked, and exactly one
// notification was sent" is.
package chaos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	ordersBase        = "http://localhost:18080"
	inventoryBase     = "http://localhost:18082"
	notificationsBase = "http://localhost:18083"
	kafkaExternal     = "localhost:29092"

	// composeProject matches `name:` in docker-compose.yml. Container names are derived
	// from it, and docker network commands need the full name rather than the service.
	composeProject = "sw-edm"
	composeNetwork = "sw-edm_default"
)

// --- docker control ---------------------------------------------------------

func compose(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func docker(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	return string(out), err
}

// repoRoot finds the directory holding docker-compose.yml, walking up from the test's
// working directory. Hard-coding "../.." breaks the moment someone runs the test from
// somewhere else, and the failure looks like a docker problem rather than a path problem.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(dir + "/docker-compose.yml"); err == nil {
			return dir
		}
		dir += "/.."
	}
	t.Fatal("could not find docker-compose.yml above the test directory")
	return ""
}

// stopService kills a container without removing it, and registers a cleanup that brings
// it back. Registered immediately so a test that fails mid-scenario still restores the
// stack — a chaos test that leaves the system broken has broken every test after it.
func stopService(t *testing.T, service string) {
	t.Helper()
	t.Logf("chaos: stopping %s", service)
	compose(t, "stop", "-t", "5", service)
	t.Cleanup(func() {
		t.Logf("chaos: restoring %s", service)
		compose(t, "start", service)
	})
}

func startService(t *testing.T, service string) {
	t.Helper()
	t.Logf("chaos: starting %s", service)
	compose(t, "start", service)
}

// partition disconnects a container from the docker network.
//
// Deliberately different from stopping it. A stopped service refuses connections
// immediately, which is an unambiguous failure the caller can retry cleanly. A partitioned
// service accepts nothing and answers nothing, so calls HANG until their deadline — which
// produces the ambiguous outcome the saga has to resolve by asking rather than guessing.
// That distinction is the whole reason both scenarios exist.
// Returns a heal function that is safe to call any number of times. It is also registered
// as a cleanup, so a test that fails before healing still restores the network — and a test
// that heals explicitly does not then produce a spurious "already connected" error from the
// cleanup running second.
func partition(t *testing.T, container string) (heal func()) {
	t.Helper()
	t.Logf("chaos: partitioning %s from %s", container, composeNetwork)
	if out, err := docker(t, "network", "disconnect", composeNetwork, container); err != nil {
		t.Fatalf("partitioning %s: %v\n%s", container, err, out)
	}

	var once sync.Once
	heal = func() {
		once.Do(func() {
			t.Logf("chaos: healing partition for %s", container)
			if out, err := docker(t, "network", "connect", composeNetwork, container); err != nil {
				t.Logf("healing partition for %s: %v\n%s", container, err, out)
			}
		})
	}
	t.Cleanup(heal)
	return heal
}

func containerName(service string) string { return composeProject + "-" + service + "-1" }

// --- HTTP helpers -----------------------------------------------------------

type money struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

type line struct {
	SKU        string `json:"sku"`
	Quantity   uint32 `json:"quantity"`
	UnitAmount money  `json:"unit_amount"`
}

type orderResponse struct {
	OrderID       string `json:"order_id"`
	State         string `json:"state"`
	CorrelationID string `json:"correlation_id"`
	PaymentID     string `json:"payment_id"`
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"cancellation_reason"`
	FailedStep    string `json:"failed_step"`
}

type stepLog struct {
	Index    int    `json:"index"`
	Name     string `json:"name"`
	State    string `json:"state"`
	Attempts int    `json:"attempts"`
	Detail   string `json:"detail"`
}

type sagaSnapshot struct {
	State       string    `json:"state"`
	CurrentStep int       `json:"current_step"`
	Attempts    int       `json:"attempts"`
	LastError   string    `json:"last_error"`
	Steps       []stepLog `json:"steps"`
}

type stockItem struct {
	SKU       string `json:"sku"`
	OnHand    int    `json:"on_hand"`
	Reserved  int    `json:"reserved"`
	Available int    `json:"available"`
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

func placeOrder(t *testing.T, token, sku string, qty uint32) (orderResponse, error) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"customer_id":          "1d5a8c3f-7b2e-4d9a-8f6c-3e5b7d9f1a2c",
		"customer_email":       "chaos@example.com",
		"payment_method_token": token,
		"lines": []line{{
			SKU: sku, Quantity: qty, UnitAmount: money{AmountMinor: 1250, Currency: "GBP"},
		}},
	})
	resp, err := httpClient.Post(ordersBase+"/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		return orderResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return orderResponse{}, fmt.Errorf("placing order: status %d", resp.StatusCode)
	}
	var out orderResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return orderResponse{}, err
	}
	return out, nil
}

func getOrder(t *testing.T, id string) orderResponse {
	t.Helper()
	var out orderResponse
	getJSON(t, ordersBase+"/orders/"+id, &out)
	return out
}

func getSaga(t *testing.T, id string) sagaSnapshot {
	t.Helper()
	var out sagaSnapshot
	getJSON(t, ordersBase+"/orders/"+id+"/saga", &out)
	return out
}

func getStock(t *testing.T, sku string) stockItem {
	t.Helper()
	var out struct {
		Items []stockItem `json:"items"`
	}
	getJSON(t, inventoryBase+"/stock?sku="+sku, &out)
	for _, i := range out.Items {
		if i.SKU == sku {
			return i
		}
	}
	t.Fatalf("no stock record for %s", sku)
	return stockItem{}
}

func setStock(t *testing.T, sku string, onHand int) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"items": []map[string]any{{"sku": sku, "on_hand": onHand, "reserved": 0}},
	})
	req, _ := http.NewRequest(http.MethodPut, inventoryBase+"/admin/stock", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("setting stock: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("setting stock: status %d", resp.StatusCode)
	}
}

type notification struct {
	NotificationID string `json:"notification_id"`
	SourceEventID  string `json:"source_event_id"`
	Template       string `json:"template"`
	State          string `json:"state"`
}

func getNotifications(t *testing.T, orderID string) []notification {
	t.Helper()
	var out struct {
		Notifications []notification `json:"notifications"`
	}
	getJSON(t, fmt.Sprintf("%s/orders/%s/notifications", notificationsBase, orderID), &out)
	return out.Notifications
}

// outboxPending reads the backlog a service is holding. This is the number that proves an
// outage was absorbed rather than survived by luck.
func outboxPending(t *testing.T, base string) int {
	t.Helper()
	var out struct {
		Pending int `json:"outbox_pending"`
	}
	getJSON(t, base+"/health/ready", &out)
	return out.Pending
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatalf("decoding %s: %v", url, err)
	}
}

// --- waiting ----------------------------------------------------------------

// eventually polls until cond returns true. Polling rather than sleeping a fixed duration:
// recovery times vary with the machine, and a fixed sleep is either flaky or slow.
//
// On timeout it calls describe() so the failure message says what the system actually
// looked like. "Timed out" on its own is the least useful failure a chaos test can produce.
func eventually(t *testing.T, within time.Duration, what string, cond func() bool, describe func() string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	detail := ""
	if describe != nil {
		detail = "\n" + describe()
	}
	t.Fatalf("timed out after %s waiting for %s%s", within, what, detail)
}

func awaitTerminal(t *testing.T, orderID string, within time.Duration) orderResponse {
	t.Helper()
	var final orderResponse
	eventually(t, within, "order "+orderID+" to reach a terminal state",
		func() bool {
			final = getOrder(t, orderID)
			return final.State == "CONFIRMED" || final.State == "CANCELLED"
		},
		func() string {
			saga := getSaga(t, orderID)
			dump, _ := json.MarshalIndent(saga, "", "  ")
			return fmt.Sprintf("order state=%s\nsaga:\n%s", final.State, dump)
		})
	return final
}

func waitForService(t *testing.T, base string, within time.Duration) {
	t.Helper()
	eventually(t, within, "service at "+base+" to become ready", func() bool {
		resp, err := httpClient.Get(base + "/health/ready")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, nil)
}

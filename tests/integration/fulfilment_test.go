//go:build integration

// Cross-service tests: the parts that only become true once all four services are running.
//
// These assert on observable state through each service's own API — order status from
// orders, stock levels from inventory, notification records from notifications. Nothing
// here reads a database directly: a test that reaches into a service's tables passes when
// the service is broken in exactly the way that matters.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

var (
	inventoryBase     = envOr("INVENTORY_BASE_URL", "http://localhost:18082")
	notificationsBase = envOr("NOTIFICATIONS_BASE_URL", "http://localhost:18083")
)

type stockItem struct {
	SKU       string `json:"sku"`
	OnHand    int    `json:"on_hand"`
	Reserved  int    `json:"reserved"`
	Available int    `json:"available"`
}

type notification struct {
	NotificationID string `json:"notification_id"`
	SourceEventID  string `json:"source_event_id"`
	Channel        string `json:"channel"`
	Template       string `json:"template"`
	State          string `json:"state"`
	Attempts       int    `json:"attempts"`
	RecipientHash  string `json:"recipient_hash"`
}

// setStock pins a SKU to a known level.
//
// Tests set their own levels rather than depending on services/inventory/seed.json.
// Depending on a fixture makes every assertion sensitive to an edit nobody expects to be
// load-bearing, and makes tests order-dependent as soon as one of them consumes stock.
func setStock(t *testing.T, sku string, onHand int) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"items": []map[string]any{{"sku": sku, "on_hand": onHand, "reserved": 0}},
	})
	req, err := http.NewRequest(http.MethodPut, inventoryBase+"/admin/stock", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building stock request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("setting stock: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("setting stock: status %d", resp.StatusCode)
	}
}

func getStock(t *testing.T, sku string) stockItem {
	t.Helper()
	resp, err := http.Get(inventoryBase + "/stock?sku=" + sku)
	if err != nil {
		t.Fatalf("reading stock: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Items []stockItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding stock: %v", err)
	}
	for _, i := range out.Items {
		if i.SKU == sku {
			return i
		}
	}
	t.Fatalf("no stock record for %s", sku)
	return stockItem{}
}

func placeFor(t *testing.T, token, sku string, qty uint32) orderResponse {
	t.Helper()
	return place(t, token, line{SKU: sku, Quantity: qty, UnitAmount: money{1250, "GBP"}})
}

// awaitNotifications polls until at least one notification exists for the order, or gives
// up. Notifications are asynchronous and arrive after the order is already terminal, so
// asserting immediately after confirmation is a race.
func awaitNotifications(t *testing.T, orderID string, within time.Duration) []notification {
	t.Helper()
	deadline := time.Now().Add(within)
	var last []notification
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("%s/orders/%s/notifications", notificationsBase, orderID))
		if err == nil {
			var out struct {
				Notifications []notification `json:"notifications"`
			}
			decodeErr := json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			if decodeErr == nil {
				last = out.Notifications
				if len(last) > 0 {
					return last
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return last
}

// TestFulfilmentDeductsStock is the whole system in one test: four saga steps across three
// services, ending with stock actually gone and the customer actually told.
func TestFulfilmentDeductsStock(t *testing.T) {
	const sku = "SKU-TEST-FULFIL"
	setStock(t, sku, 10)

	placed := placeFor(t, "pm_ok", sku, 3)
	final := awaitTerminal(t, placed.OrderID, 30*time.Second)

	if final.State != "CONFIRMED" {
		t.Fatalf("state = %s, want CONFIRMED (reason %q)", final.State, final.Reason)
	}
	if final.ReservationID == "" {
		t.Error("a confirmed order must carry the reservation that supplied it")
	}

	saga := getSaga(t, placed.OrderID)
	wantSteps := []string{"reserve_stock", "authorize_payment", "capture_payment", "commit_reservation"}
	if len(saga.Steps) != len(wantSteps) {
		t.Fatalf("saga ran %d steps, want %d: %+v", len(saga.Steps), len(wantSteps), saga.Steps)
	}
	for i, want := range wantSteps {
		if saga.Steps[i].Name != want {
			t.Errorf("step %d = %s, want %s", i, saga.Steps[i].Name, want)
		}
		if saga.Steps[i].State != "DONE" {
			t.Errorf("step %s = %s, want DONE", saga.Steps[i].Name, saga.Steps[i].State)
		}
	}

	// Committed, not merely reserved: on_hand falls and reserved returns to zero. If only
	// `reserved` had moved, the units would still be counted as ours and the sale would
	// not have happened as far as the warehouse is concerned.
	stock := getStock(t, sku)
	if stock.OnHand != 7 {
		t.Errorf("on_hand = %d, want 7 (10 minus 3 committed)", stock.OnHand)
	}
	if stock.Reserved != 0 {
		t.Errorf("reserved = %d, want 0 — commit must clear the hold, not just deduct", stock.Reserved)
	}

	notes := awaitNotifications(t, placed.OrderID, 20*time.Second)
	if len(notes) != 1 {
		t.Fatalf("got %d notifications, want exactly 1: %+v", len(notes), notes)
	}
	if notes[0].Template != "order_confirmed" || notes[0].State != "SENT" {
		t.Errorf("notification = %s/%s, want order_confirmed/SENT", notes[0].Template, notes[0].State)
	}
	if len(notes[0].RecipientHash) != 64 {
		t.Errorf("recipient_hash is %d chars, want a 64-char SHA-256 — the address must never be published",
			len(notes[0].RecipientHash))
	}
}

// TestInsufficientStockFailsFirstStep proves the cheap, invisible step fails first: no
// authorization is attempted, so nothing ever appears on the customer's card.
func TestInsufficientStockFailsFirstStep(t *testing.T) {
	const sku = "SKU-TEST-SCARCE"
	setStock(t, sku, 2)

	placed := placeFor(t, "pm_ok", sku, 5)
	final := awaitTerminal(t, placed.OrderID, 30*time.Second)

	if final.State != "CANCELLED" {
		t.Fatalf("state = %s, want CANCELLED", final.State)
	}
	if final.Reason != "INSUFFICIENT_STOCK" {
		t.Errorf("reason = %q, want INSUFFICIENT_STOCK", final.Reason)
	}
	if final.FailedStep != "RESERVE_STOCK" {
		t.Errorf("failed_step = %q, want RESERVE_STOCK", final.FailedStep)
	}
	if final.PaymentID != "" {
		t.Error("no payment should exist: stock is checked before the card is touched")
	}

	saga := getSaga(t, placed.OrderID)
	if len(saga.Steps) != 1 {
		t.Errorf("saga ran %d steps, want 1 — it must stop at the first failure", len(saga.Steps))
	}
	// The refusal names the SKU and the shortfall, so a support query is answerable from
	// the saga log without reading inventory's database.
	if saga.LastError == "" {
		t.Error("the saga recorded no reason for the refusal")
	}

	if stock := getStock(t, sku); stock.Reserved != 0 || stock.OnHand != 2 {
		t.Errorf("stock moved on a refused reservation: %+v", stock)
	}
}

// TestDeclineAfterReservationReleasesStock is the compensation test that matters most.
//
// Stock is reserved, then the card is declined. If the release compensation did not run,
// those units would stay reserved forever and quietly leak out of the available pool —
// a failure nobody notices until the shop cannot sell something it physically has.
func TestDeclineAfterReservationReleasesStock(t *testing.T) {
	const sku = "SKU-TEST-RELEASE"
	setStock(t, sku, 6)

	placed := placeFor(t, "pm_declined", sku, 4)
	final := awaitTerminal(t, placed.OrderID, 30*time.Second)

	if final.State != "CANCELLED" {
		t.Fatalf("state = %s, want CANCELLED", final.State)
	}
	if final.Reason != "PAYMENT_DECLINED" {
		t.Errorf("reason = %q, want PAYMENT_DECLINED", final.Reason)
	}
	if final.FailedStep != "AUTHORIZE_PAYMENT" {
		t.Errorf("failed_step = %q, want AUTHORIZE_PAYMENT", final.FailedStep)
	}

	saga := getSaga(t, placed.OrderID)
	var reserve *stepLog
	for i := range saga.Steps {
		if saga.Steps[i].Name == "reserve_stock" {
			reserve = &saga.Steps[i]
		}
	}
	if reserve == nil {
		t.Fatal("reserve_stock is missing from the saga log")
	}
	if reserve.State != "COMPENSATED" {
		t.Errorf("reserve_stock = %s, want COMPENSATED", reserve.State)
	}

	stock := getStock(t, sku)
	if stock.OnHand != 6 {
		t.Errorf("on_hand = %d, want 6 — a released reservation must not deduct anything", stock.OnHand)
	}
	if stock.Reserved != 0 {
		t.Errorf("reserved = %d, want 0 — the hold leaked", stock.Reserved)
	}
	if stock.Available != 6 {
		t.Errorf("available = %d, want 6 — releasing must be a true undo", stock.Available)
	}
}

// TestCancellationNotifiesCustomer checks the other template, and that a cancelled order
// still produces exactly one notification.
func TestCancellationNotifiesCustomer(t *testing.T) {
	const sku = "SKU-TEST-NOTIFY-CANCEL"
	setStock(t, sku, 5)

	placed := placeFor(t, "pm_declined", sku, 1)
	final := awaitTerminal(t, placed.OrderID, 30*time.Second)
	if final.State != "CANCELLED" {
		t.Fatalf("state = %s, want CANCELLED", final.State)
	}

	notes := awaitNotifications(t, placed.OrderID, 20*time.Second)
	if len(notes) != 1 {
		t.Fatalf("got %d notifications, want exactly 1: %+v", len(notes), notes)
	}
	if notes[0].Template != "order_cancelled" {
		t.Errorf("template = %q, want order_cancelled", notes[0].Template)
	}
}

// TestNotificationIsNotDuplicated is the deduplication proof.
//
// It waits well past the point where the order is terminal and every event has been
// consumed, then checks the count again. Kafka redelivers on rebalance and the outbox
// publishes at least once, so more than one delivery attempt is expected — but only one
// notification may exist, because (source_event_id, channel) is unique.
func TestNotificationIsNotDuplicated(t *testing.T) {
	const sku = "SKU-TEST-DEDUP"
	setStock(t, sku, 5)

	placed := placeFor(t, "pm_ok", sku, 1)
	if final := awaitTerminal(t, placed.OrderID, 30*time.Second); final.State != "CONFIRMED" {
		t.Fatalf("state = %s, want CONFIRMED", final.State)
	}

	first := awaitNotifications(t, placed.OrderID, 20*time.Second)
	if len(first) != 1 {
		t.Fatalf("got %d notifications, want 1", len(first))
	}

	time.Sleep(3 * time.Second)
	second := awaitNotifications(t, placed.OrderID, 5*time.Second)
	if len(second) != 1 {
		t.Fatalf("got %d notifications after settling, want 1 — a redelivery produced a duplicate: %+v",
			len(second), second)
	}
	if second[0].NotificationID != first[0].NotificationID {
		t.Error("the notification id changed, so a second record was created for one event")
	}
}

// TestInventoryReadinessReportsOutboxBacklog checks the endpoint the chaos tests depend on.
// If it does not report a number, "the backlog drained after the broker came back" is not
// something a test can assert.
func TestInventoryReadinessReportsOutboxBacklog(t *testing.T) {
	resp, err := http.Get(inventoryBase + "/health/ready")
	if err != nil {
		t.Fatalf("reading readiness: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Status  string `json:"status"`
		Pending *int   `json:"outbox_pending"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding readiness: %v", err)
	}
	if body.Pending == nil {
		t.Error("readiness does not report outbox_pending")
	}
}

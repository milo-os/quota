package quotaclaim

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.miloapis.com/quota/pkg/apis/quota"
)

func TestTableConvertorMatchesMiloColumns(t *testing.T) {
	tc := NewTableConvertor()
	granted := validQuotaClaim()
	granted.CreationTimestamp = metav1.NewTime(time.Now().Add(-5 * time.Hour))
	granted.Status.Conditions = []metav1.Condition{{Type: quota.QuotaClaimGranted, Status: metav1.ConditionTrue}}
	pending := validQuotaClaim()
	pending.Name = "pending"
	pending.Spec.Requests = nil

	table, err := tc.ConvertToTable(context.Background(), &quota.QuotaClaimList{
		ListMeta: metav1.ListMeta{ResourceVersion: "42"},
		Items:    []quota.QuotaClaim{*granted, *pending},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range table.ColumnDefinitions {
		names = append(names, c.Name)
	}
	if want := []string{"Name", "Granted", "Resource", "Age"}; !equal(names, want) {
		t.Fatalf("columns = %v, want %v", names, want)
	}
	if table.ColumnDefinitions[2].Priority != 1 {
		t.Error("Resource column must be priority 1 (wide output only)")
	}
	if table.ResourceVersion != "42" || len(table.Rows) != 2 {
		t.Fatalf("unexpected table: rv=%q rows=%d", table.ResourceVersion, len(table.Rows))
	}
	if got := table.Rows[0].Cells; got[0] != "test-claim" || got[1] != "True" || got[2] != "widgets" || got[3] != "5h" {
		t.Errorf("granted row = %v", got)
	}
	if got := table.Rows[1].Cells; got[1] != nil || got[2] != nil {
		t.Errorf("absent condition and requests should render as <none>, got %v", got)
	}

	noHeaders, err := tc.ConvertToTable(context.Background(), granted, &metav1.TableOptions{NoHeaders: true})
	if err != nil || len(noHeaders.ColumnDefinitions) != 0 || len(noHeaders.Rows) != 1 {
		t.Fatalf("NoHeaders single-object conversion failed: %v %+v", err, noHeaders)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

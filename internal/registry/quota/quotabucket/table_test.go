package quotabucket

import (
	"context"
	"testing"
)

func TestTableConvertorMatchesMiloColumns(t *testing.T) {
	b := validQuotaBucket()
	b.Status.Limit, b.Status.Allocated, b.Status.Available, b.Status.ClaimCount = 10, 4, 6, 2
	table, err := NewTableConvertor().ConvertToTable(context.Background(), b, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Name", "Resource Type", "Limit", "Allocated", "Available", "Claims", "Age"}
	if len(table.ColumnDefinitions) != len(want) {
		t.Fatalf("got %d columns, want %v", len(table.ColumnDefinitions), want)
	}
	for i, c := range table.ColumnDefinitions {
		if c.Name != want[i] {
			t.Errorf("column %d = %q, want %q", i, c.Name, want[i])
		}
	}
	cells := table.Rows[0].Cells
	if cells[2] != int64(10) || cells[3] != int64(4) || cells[4] != int64(6) || cells[5] != int64(2) {
		t.Errorf("unexpected integer cells %v", cells)
	}
}

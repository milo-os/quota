package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/component-base/metrics/testutil"

	"go.miloapis.com/quota/internal/metrics"
	quotainternal "go.miloapis.com/quota/pkg/apis/quota"
)

func TestIsServerError(t *testing.T) {
	gr := schema.GroupResource{Group: "quota.miloapis.com", Resource: "quotaclaims"}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{name: "storage internal error", ctx: context.Background(), err: storage.NewInternalError(errors.New("commit")), want: true},
		{name: "api internal error", ctx: context.Background(), err: apierrors.NewInternalError(errors.New("x")), want: true},
		{name: "plain error", ctx: context.Background(), err: errors.New("connection refused"), want: true},
		{name: "missing bucket", ctx: context.Background(), err: apierrors.NewForbidden(gr, "c", errors.New("no bucket")), want: false},
		{name: "already exists", ctx: context.Background(), err: apierrors.NewAlreadyExists(gr, "c"), want: false},
		{name: "bad request", ctx: context.Background(), err: apierrors.NewBadRequest("no requests"), want: false},
		{name: "caller cancelled", ctx: cancelled, err: storage.NewInternalError(errors.New("begin tx")), want: false},
		{name: "deadline", ctx: context.Background(), err: context.DeadlineExceeded, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isServerError(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("isServerError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// An allocation that fails inside the database counts as result="error",
// which the QuotaClaimErrors alert watches. No Postgres is needed: the DSN
// points at a closed port, so beginning the transaction fails.
func TestAllocateStorageFailureCountsError(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://quota@127.0.0.1:1/none?connect_timeout=1&sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	a := NewAllocator(db, allocatorTestCodec, WithMaxBatchSize(1))
	claim := &quotainternal.QuotaClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: quotainternal.QuotaClaimSpec{
			ConsumerRef: testConsumer(),
			Requests:    []quotainternal.ResourceRequest{{ResourceType: "cpu", Amount: 1}},
		},
	}
	counter := metrics.QuotaClaimTotal.WithLabelValues(metrics.ClaimResultError)
	before, err := testutil.GetCounterMetricValue(counter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Allocate(context.Background(), claim); err == nil {
		t.Fatal("Allocate succeeded against an unreachable database")
	}
	after, err := testutil.GetCounterMetricValue(counter)
	if err != nil {
		t.Fatal(err)
	}
	if after-before != 1 {
		t.Fatalf("quota_claim_total{result=error} rose by %v, want 1", after-before)
	}
}

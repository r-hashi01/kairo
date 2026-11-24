package kairo_test

import (
	"context"
	"testing"

	kairo "github.com/r-hashi01/kairo/sdk/go"
	"github.com/r-hashi01/kairo/sdk/go/kairotest"
)

func TestInMemory(t *testing.T) {
	kairotest.Run(t, func(t *testing.T) kairotest.Opener {
		store := kairo.NewMemStore()
		return func() kairo.Store { return store }
	})
}

func TestMemStoreCreationOrder(t *testing.T) {
	s := kairo.NewMemStore()
	put := func(id, parent, workflow string) {
		t.Helper()
		err := s.WithRun(context.Background(), id, func(*kairo.RunRow) (*kairo.Changes, error) {
			return &kairo.Changes{Row: &kairo.RunRow{ID: id, Plan: "p", Hash: "h", State: []byte{1}, Status: "running",
				CreatedAt: 42, UpdatedAt: 42, Parent: parent, Workflow: workflow}}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	put("z-root", "", "orders")
	put("a-root", "", "orders")
	put("z-child", "parent", "")
	put("a-child", "parent", "")
	rows, err := s.List(context.Background(), kairo.ListFilter{Workflow: "orders", After: "z-root"})
	if err != nil || len(rows) != 1 || rows[0].ID != "a-root" {
		t.Fatalf("list: %+v %v", rows, err)
	}
	children, err := s.Children(context.Background(), "parent")
	if err != nil || len(children) != 2 || children[0].ID != "z-child" || children[1].ID != "a-child" {
		t.Fatalf("children: %+v %v", children, err)
	}
}

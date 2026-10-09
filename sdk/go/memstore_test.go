package kairo_test

import (
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
